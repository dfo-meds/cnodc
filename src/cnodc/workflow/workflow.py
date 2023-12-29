import gzip
import hashlib
import shutil
import tempfile
import uuid
from urllib.parse import quote

import zrlog

from cnodc.nodb import NODBControllerInstance, NODBController, structures
from cnodc.storage import StorageController, BaseStorageHandle
from autoinject import injector

from cnodc.storage.base import StorageTier, StorageFileHandle
from cnodc.util import CNODCError, HaltFlag, dynamic_object
import typing as t
import pathlib
import datetime


VALID_METADATA_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_:;.,\\/\"'?!(){}[]@<>=-+*#$&`|~^%"

VALID_FILENAME_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-."

RESERVED_FILENAMES = (
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
)


class FileInfo:

    def __init__(self,
                 file_path: t.Optional[str] = None,
                 filename: t.Optional[str] = None,
                 is_gzipped: bool = False,
                 last_modified_date: t.Optional[datetime.datetime] = None):
        self.file_path = file_path
        self.filename = filename
        self.is_gzipped = is_gzipped
        self.last_modified_date = last_modified_date

    def to_map(self) -> dict:
        map_ = {}
        if self.file_path:
            map_['file_path'] = self.file_path
        if self.filename:
            map_['filename'] = self.filename
        if self.is_gzipped:
            map_['is_gzipped'] = self.is_gzipped
        if self.last_modified_date is not None:
            map_['mod_date'] = self.last_modified_date.isoformat()
        return map_

    @staticmethod
    def from_map(map_: dict):
        if 'file_path' not in map_:
            raise CNODCError('Missing file path', 'PAYLOAD', 1005)
        return FileInfo(
            map_['file_path'] if 'file_path' in map_ else None,
            map_['filename'] if 'filename' in map_ else None,
            map_['is_gzipped'] if 'is_gzipped' in map_ else False,
            datetime.datetime.fromisoformat(map_['mod_date']) if 'mod_date' in map_ and map_['mod_date'] else None
        )


class WorkflowPayload:

    def __init__(self,
                 workflow_name: str,
                 current_step: int,
                 headers: dict,
                 metadata: dict):
        self.workflow_name = workflow_name
        self.current_step = current_step
        self.headers = headers or {}
        self.metadata = metadata or {}

    def update_for_propagation(self, payload, next_step: bool = False):
        if isinstance(payload, WorkflowPayload):
            payload.metadata.update(self.metadata)
            payload.headers.update(self.headers)
            payload.workflow_name = self.workflow_name
            if next_step:
                payload.current_step = self.current_step + 1

        else:
            if '_metadata' not in payload:
                payload['_metadata'] = self.metadata
            else:
                payload['_metadata'].update(self.metadata)
            if 'headers' not in payload:
                payload['headers'] = self.headers
            else:
                payload['headers'].update(self.headers)
            if 'workflow' not in payload:
                payload['workflow'] = {
                    'name': self.workflow_name,
                    'step': self.current_step + (0 if not next_step else 1)
                }

    def to_map(self):
        return {
            'workflow': {
                'name': self.workflow_name,
                'step': self.current_step
            },
            'headers': self.headers,
            'metadata': self.metadata
        }

    @staticmethod
    def build(queue_item: structures.NODBQueueItem):
        if 'workflow' not in queue_item.data:
            raise CNODCError('Missing item.data[workflow]', 'PAYLOAD', 1000)
        if 'name' not in queue_item.data['workflow']:
            raise CNODCError('Missing item.data[workflow][name]', 'PAYLOAD', 1001)
        if 'step' not in queue_item.data['workflow']:
            raise CNODCError('Missing item.data[workflow][step]', 'PAYLOAD', 1002)
        try:
            step_no = int(queue_item.data['workflow']['step'])
        except ValueError as ex:
            raise CNODCError('Invalid step number', 'PAYLOAD', 1004) from ex
        base_kwargs = {
                'workflow_name': queue_item.data['workflow']['name'],
                'workflow_step': step_no,
                'headers': queue_item.data['headers'] if 'headers' in queue_item.data else {},
                'metadata': queue_item.data['_metadata'] if '_metadata' in queue_item.data else {}
        }
        if 'file_info' in queue_item.data:
            return FilePayload.from_map(queue_item.data['file_info'], **base_kwargs)
        elif 'item_info' in queue_item.data:
            return ItemPayload.from_map(queue_item.data['item_info'], **base_kwargs)
        elif 'source_info' in queue_item.data:
            return SourceFilePayload.from_map(queue_item.data['source_info'], **base_kwargs)
        elif 'batch_info' in queue_item.data:
            return BatchPayload.from_map(queue_item.data['batch_info'], **base_kwargs)
        else:
            raise CNODCError('Unknown workflow payload type', 'PAYLOAD', 1003)


class FilePayload(WorkflowPayload):

    def __init__(self,
                 file_info: FileInfo,
                 **kwargs):
        super().__init__(**kwargs)
        self.file_info = file_info

    def to_map(self):
        map_ = super().to_map()
        map_['file_info'] = self.file_info.to_map()
        return map_

    @staticmethod
    def from_map(map_: dict, **kwargs):
        return FilePayload(
            file_info=FileInfo.from_map(map_),
            **kwargs
        )


class SourceFilePayload(WorkflowPayload):

    def __init__(self,
                 source_file_uuid: str,
                 received_date: datetime.date,
                 **kwargs):
        super().__init__(**kwargs)
        self.source_uuid = source_file_uuid
        self.received_date = received_date

    def to_map(self):
        map_ = super().to_map()
        map_['source_info'] = {
            'source_uuid': self.source_uuid,
            'received': self.received_date.isoformat()
        }
        return map_

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'source_uuid' not in map_:
            raise CNODCError('Missing source_uuid', 'PAYLOAD', 1007)
        if 'received' not in map_:
            raise CNODCError('Missing recieved date', 'PAYLOAD', 1008)
        return SourceFilePayload(
            map_['source_uuid'],
            datetime.date.fromisoformat(map_['received']),
            **kwargs
        )


class BatchPayload(WorkflowPayload):

    def __init__(self,
                 batch_uuid: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.batch_uuid = batch_uuid

    def to_map(self):
        map_ = super().to_map()
        map_['batch_info'] = {
            'uuid': self.batch_uuid
        }
        return map_

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing uuid', 'PAYLOAD', 1009)
        return BatchPayload(
            map_['uuid'],
            **kwargs
        )


class ItemPayload(WorkflowPayload):

    def __init__(self,
                 item_uuid: str,
                 item_received: datetime.date,
                 source_file_uuid: t.Optional[str] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.uuid = item_uuid
        self.received_date = item_received
        self.source_uuid = source_file_uuid

    def to_map(self):
        map_ = super().to_map()
        map_['item_info'] = {
            'uuid': self.uuid,
            'source_uuid': self.source_uuid,
            'received': self.received_date.isoformat()
        }
        return map_

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing item uuid', 'PAYLOAD', 1005)
        if 'received' not in map_:
            raise CNODCError('Missing item received date', 'PAYLOAD', 1006)
        return ItemPayload(
            map_['uuid'],
            datetime.date.fromisoformat(map_['received']),
            map_['source_uuid'] if 'source_uuid' in map_ else None,
            **kwargs
        )


class WorkflowController:

    nodb: NODBController = None
    storage: StorageController = None

    @injector.inject
    def __init__(self,
                 workflow_name: str,
                 config: dict,
                 process_metadata: t.Optional[dict] = None,
                 halt_flag: HaltFlag = None):
        self._process_metadata = process_metadata or {}
        self.name: str = workflow_name
        self.config = config
        self.halt_flag = halt_flag
        self._log = zrlog.get_logger("cnodc.workflow")

    def handle_incoming_file(self, local_path: pathlib.Path, headers: dict, post_hook: t.Optional[callable] = None, db: NODBControllerInstance = None):
        if db is not None:
            return self._handle_incoming_file(local_path, headers, post_hook, db)
        else:
            with self.nodb as db:
                self._handle_incoming_file(local_path, headers, post_hook, db)
                db.commit()

    def queue_step(self,
                        payload: WorkflowPayload,
                        priority: int = None,
                        unique_key: str = None,
                        db: NODBControllerInstance = None):
        if db is not None:
            return self._queue_step(payload, priority, unique_key, db)
        else:
            with self.nodb as db:
                self._queue_step(payload, priority, unique_key, db)
                db.commit()

    def has_more_steps(self, current_idx: int):
        if 'processing_steps' not in self.config or not self.config['processing_steps']:
            return False
        if current_idx < -1 or current_idx >= len(self.config['processing_steps']):
            return False
        return True

    def _handle_incoming_file(self, local_path: pathlib.Path, headers: dict, post_hook: t.Optional[callable], db: NODBControllerInstance):
        file_handles = []
        working_file = None
        try:
            if 'default_headers' in self.config:
                for x in self.config['default_headers']:
                    if x not in headers:
                        headers[x] = self.config['default_headers'][x]
            if 'validation' in self.config:
                self._validate_file_upload(local_path, headers, self.config['validation'])
            with_gzip = False
            filename = self._determine_filename(headers)
            gzip_filename = filename + ".gz"
            with tempfile.TemporaryDirectory as td:
                td = pathlib.Path(td)
                gzip_file = td / "bin.gz"
                gzip_made = False
                if 'working_target' in self.config:
                    with_gzip = 'gzip' in self.config['working_target'] and self.config['workflow_target']['gzip']
                    if with_gzip:
                        self._gzip_local_file(local_path, gzip_file)
                        gzip_made = True
                        working_file, target_tier = self._handle_file_upload(gzip_file, gzip_filename, headers, self.config['working_target'])
                    else:
                        working_file, target_tier = self._handle_file_upload(local_path, filename, headers, self.config['working_target'])
                    file_handles.append((working_file, target_tier))
                if 'additional_targets' in self.config:
                    for target in self.config['additional_targets']:
                        if 'gzip' in target and target['gzip']:
                            if not gzip_made:
                                self._gzip_local_file(local_path, gzip_file)
                                gzip_made = True
                            file_handles.append(self._handle_file_upload(gzip_file, gzip_filename, headers, target))
                        else:
                            file_handles.append(self._handle_file_upload(local_path, filename, headers, target))
            if working_file is not None:
                if self.has_more_steps(-1):
                    file_info = FileInfo(
                        working_file.path(),
                        gzip_filename if with_gzip else filename,
                        with_gzip,
                        (
                            headers['last-modified-time']
                            if 'last-modified-time' in headers and headers['last-modified-time'] else
                            datetime.datetime.now(datetime.timezone.utc)
                        )
                    )
                    self._queue_step(
                        FilePayload(file_info, current_step=0, headers=headers, workflow_name=self.name),
                        priority=None,
                        unique_key=hashlib.md5(file_info.file_path.encode('utf-8', errors='replace')).hexdigest(),
                        db=db
                    )
            if post_hook is not None:
                post_hook(db)
            db.commit()
            closing_handles = file_handles
            file_handles = []
            for handle, tier in closing_handles:
                if handle is not None and tier is not None:
                    handle.set_tier(tier)
        except Exception as ex:
            for fh, _ in file_handles:
                if fh is not None:
                    fh.remove()
            if isinstance(ex, CNODCError):
                raise ex
            else:
                raise CNODCError(f"Exception while processing incoming file: {ex.__class__.__name__}: {str(ex)}", "WORKFLOW", 1000)

    def _gzip_local_file(self, local_path, gzip_file):
        with open(local_path, "rb") as src:
            with gzip.open(gzip_file, "wb") as dest:
                # NB: 2.5 MiB per read translates to about 0.5 seconds between reads in testing. Thus, splitting the
                # file into roughly this size of chunks should allow the script to break within 0.5 seconds still.
                # Performance is similar to using shutil.copyfileobj().
                src_bytes = src.read(2621440)
                while src_bytes != b'':
                    dest.write(src_bytes)
                    if self.halt_flag:
                        self.halt_flag.check_continue(True)
                    src_bytes = src.read(2621440)

    def _determine_filename(self, metadata: dict) -> str:
        filename = None
        if 'filename_pattern' in self.config and self.config['filename_pattern']:
            filename = self._sanitize_filename(self._substitute_headers(self.config['filename_pattern'], metadata))
        if filename is None and 'filename' in metadata and 'accept_user_filename' in self.config and self.config['accept_user_filename']:
            filename = self._sanitize_filename(metadata['filename'])
        if filename is None and 'default-filename' in metadata:
            filename = self._sanitize_filename(metadata['default-filename'])
        if filename is None:
            filename = str(uuid.uuid4())
        return filename

    def _get_storage_metadata(self, templates: dict[str, str], metadata: dict) -> dict:
        return {
            x: self._sanitize_storage_metadata(self._substitute_headers(str(templates[x]), metadata))
            for x in templates
        }

    def _sanitize_storage_metadata(self, s: str) -> str:
        return quote(str(s), safe=VALID_METADATA_CHARACTERS)

    def _substitute_headers(self, s: str, metadata: dict) -> str:
        for h in metadata:
            s = s.replace("%{" + h.lower() + "}", metadata[h])
        return s.replace('${now}', datetime.datetime.now(datetime.timezone.utc).isoformat())

    def _sanitize_filename(self, filename: str) -> t.Optional[str]:
        filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])
        filename = filename.rstrip(".")
        if filename == "" or len(filename) > 255:
            return None
        check = filename if "." not in filename else filename[:filename.find(".")]
        if check in RESERVED_FILENAMES:
            return None
        return filename

    def _get_step_info(self, step_idx: int):
        if 'processing_steps' not in self.config or not self.config['processing_steps']:
            raise CNODCError('No processing steps defined', 'WORKFLOW', 1001)
        if step_idx < 0 or step_idx >= len(self.config['processing_steps']):
            raise CNODCError(f"Invalid step index [{step_idx}]", 'WORKFLOW', 1002)
        return self.config['processing_steps'][step_idx]

    def _queue_step(self,
                    payload: WorkflowPayload,
                    priority: t.Optional[int],
                    unique_key: t.Optional[str],
                    db: NODBControllerInstance):
        queue_info = self._get_step_info(payload.current_step)
        payload_dict = payload.to_map()
        payload_dict['_metadata']['send_time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        payload_dict['_metadata'].update(self._process_metadata)
        if priority is None and 'priority' in queue_info:
            try:
                priority = int(queue_info['priority'])
            except ValueError:
                self._log.exception(f"Invalid priority for queue item [{self.name}:{payload.current_step}]")
        db.create_queue_item(
            queue_name=queue_info['name'],
            data=payload_dict,
            priority=priority,
            unique_item_key=unique_key
        )

    def _validate_file_upload(self, local_path: pathlib.Path, metadata: dict, validation: str):
        dynamic_object(validation)(local_path, metadata)

    def _handle_file_upload(self, local_path: pathlib.Path, filename: str, metadata: dict, upload_kwargs: dict) -> tuple[StorageFileHandle, t.Optional[StorageTier]]:
        if 'directory' not in upload_kwargs:
            raise CNODCError("Missing directory for workflow upload action", "WORKFLOW", 1003)
        storage_metadata = self._get_storage_metadata(
            upload_kwargs['metadata'] if 'metadata' in upload_kwargs and upload_kwargs['metadata'] else {},
            metadata
        )
        storage_tier = StorageTier(upload_kwargs['tier']) if 'tier' in upload_kwargs and upload_kwargs['tier'] else None
        target_dir_handle = self.storage.get_handle(upload_kwargs['directory'], halt_flag=self.halt_flag)
        allow_overwrite = metadata['allow-overwrite'] == '1' if 'allow-overwrite' in metadata else False
        if 'allow_overwrite' in upload_kwargs:
            if upload_kwargs['allow_overwrite'] == 'never':
                allow_overwrite = False
            elif upload_kwargs['allow_overwrite'] == 'always':
                allow_overwrite = True
        file_handle = target_dir_handle.child(filename)
        file_handle.upload(
            local_path,
            allow_overwrite=allow_overwrite,
            storage_tier=StorageTier.FREQUENT,
            metadata=storage_metadata
        )
        if storage_tier is None or (not file_handle.supports_tiering()) or storage_tier == StorageTier.FREQUENT:
            return file_handle, None
        else:
            return file_handle, storage_tier

