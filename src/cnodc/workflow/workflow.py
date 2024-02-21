"""
    Workflows represent a series of steps that an input object (usually a file) will undergo once received.

    Each step has an input and output which is represented by a payload type. These payload types are:

    1. File (a physical file located on a computer). Workflows are started with a file payload
    2. SourceFile (a physical file referenced by an entry in the database).
    3. Batch (a set of one or more working records, referenced by their batch ID)
    4. Observation (a specific observation, which is a working record once finalized to the DB)
"""
import gzip
import hashlib
import tempfile
import uuid
from urllib.parse import quote
import zrlog
from cnodc.nodb import NODBControllerInstance, NODBController, structures
from cnodc.storage import StorageController, StorageTier, BaseStorageHandle
from autoinject import injector
from cnodc.util import CNODCError, HaltFlag, dynamic_object, haltable_gzip
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
    """Represents information about a file submitted to the workflow process.

    Note that this is NOT a file in the database but a raw file on storage
    somewhere (e.g. an Azure Blob or an Azure File Share).
    """

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
        """Convert the object into a map."""
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
        """Build the object from a map."""
        if 'file_path' not in map_:
            raise CNODCError('Missing file path', 'PAYLOAD', 1005)
        return FileInfo(
            map_['file_path'] if 'file_path' in map_ else None,
            map_['filename'] if 'filename' in map_ else None,
            map_['is_gzipped'] if 'is_gzipped' in map_ else False,
            datetime.datetime.fromisoformat(map_['mod_date']) if 'mod_date' in map_ and map_['mod_date'] else None
        )


class WorkflowPayload:
    """Generalized class for all workflow payloads"""

    def __init__(self,
                 workflow_name: t.Optional[str] = None,
                 current_step: t.Optional[int] = None,
                 metadata: t.Optional[dict] = None):
        self.workflow_name = workflow_name
        self.current_step = current_step
        self.metadata = metadata or {}

    def load_workflow(self,
                      db: NODBControllerInstance,
                      halt_flag: HaltFlag = None):
        """Find the workflow associated with this payload and load the controller for it."""
        workflow_config = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
        return WorkflowController(
            workflow_name=self.workflow_name,
            config=workflow_config.configuration,
            halt_flag=halt_flag
        )

    def set_metadata(self, key, value):
        """Set a metadata property (or delete it if the value is None)"""
        if value is not None:
            self.metadata[key] = value
        elif key in self.metadata:
            del self.metadata[key]

    def get_metadata(self, key, default=None):
        """Retrieve a metadata property, or the default if it is not present"""
        if key in self.metadata and self.metadata[key] is not None:
            return self.metadata[key]
        return default

    def clone(self):
        """Create a deep copy of the payload."""
        return WorkflowPayload.from_map(self.to_map())

    def set_subqueue_name(self, subqueue_name: t.Optional[str]):
        """Set the name of the subqueue for the queue item."""
        self.set_metadata('manual-subqueue', subqueue_name)

    def set_unique_key(self, item_key: t.Optional[str]):
        """Set the unique key for the queue item."""
        self.set_metadata('unique-item-key', item_key)

    def set_priority(self, new_priority: t.Optional[int]):
        """Set the priority of the queue item."""
        self.set_metadata('queue-priority', new_priority)

    def set_followup_queue(self, queue_name: t.Optional[str]):
        """Set the follow-up queue after a manual review cycle"""
        self.set_metadata('post-review-queue', queue_name)

    def increment_priority(self, increment: int = 1):
        """Increment the priority by the given amount."""
        if 'queue-priority' in self.metadata and isinstance(self.metadata['queue-priority'], int):
            self.metadata['queue-priority'] = self.metadata['queue-priority'] + increment
        else:
            self.metadata['queue-priority'] = increment

    def decrement_priority(self, increment: int = 1):
        """Decrement the priority by the given amount."""
        self.increment_priority(increment * -1)

    def enqueue_followup(self, db: NODBControllerInstance) -> bool:
        """Create a queue item based on the follow-up queue name."""
        if 'post-review-queue' in self.metadata and self.metadata['post-review-queue']:
            queue_name = self.metadata['post-review-queue']
            del self.metadata['post-review-queue']
            self.enqueue(db, queue_name)
            return True
        return False

    def enqueue(self,
                db: NODBControllerInstance,
                queue_name: str,
                override_priority: t.Optional[int] = None):
        """Enqueue this payload in the given queue."""
        if self.metadata is None:
            self.metadata = {}
        self.metadata['send_time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        kwargs = {
            'queue_name': queue_name,
            'data': self.to_map(),
        }
        if override_priority is not None:
            kwargs['priority'] = override_priority
        elif 'queue-priority' in self.metadata and isinstance(self.metadata['queue-priority'], int):
            kwargs['priority'] = self.metadata['queue-priority']
        if 'manual-subqueue' in self.metadata and self.metadata['manual-subqueue']:
            kwargs['subqueue_name'] = self.metadata['manual-subqueue']
        if 'unique-item-key' in self.metadata and self.metadata['unique-item-key']:
            kwargs['unique_item_key'] = self.metadata['unique-item-key']
        db.create_queue_item(**kwargs)

    def copy_details_from(self, payload, next_step: bool = False):
        """Copy key details (workflow info and metadata) from another payload into this one."""
        self.workflow_name = payload.workflow_name
        self.current_step = payload.current_step
        if next_step:
            self.current_step += 1
        for key in payload.metadata:
            if key not in self.metadata:
                self.metadata[key] = payload.metadata[key]

    def to_map(self) -> dict:
        """Convert this object to a map."""
        return {
            'workflow': {
                'name': self.workflow_name,
                'step': self.current_step
            },
            'metadata': self.metadata
        }

    @staticmethod
    def from_queue_item(queue_item: structures.NODBQueueItem):
        """Create a workflow payload from a queue item."""
        return WorkflowPayload.from_map(queue_item.data)

    @staticmethod
    def from_map(data: dict):
        """Create a workflow payload from a map."""
        if 'workflow' not in data:
            raise CNODCError('Missing item.data[workflow]', 'PAYLOAD', 1000)
        if 'name' not in data['workflow']:
            raise CNODCError('Missing item.data[workflow][name]', 'PAYLOAD', 1001)
        if 'step' not in data['workflow']:
            raise CNODCError('Missing item.data[workflow][step]', 'PAYLOAD', 1002)
        try:
            step_no = int(data['workflow']['step'])
        except ValueError as ex:
            raise CNODCError('Invalid step number', 'PAYLOAD', 1004) from ex
        base_kwargs = {
                'workflow_name': data['workflow']['name'],
                'current_step': step_no,
                'metadata': data['_metadata'] if '_metadata' in data else {}
        }
        if 'file_info' in data:
            return FilePayload.from_map(data['file_info'], **base_kwargs)
        elif 'item_info' in data:
            return ObservationPayload.from_map(data['item_info'], **base_kwargs)
        elif 'source_info' in data:
            return SourceFilePayload.from_map(data['source_info'], **base_kwargs)
        elif 'batch_info' in data:
            return BatchPayload.from_map(data['batch_info'], **base_kwargs)
        else:
            raise CNODCError('Unknown workflow payload type', 'PAYLOAD', 1003)


class FilePayload(WorkflowPayload):
    """Workflow payload for a physical file on disk."""

    def __init__(self,
                 file_info: FileInfo,
                 **kwargs):
        super().__init__(**kwargs)
        self.file_info = file_info

    def to_map(self):
        map_ = super().to_map()
        map_['file_info'] = self.file_info.to_map()
        return map_

    @injector.inject
    def download(self,
                 target_dir: pathlib.Path,
                 storage: StorageController = None,
                 halt_flag: HaltFlag = None) -> pathlib.Path:
        """Download the file to a given directory."""
        handle = storage.get_handle(self.file_info.file_path, halt_flag=halt_flag)
        if handle is None:
            raise CNODCError('Cannot handle file path', 'PAYLOAD', 2000)
        if not handle.exists():
            raise CNODCError('File path does not exist', 'PAYLOAD', 2001)
        target_name = self.file_info.filename
        if self.file_info.is_gzipped:
            if target_name.lower().endswith('.gz'):
                gzip_target_name = target_name
                target_name = target_name[:-3]
            else:
                gzip_target_name = f"{target_name}.gz"
            gzip_path = target_dir / gzip_target_name
            target_path = target_dir / target_name
            handle.download(gzip_path)
            with gzip.open(gzip_path, "wb") as src:
                with open(target_path, "rb") as dest:
                    chunk = src.read(2097152)
                    while chunk != b'':
                        if halt_flag:
                            halt_flag.check_continue(True)
                        dest.write(chunk)
                        if halt_flag:
                            halt_flag.check_continue(True)
                        chunk = src.read(2097152)
            return target_path
        else:
            file_path = target_dir / target_name
            handle.download(file_path)
            return file_path

    @staticmethod
    def from_map(map_: dict, **kwargs):
        return FilePayload(
            file_info=FileInfo.from_map(map_),
            **kwargs
        )


class SourceFilePayload(WorkflowPayload):
    """Represent a source file in the database."""

    def __init__(self,
                 source_file_uuid: str,
                 received_date: datetime.date,
                 **kwargs):
        super().__init__(**kwargs)
        self.source_uuid = source_file_uuid
        self.received_date = received_date

    def load_source_file(self, db: NODBControllerInstance, **kwargs) -> structures.NODBSourceFile:
        """Load the referenced source file from the database."""
        source_file = structures.NODBSourceFile.find_by_uuid(
            db=db,
            source_uuid=self.source_uuid,
            received=self.received_date, **kwargs
        )
        if source_file is None:
            raise CNODCError('Invalid payload, no such UUID')
        return source_file

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

    @staticmethod
    def from_source_file(source_file: structures.NODBSourceFile):
        """Build a source file payload from a given source file."""
        return SourceFilePayload(
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date
        )


class BatchPayload(WorkflowPayload):
    """Represents a payload referencing an NODB batch."""

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

    def load_batch(self, db: NODBControllerInstance, **kwargs) -> structures.NODBBatch:
        """Load the referenced batch from the database."""
        batch = structures.NODBBatch.find_by_uuid(
            db=db,
            batch_uuid=self.batch_uuid, **kwargs
        )
        if batch is None:
            raise CNODCError('Invalid batch, no such UUID')
        return batch

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing uuid', 'PAYLOAD', 1009)
        return BatchPayload(
            map_['uuid'],
            **kwargs
        )

    @staticmethod
    def from_batch(batch: structures.NODBBatch):
        """Build a payload from a batch object"""
        return BatchPayload(batch.batch_uuid)


class ObservationPayload(WorkflowPayload):
    """A payload referencing a specific observation in the database."""

    def __init__(self,
                 item_uuid: str,
                 item_received: datetime.date,
                 **kwargs):
        super().__init__(**kwargs)
        self.uuid = item_uuid
        self.received_date = item_received

    def to_map(self):
        map_ = super().to_map()
        map_['item_info'] = {
            'uuid': self.uuid,
            'received': self.received_date.isoformat()
        }
        return map_

    def load_observation(self, db, **kwargs) -> structures.NODBObservation:
        """Find the related observation in the database"""
        obs = structures.NODBObservation.find_by_uuid(db, self.uuid, self.received_date, **kwargs)
        if obs is None:
            raise CNODCError('No such observation', 'PAYLOAD', 1010, is_recoverable=False)
        return obs

    def load_observation_data(self, db, **kwargs) -> structures.NODBObservationData:
        """Find the related observation data in the database"""
        obs_data = structures.NODBObservationData.find_by_uuid(db, self.uuid, self.received_date, **kwargs)
        if obs_data is None:
            raise CNODCError('No such observation data', 'PAYLOAD', 1011, is_recoverable=False)
        return obs_data

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing item uuid', 'PAYLOAD', 1005)
        if 'received' not in map_:
            raise CNODCError('Missing item received date', 'PAYLOAD', 1006)
        return ObservationPayload(
            map_['uuid'],
            datetime.date.fromisoformat(map_['received']),
            **kwargs
        )

    @staticmethod
    def from_observation(obs: t.Union[structures.NODBObservation, structures.NODBObservationData]):
        """Build a payload from an observation or observation data object."""
        return ObservationPayload(
            obs.obs_uuid,
            obs.received_date
        )


class WorkflowController:
    """Manages the flow of an object through a workflow based on its configuration."""

    nodb: NODBController = None
    storage: StorageController = None

    @injector.construct
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

    def handle_incoming_file(self, local_path: pathlib.Path, metadata: dict, post_hook: t.Optional[callable] = None, db: NODBControllerInstance = None):
        """Start the workflow for a file stored on the local hard disk."""
        if db is not None:
            self._handle_incoming_file(local_path, metadata, post_hook, db)
        else:
            with self.nodb as db:
                self._handle_incoming_file(local_path, metadata, post_hook, db)
                db.commit()

    def _handle_incoming_file(self, local_path: pathlib.Path, metadata: dict, post_hook: t.Optional[callable], db: NODBControllerInstance):
        """Start the workflow for a file."""
        self._log.debug(f"Processing file [{local_path}]")
        file_handles = []
        working_file = None
        # Add the default metadata
        self._extend_metadata(metadata)
        # Validate the upload
        self._validate_file_upload(local_path, metadata)
        # Upload the file to various locations and queue the working file
        self._upload_and_queue_file(local_path, metadata, post_hook, db)

    def _extend_metadata(self, metadata: dict):
        """Extend the input metadata with the default metadata"""
        if 'default_metadata' in self.config:
            for x in self.config['default_metadata']:
                if x not in metadata:
                    self._log.debug(f"Setting default metadata {x}={self.config['default_metadata'][x]}")
                    metadata[x] = self.config['default_metadata'][x]

    def _validate_file_upload(self, local_path: pathlib.Path, metadata: dict):
        """Validate the uploaded file"""
        if 'validation' in self.config:
            self._log.info(f"Validating uploaded file")
            dynamic_object(self.config['validation'])(local_path, metadata)

    def _upload_and_queue_file(self, local_path: pathlib.Path, metadata: dict, post_hook, db):
        """Upload the file and queue it if all succeed."""
        with tempfile.TemporaryDirectory() as td:
            gzip_made = False
            filename = self._determine_filename(metadata)
            gzip_filename = filename + ".gz"
            td = pathlib.Path(td)
            gzip_file = td / "bin.gz"
            file_handles = []
            with_gzip = False
            try:
                if 'working_target' in self.config:
                    with_gzip = 'gzip' in self.config['working_target'] and self.config['working_target']['gzip']
                    if with_gzip:
                        self._log.info(f"Creating gzipped version of local file")
                        haltable_gzip(local_path, gzip_file, halt_flag=self.halt_flag)
                        gzip_made = True
                        working_file, target_tier = self._handle_file_upload(gzip_file, gzip_filename, metadata, self.config['working_target'])
                    else:
                        working_file, target_tier = self._handle_file_upload(local_path, filename, metadata, self.config['working_target'])
                    file_handles.append((working_file, target_tier))
                if 'additional_targets' in self.config:
                    for target in self.config['additional_targets']:
                        if 'gzip' in target and target['gzip']:
                            if not gzip_made:
                                self._log.info(f"Creating gzipped version of local file")
                                haltable_gzip(local_path, gzip_file, halt_flag=self.halt_flag)
                                gzip_made = True
                            file_handles.append(self._handle_file_upload(gzip_file, gzip_filename, metadata, target))
                        else:
                            file_handles.append(self._handle_file_upload(local_path, filename, metadata, target))
                # NB: these are done in the try/except so that the file handles can be removed upon failure
                if working_file:
                    self._queue_working_file(working_file, metadata, gzip_filename if with_gzip else filename, with_gzip, db)
                if post_hook is not None:
                    post_hook(db)
                db.commit()
                # We save setting the tier for last because setting the tier on Blobs to ARCHIVE and then deleting them
                # comes with a cost. It is better to briefly upload them as HOT, confirm every is good, then set them
                # to archive later. Errors are logged here and failures ignored, so the tiers may need to be manually
                # set if there are errors setting the tier.
                fh = file_handles
                file_handles = []
                self._finish_file_handles(fh)
            except Exception as ex:
                for fh, _ in file_handles:
                    if fh is not None:
                        fh.remove()
                if isinstance(ex, CNODCError):
                    raise ex from ex
                else:
                    raise CNODCError(f"Exception while processing incoming file: {ex.__class__.__name__}: {str(ex)}", "WORKFLOW", 1000) from ex

    def _queue_working_file(self,
                            working_file: BaseStorageHandle,
                            metadata: dict,
                            filename: str,
                            with_gzip: bool,
                            db):
        """Queue the working file."""
        if self.has_more_steps(-1):
            if 'last-modified-time' in metadata and metadata['last-modified-time']:
                lmt = metadata['last-modified-time']
                del metadata['last-modified-time']
            else:
                lmt = datetime.datetime.now(datetime.timezone.utc).isoformat()
            file_info = FileInfo(
                working_file.path(),
                filename,
                with_gzip,
                lmt
            )
            payload = FilePayload(file_info, current_step=0, metadata=metadata, workflow_name=self.name)
            payload.set_unique_key(hashlib.md5(file_info.file_path.encode('utf-8', errors='replace')).hexdigest())
            self._queue_step(payload, db)

    def _finish_file_handles(self, file_handles):
        """Set the tier on all file handles."""
        closing_handles = file_handles
        for handle, tier in closing_handles:
            if handle is not None and tier is not None:
                try:
                    handle.set_tier(tier)
                except Exception as ex:
                    self._log.exception(f"Exception setting tier on [{handle.path()}] to [{tier}]")

    def queue_step(self,
                        payload: WorkflowPayload,
                        db: NODBControllerInstance = None):
        """Queue the payload for its given step."""
        if db is not None:
            self._queue_step(payload, db)
        else:
            with self.nodb as db:
                self._queue_step(payload, db)
                db.commit()

    def _queue_step(self,
                    payload: WorkflowPayload,
                    db: NODBControllerInstance):
        queue_info = self._get_step_info(payload.current_step)
        priority = None
        if 'priority' in queue_info and queue_info['priority']:
            try:
                priority = int(queue_info['priority'])
            except (ValueError, TypeError):
                self._log.exception(f"Invalid default priority for workflow step [{self.name}:{payload.current_step}]")
        payload.enqueue(db, queue_info['name'], priority)

    def has_more_steps(self, current_idx: int):
        """Check if there are more steps."""
        if 'processing_steps' not in self.config or not self.config['processing_steps']:
            return False
        if current_idx < -1 or current_idx >= len(self.config['processing_steps']):
            return False
        return True

    def _determine_filename(self, metadata: dict) -> str:
        """Determine the filename to save the uploaded file as."""
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
        """Get the necessary metadata for storage."""
        return {
            x: self._sanitize_storage_metadata(self._substitute_headers(str(templates[x]), metadata))
            for x in templates
        }

    def _sanitize_storage_metadata(self, s: str) -> str:
        """Santizie data for storage."""
        return quote(str(s), safe=VALID_METADATA_CHARACTERS)

    def _substitute_headers(self, s: str, metadata: dict) -> str:
        """Sanitize and substitute headers"""
        for h in metadata:
            s = s.replace("%{" + h.lower() + "}", metadata[h])
        return s.replace('${now}', datetime.datetime.now(datetime.timezone.utc).isoformat())

    def _sanitize_filename(self, filename: str) -> t.Optional[str]:
        """Sanitize a filename for storage on all systems."""
        filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])
        filename = filename.rstrip(".")
        if filename == "" or len(filename) > 255:
            return None
        check = filename if "." not in filename else filename[:filename.find(".")]
        if check in RESERVED_FILENAMES:
            return None
        return filename

    def _get_step_info(self, step_idx: int) -> dict:
        """Retrieve the step information at a given step."""
        if 'processing_steps' not in self.config or not self.config['processing_steps']:
            raise CNODCError('No processing steps defined', 'WORKFLOW', 1001)
        if step_idx < 0 or step_idx >= len(self.config['processing_steps']):
            raise CNODCError(f"Invalid step index [{step_idx}]", 'WORKFLOW', 1002)
        step_info = self.config['processing_steps'][step_idx]
        if isinstance(step_info, str):
            return {'name': step_info}
        return step_info

    def _handle_file_upload(self, local_path: pathlib.Path, filename: str, metadata: dict, upload_kwargs: dict) -> tuple[BaseStorageHandle, t.Optional[StorageTier]]:
        """Upload a file to a given location."""
        if 'directory' not in upload_kwargs:
            raise CNODCError("Missing directory for workflow upload action", "WORKFLOW", 1003)
        self._log.info(f"Uploading file to {upload_kwargs['directory']}")
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
