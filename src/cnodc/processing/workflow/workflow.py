"""
    Workflows represent a series of steps that an input object (usually a file) will undergo once received.

    Each step has an input and output which is represented by a payload type. These payload types are:

    1. File (a physical file located on a computer). Workflows are started with a file payload
    2. SourceFile (a physical file referenced by an entry in the database).
    3. Batch (a set of one or more working records, referenced by their batch ID)
    4. Observation (a specific observation, which is a working record once finalized to the DB)
"""
import hashlib
import tempfile
import uuid
from urllib.parse import quote
import zrlog

from cnodc.nodb import NODBControllerInstance, NODBController, NODBUploadWorkflow
from cnodc.storage import StorageController, StorageTier, BaseStorageHandle
from autoinject import injector
from cnodc.util import CNODCError, HaltFlag, dynamic_object, gzip_with_halt
import typing as t
import pathlib
import datetime

from cnodc.processing.workflow.payloads import FileInfo, WorkflowPayload, FilePayload

VALID_METADATA_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_:;.,\\/\"'?!(){}[]@<>=-+*#$&`|~^"

VALID_FILENAME_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-."

RESERVED_FILENAMES = (
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
)


class WorkflowController:
    """Manages the flow of an object through a workflow based on its configuration.
     """

    storage: StorageController = None

    @injector.construct
    def __init__(self,
                 workflow_name: str,
                 config: dict,
                 process_metadata: t.Optional[dict] = None,
                 halt_flag: HaltFlag = None):
        self._process_metadata = process_metadata or {}
        self._step_list = None
        self.name: str = workflow_name
        self.config = config
        self.halt_flag = halt_flag
        self._log = zrlog.get_logger("cnodc.workflow")

    def handle_incoming_file(self, local_path: pathlib.Path, metadata: dict, post_hook: t.Optional[callable], db: NODBControllerInstance, unique_queue_id: t.Optional[str] = None):
        """Start the workflow for a file."""
        self._log.debug(f"Processing file [{local_path}]")
        file_handles = []
        working_file = None
        # Add the default metadata
        self._extend_metadata(metadata)
        # Validate the upload
        self._validate_file_upload(local_path, metadata)
        # Upload the file to various locations and queue the working file
        self._upload_and_queue_file(local_path, metadata, post_hook, db, unique_queue_id)

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

    def _upload_and_queue_file(self, local_path: pathlib.Path, metadata: dict, post_hook, db, unique_queue_id: t.Optional[str] = None):
        """Upload the file and queue it if all succeed."""
        with tempfile.TemporaryDirectory() as td:
            gzip_made = False
            filename = self._determine_filename(metadata)
            gzip_filename = filename + ".gz"
            td = pathlib.Path(td)
            gzip_file = td / "bin.gz"
            file_handles = []
            with_gzip = False
            working_file = None
            try:
                if 'working_target' in self.config:
                    with_gzip = 'gzip' in self.config['working_target'] and self.config['working_target']['gzip']
                    if with_gzip:
                        self._log.info(f"Creating gzipped version of local file")
                        gzip_with_halt(local_path, gzip_file, halt_flag=self.halt_flag)
                        gzip_made = True
                        working_file, target_tier = self._handle_file_upload(gzip_file, gzip_filename, metadata, self.config['working_target'], gzip=True)
                    else:
                        working_file, target_tier = self._handle_file_upload(local_path, filename, metadata, self.config['working_target'])
                    file_handles.append((working_file, target_tier))
                if 'additional_targets' in self.config:
                    for target in self.config['additional_targets']:
                        if 'gzip' in target and target['gzip']:
                            if not gzip_made:
                                self._log.info(f"Creating gzipped version of local file")
                                gzip_with_halt(local_path, gzip_file, halt_flag=self.halt_flag)
                                gzip_made = True
                            file_handles.append(self._handle_file_upload(gzip_file, gzip_filename, metadata, target, gzip=True))
                        else:
                            file_handles.append(self._handle_file_upload(local_path, filename, metadata, target))
                # NB: these are done in the try/except so that the file handles can be removed upon failure
                if working_file:
                    self._queue_working_file(working_file, metadata, gzip_filename if with_gzip else filename, with_gzip, db, unique_queue_id)
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
                            db,
                            unique_file_key: t.Optional[str] = None):
        """Queue the working file."""
        if self.has_more_steps(None):
            if 'last-modified-time' in metadata and metadata['last-modified-time']:
                lmt = datetime.datetime.fromisoformat(metadata['last-modified-time'])
            else:
                lmt = datetime.datetime.now(datetime.timezone.utc)
                metadata['last-modified-time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            file_info = FileInfo(
                working_file.path(),
                filename,
                with_gzip,
                lmt
            )
            payload = FilePayload(file_info, current_step=None, current_step_done=True, metadata=metadata, workflow_name=self.name)
            if unique_file_key:
                payload.set_unique_key(hashlib.md5(unique_file_key.encode('utf-8', errors='replace')).hexdigest())
            else:
                payload.set_unique_key(hashlib.md5(file_info.file_path.encode('utf-8', errors='replace')).hexdigest())
            self.queue_step(payload, db)

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
                    db: NODBControllerInstance):
        if payload.current_step_done:
            payload.current_step = self._get_next_step(payload.current_step)
        payload.current_step_done = False
        queue_info = self._get_step_info(payload.current_step)
        priority = None
        if 'priority' in queue_info and queue_info['priority']:
            try:
                priority = int(queue_info['priority'])
            except ValueError:
                self._log.error(f"Invalid default priority for workflow step [{self.name}:{payload.current_step}]")
            except TypeError:
                self._log.error(f"Invalid default priority for workflow step [{self.name}:{payload.current_step}]")
        if 'worker_metadata' in queue_info and queue_info['worker_metadata']:
            payload.metadata.update(queue_info['worker_metadata'])
        payload.enqueue(db, queue_info['name'], priority)

    def step_list(self) -> list[str]:
        if self._step_list is None:
            self._step_list = []
            if 'processing_steps' in self.config and self.config['processing_steps']:
                self._step_list = NODBUploadWorkflow.build_ordered_processing_steps(self.config['processing_steps'])
        return self._step_list

    def _validate_step(self, step_name: t.Optional[str]) -> tuple[int, bool]:
        steps = self.step_list()
        if step_name is None:
            return -1, len(steps) > 0
        if step_name not in steps:
            raise CNODCError(f"Invalid step name [{step_name}]", "WORKFLOW", 1002)
        step_idx = steps.index(step_name)
        return step_idx, step_idx < (len(steps) - 1)

    def _get_next_step(self, current_step: t.Optional[str]):
        steps = self.step_list()
        current_idx, has_more = self._validate_step(current_step)
        if not has_more:
            raise CNODCError(f"No more steps for workflow [{self.name}] after [{current_step}]", "WORKFLOW", 1004)
        return steps[current_idx + 1]

    def _get_step_info(self, step_name: str) -> dict:
        self._validate_step(step_name)
        return self.config['processing_steps'][step_name]

    def has_more_steps(self, current_step: t.Optional[str]):
        """Check if there are more steps."""
        _, has_more = self._validate_step(current_step)
        return has_more

    def _determine_filename(self, metadata: dict) -> str:
        """Determine the filename to save the uploaded file as."""
        filename = None
        if 'filename_pattern' in self.config and self.config['filename_pattern']:
            filename = WorkflowController._sanitize_filename(WorkflowController._substitute_headers(self.config['filename_pattern'], metadata))
        if filename is None and 'filename' in metadata and 'accept_user_filename' in self.config and self.config['accept_user_filename']:
            filename = WorkflowController._sanitize_filename(metadata['filename'])
        if filename is None and 'default-filename' in metadata:
            filename = WorkflowController._sanitize_filename(metadata['default-filename'])
        if filename is None:
            filename = str(uuid.uuid4())
        return filename

    @staticmethod
    def _get_storage_metadata(templates: dict[str, str], metadata: dict) -> dict:
        """Get the necessary metadata for storage."""
        return {
            x: WorkflowController._sanitize_storage_metadata(WorkflowController._substitute_headers(str(templates[x]), metadata))
            for x in templates
        }

    @staticmethod
    def _sanitize_storage_metadata(s: str) -> str:
        """Santizie data for storage."""
        return quote(str(s), safe=VALID_METADATA_CHARACTERS)

    @staticmethod
    def _substitute_headers(s: str, metadata: dict, _now: t.Optional[datetime.datetime] = None) -> str:
        """Sanitize and substitute headers"""
        for h in metadata:
            s = s.replace("%{" + h.lower() + "}", str(metadata[h]))
        _now = _now or datetime.datetime.now(datetime.timezone.utc)
        return s.replace('%{now}', _now.isoformat())

    @staticmethod
    def _sanitize_filename(filename: str) -> t.Optional[str]:
        """Sanitize a filename for storage on all systems."""
        filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])
        filename = filename.rstrip(".")
        if filename == "" or len(filename) > 255:
            return None
        check = filename if "." not in filename else filename[:filename.find(".")]
        if check in RESERVED_FILENAMES:
            return None
        return filename

    def _handle_file_upload(self, local_path: pathlib.Path, filename: str, metadata: dict, upload_kwargs: dict, gzip: bool = False) -> tuple[BaseStorageHandle, t.Optional[StorageTier]]:
        """Upload a file to a given location."""
        if 'directory' not in upload_kwargs or not upload_kwargs['directory']:
            raise CNODCError("Missing/invalid directory for workflow upload action", "WORKFLOW", 1003)
        self._log.info(f"Uploading file to {upload_kwargs['directory']}")
        target_dir_handle = self.storage.get_handle(upload_kwargs['directory'], halt_flag=self.halt_flag)
        if target_dir_handle is None:
            raise CNODCError(f'Invalid directory [{upload_kwargs['directory']} for uploading', 'WORKFLOW', 1005)
        file_handle = target_dir_handle.child(filename)

        storage_tier = StorageTier(upload_kwargs['tier']) if 'tier' in upload_kwargs and upload_kwargs['tier'] else None
        if not file_handle.supports_tiering():
            storage_tier = None

        storage_metadata = self.storage.build_metadata(
            gzip=gzip,
            storage_tier=storage_tier,
        )
        storage_metadata.update(WorkflowController._get_storage_metadata(
            upload_kwargs['metadata'] if 'metadata' in upload_kwargs and upload_kwargs['metadata'] else {},
            metadata
        ))

        allow_overwrite = metadata['allow-overwrite'] == '1' if 'allow-overwrite' in metadata else False
        if 'allow_overwrite' in upload_kwargs:
            if upload_kwargs['allow_overwrite'] == 'never':
                allow_overwrite = False
            elif upload_kwargs['allow_overwrite'] == 'always':
                allow_overwrite = True

        file_handle.upload(
            local_path,
            allow_overwrite=allow_overwrite,
            storage_tier=StorageTier.FREQUENT,
            metadata=storage_metadata
        )

        if storage_tier is None or storage_tier == StorageTier.FREQUENT:
            return file_handle, None
        else:
            return file_handle, storage_tier
