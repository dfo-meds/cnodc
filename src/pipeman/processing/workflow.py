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

from medsutil.exceptions import ex_pretty
from nodb.workflow import WorkflowConfiguration, WorkflowDirectory, ProcessingStep, OverwriteOption
from medsutil.storage import StorageController, StorageTier, FilePath
from autoinject import injector

from medsutil.storage.interface import StorageError, FeatureFlag
from pipeman.exceptions import CNODCError
from medsutil.halts import HaltFlag, gzip_with_halt
import typing as t
import pathlib
import datetime

from pipeman.processing.payloads import WorkflowPayload, FilePayload
import medsutil.awaretime as awaretime
if t.TYPE_CHECKING:
    import nodb.interface as interface

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
                 config: WorkflowConfiguration,
                 process_metadata: t.Optional[dict[str, str]] = None,
                 halt_flag: HaltFlag = None):
        self._process_metadata = process_metadata or {}
        self._step_list = None
        self.name: str = workflow_name
        self.config: WorkflowConfiguration = config
        self.halt_flag = halt_flag
        self._log = zrlog.get_logger(f"cnodc.workflow.{workflow_name}")

    def handle_incoming_file(self,
                             local_path: pathlib.Path,
                             metadata: dict[str, str],
                             db: interface.NODBInstance,
                             success_hook: t.Optional[t.Callable[[], None]] = None,
                             unique_queue_id: t.Optional[str] = None,
                             correlation_id: t.Optional[str] = None):
        """Start the workflow for a file."""
        self._log.debug(f"Processing file [%s]", local_path)
        # Add the default metadata
        self._extend_metadata(metadata)
        # Determine filename
        filename = self._determine_filename(metadata)
        # Validate the upload
        self._validate_file_upload(local_path, metadata, filename)
        # Upload the file to various locations and queue the working file
        self._upload_and_queue_file(local_path, metadata, success_hook, db, unique_queue_id, filename, correlation_id)

    def _extend_metadata(self, metadata: dict[str, str]):
        """Extend the input metadata with the default metadata"""
        self.config.extend_metadata(metadata)

    def _validate_file_upload(self, local_path: pathlib.Path, metadata: dict[str, str], filename: str):
        """Validate the uploaded file"""
        self.config.validate_upload(local_path, metadata, filename)

    def _upload_and_queue_file(self, local_path: pathlib.Path, metadata: dict[str, str], success_hook: t.Callable[[],None] | None, db: interface.NODBInstance, unique_queue_id: t.Optional[str], filename: str, correlation_id: t.Optional[str] = None):
        """Upload the file and queue it if all succeed."""

        with tempfile.TemporaryDirectory() as td:
            td = pathlib.Path(td)
            gzip_filename = filename + ".gz"
            gzip_file = td / "bin.gz"
            def _get_gzip_file():
                if not gzip_file.exists():
                    self._log.debug(f"Creating gzipped version of local file")
                    gzip_with_halt(local_path, gzip_file, halt_flag=self.halt_flag)
                return gzip_file, gzip_filename

            file_handles: list[tuple[FilePath, StorageTier | None, str]] = []
            try:
                for target in (self.config.working_target, *self.config.additional_targets):
                    if target is None:
                        continue
                    if target.gzip:
                        lp, fn = _get_gzip_file()
                    else:
                        lp, fn = local_path, filename
                    file_handles.append(self._handle_file_upload(lp, fn, metadata, target))

                # NB: these are done in the try/except so that the file handles can be removed upon failure
                if self.config.working_target is not None:
                    self._queue_working_file(file_handles[0][0], metadata, file_handles[0][2], self.config.working_target.gzip, db, unique_queue_id, correlation_id)
                if success_hook is not None:
                    success_hook()
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
                    raise
                else:
                    raise CNODCError(f"Exception while processing incoming file: {ex_pretty(ex)}", "WORKFLOW", 1000) from ex

    def _queue_working_file(self,
                            working_file: FilePath,
                            metadata: dict[str, str],
                            filename: str,
                            with_gzip: bool,
                            db: interface.NODBInstance,
                            unique_file_key: t.Optional[str] = None,
                            correlation_id: t.Optional[str] = None):
        """Queue the working file."""
        if self.has_more_steps(None):
            if 'last-modified-date' in metadata and metadata['last-modified-date']:
                lmd = awaretime.from_isoformat(metadata['last-modified-date'])
            else:
                lmd = awaretime.utc_now()
                metadata['last-modified-date'] = lmd.isoformat()
            payload = FilePayload(
                file_path=working_file.path(),
                filename=filename,
                is_gzipped=with_gzip,
                last_modified_date=lmd,
                current_step=None,
                current_step_done=True,
                metadata=metadata,
                workflow_name=self.name,
                correlation_id=correlation_id or str(uuid.uuid4()),
                deduplicate_key=unique_file_key or hashlib.md5(working_file.path().encode('utf-8', errors='replace')).hexdigest()
            )
            self.queue_step(payload, db)
        else:
            self._log.info('No more steps for workflow')

    def _finish_file_handles(self, file_handles: list[tuple[FilePath, StorageTier | None, str]]) -> None:
        """Set the tier on all file handles."""
        closing_handles = file_handles
        for handle, tier, _ in closing_handles:
            if handle is not None and tier is not None:
                try:
                    self._log.debug('Setting tier on [%s] to [%s]', handle.path(), tier)
                    handle.set_tier(tier)
                except StorageError:
                    self._log.exception(f"Exception setting tier on [%s] to [%s]", handle.path(), tier)

    def queue_step(self,
                   payload: WorkflowPayload,
                   db: interface.NODBInstance):
        if payload.current_step_done:
            payload.current_step = self._get_next_step(payload.current_step)
        payload.current_step_done = False
        queue_info = self._get_step_info(payload.current_step)
        payload.worker_config.update(queue_info.worker_config)
        self._log.info('Queuing item to [%s]', queue_info.name)
        payload.enqueue(db, queue_info.name, queue_info.priority or 0)

    def step_list(self) -> list[str]:
        if self._step_list is None:
            self._step_list = self.config.ordered_steps() or []
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

    def _get_step_info(self, step_name: str) -> ProcessingStep:
        self._validate_step(step_name)
        return self.config.steps[step_name]

    def has_more_steps(self, current_step: t.Optional[str]):
        """Check if there are more steps."""
        _, has_more = self._validate_step(current_step)
        return has_more

    def _determine_filename(self, metadata: dict) -> str:
        """Determine the filename to save the uploaded file as."""
        filename = None
        if self.config.filename_pattern:
            filename = WorkflowController._sanitize_filename(WorkflowController._substitute_headers(self.config.filename_pattern, metadata))
        if filename is None and 'filename' in metadata and self.config.accept_user_filename:
            filename = WorkflowController._sanitize_filename(metadata['filename'])
        if filename is None and '-system-filename' in metadata:
            filename = WorkflowController._sanitize_filename(metadata['-system-filename'])
        if filename is None:
            filename = str(uuid.uuid4())
        return filename

    @staticmethod
    def _get_storage_metadata(templates: dict[str, str], metadata: dict[str, str]) -> dict[str, str]:
        """Get the necessary metadata for storage."""
        return {
            x: WorkflowController._sanitize_storage_metadata(WorkflowController._substitute_headers(str(templates[x]), metadata))
            for x in templates
        }

    @staticmethod
    def _sanitize_storage_metadata(s: t.Any) -> str:
        """Santizie data for storage."""
        return quote(str(s), safe=VALID_METADATA_CHARACTERS)

    @staticmethod
    def _substitute_headers(s: str, metadata: dict[str, str], _now: t.Optional[datetime.datetime] = None) -> str:
        """Sanitize and substitute headers"""
        for h in metadata:
            s = s.replace("%{" + h.lower() + "}", str(metadata[h]))
        n = _now or awaretime.utc_now()
        return s.replace('%{now}', n.isoformat())

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

    def _handle_file_upload(self,
                            local_path: pathlib.Path,
                            filename: str,
                            metadata: dict[str, str],
                            upload_info: WorkflowDirectory,
                            gzip: bool = False) -> tuple[FilePath, t.Optional[StorageTier], str]:
        """Upload a file to a given location."""
        target_dir_handle = self.storage.get_filepath(upload_info.directory, halt_flag=self.halt_flag)
        if target_dir_handle is None:
            raise CNODCError(f'Invalid directory [{upload_info.directory} for uploading', 'WORKFLOW', 1005)
        file_handle = target_dir_handle.child(filename)
        storage_tier = upload_info.tier
        if not file_handle.supports_feature(FeatureFlag.TIERING):
            storage_tier = None
        storage_metadata = self.storage.build_metadata(
            gzip=gzip,
            storage_tier=storage_tier,
        )
        storage_metadata.update(WorkflowController._get_storage_metadata(upload_info.metadata, metadata))

        allow_overwrite = metadata['allow-overwrite'] == '1' if 'allow-overwrite' in metadata else False

        if upload_info.allow_overwrite is OverwriteOption.NEVER:
            allow_overwrite = False
        elif upload_info.allow_overwrite is OverwriteOption.ALWAYS:
            allow_overwrite = True

        self._log.info(f"Uploading file to [%s]", file_handle.path())
        file_handle.upload(
            local_path,
            allow_overwrite=allow_overwrite,
            storage_tier=StorageTier.FREQUENT,
            metadata=storage_metadata
        )

        if storage_tier is None or storage_tier == StorageTier.FREQUENT:
            return file_handle, None, filename
        else:
            return file_handle, storage_tier, filename
