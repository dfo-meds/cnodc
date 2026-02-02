import datetime
import functools
import hashlib
import pathlib
import tempfile
import uuid
import typing as t

import zrlog

import cnodc.storage
from cnodc.nodb import NODBController
from cnodc.nodb.controller import NODBError, SqlState, ScannedFileStatus, NODBControllerInstance
from cnodc.process.scheduled_task import ScheduledTask
from cnodc.process.queue_worker import QueueWorker, QueueItemResult
from cnodc.workflow import WorkflowController
from cnodc.storage import StorageController
import cnodc.nodb.structures as structures
from cnodc.util import CNODCError, HaltFlag
from autoinject import injector


class FileScanTask(ScheduledTask):

    nodb: NODBController = None
    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name='file_scanner',
            process_version='1.0',
            defaults={
                'scan_target': None,
                'workflow_name': None,
                'queue_name': 'file_download',
                'pattern': '*',
                'recursive': False,
                'remove_downloaded_files': False,
                'reprocess_updated_files': False,
                'metadata': None
            },
            **kwargs
        )
        self._scan_target: t.Optional[cnodc.storage.BaseStorageHandle] = None

    def on_start(self):
        scan_target = self.get_config('scan_target')
        if scan_target is None:
            raise CNODCError(f'Scan target is not configured', 'FILE_SCAN', 1000)
        workflow_name = self.get_config('workflow_name')
        if workflow_name is None:
            raise CNODCError(f'Workflow name is not configured', 'FILE_SCAN', 1002)
        queue_name = self.get_config('queue_name')
        if queue_name is None:
            raise CNODCError(f'Queue name is not configured', 'FILE_SCAN', 1003)
        self._scan_target = self.storage.get_handle(scan_target, halt_flag=self._halt_flag)
        if self._scan_target is None:
            raise CNODCError(f'Scan target [{scan_target}] is not recognized', 'FILE_SCAN', 1001)

    def execute(self):
        self._log.debug(f'Scanning {self._scan_target.path()}')
        batch_id = str(uuid.uuid4())
        remove_when_complete = bool(self.get_config("remove_downloaded_files"))
        reprocess_updated_files = bool(self.get_config("reprocess_updated_files"))
        headers = self.get_config("metadata", {})
        with self.nodb as db:
            for file in self._scan_target.search(
                    self.get_config('pattern'),
                    self.get_config('recursive')):
                try:
                    full_path = file.path()
                    mod_time = file.modified_datetime() if reprocess_updated_files else None
                    status = db.scanned_file_status(full_path, mod_time)
                    if status == ScannedFileStatus.NOT_PRESENT:
                        self._log.info(f"Found new file {full_path} [{mod_time.isoformat() if mod_time else 'no-date'}]")
                        db.note_scanned_file(full_path, mod_time)
                        payload = {
                            'target_file': full_path,
                            'modified_time': mod_time.isoformat() if mod_time is not None else None,
                            'workflow_name': self.get_config('workflow_name'),
                            'remove_on_completion': remove_when_complete,
                            'metadata': headers,
                        }
                        payload['metadata'].update({
                            '_source_info': (self._process_name, self._process_version, self._process_uuid),
                            'scan_target': self._scan_target.path(),
                            'correlation_id': batch_id,
                            'scanned_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
                        })
                        db.create_queue_item(
                            queue_name=self.get_config('queue_name'),
                            data=payload,
                            unique_item_key=hashlib.md5(full_path.encode('utf-8', 'replace')).hexdigest()
                        )
                        db.commit()
                    else:
                        self._log.debug(f"Skipping old file {full_path}")
                except NODBError as ex:

                    # In all cases, we want to rollback
                    db.rollback()

                    # Serialization or unique key failure means we have one of two issues:
                    # - The file path was inserted between our own checking and inserting
                    # - The queue UUID was duplicated (unlikely)
                    # In either case, we can ignore it for now as long as we rollback.
                    # If the file doesn't get properly recorded, it will be checked on the next pass
                    if ex.sql_state() in (
                        SqlState.SERIALIZATION_FAILURE,
                        SqlState.DEADLOCK_DETECTED,
                        SqlState.UNIQUE_VIOLATION
                    ):
                        self._log.exception(f"Exception while creating database entry for scanned file")

                    # Other errors indicate a bigger issue, we want to raise those
                    else:
                        raise ex


class FileDownloadWorker(QueueWorker):

    storage: StorageController = None
    nodb: NODBController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name='file_downloader',
            process_version='1.0',
            defaults={
                'queue_name': 'file_download',
                'allow_file_deletes': False,
            },
            **kwargs
        )

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[QueueItemResult]:
        if 'target_file' not in item.data or not item.data['target_file']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [target_file]', 'FILEFLOW', 1000)
        if 'workflow_name' not in item.data or not item.data['workflow_name']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [workflow_name]', 'FILEFLOW', 1001)
        self.handle_queued_file(
            item.data['workflow_name'],
            item.data['target_file'],
            item.data['modified_time'] if 'modified_time' in item.data else None,
            item.data['metadata'] if 'metadata' in item.data else {},
            (self.get_config('allow_file_deletes') and bool(item.data['remove_on_completion'])) if 'remove_on_completion' in item.data else False
        )
        return QueueItemResult.SUCCESS

    def after_failure(self, item: structures.NODBQueueItem):
        if 'target_file' in item.data and item.data['target_file']:
            self._db.mark_scanned_item_failed(item.data['target_file'], datetime.datetime.fromisoformat(item.data['modified_time']) if 'modified_time' in item.data and item.data['modified_time'] else None)
            self._db.commit()

    def handle_queued_file(self,
                           workflow_name: str,
                           file_path: str,
                           modified_time: t.Optional[str],
                           metadata: dict,
                           remove_when_finished: bool = False):
        if modified_time:
            modified_time = datetime.datetime.fromisoformat(modified_time)
        with self.nodb as db:
            current_status = db.scanned_file_status(file_path, modified_time)
            if current_status == ScannedFileStatus.UNPROCESSED:
                handle = self.storage.get_handle(file_path, halt_flag=self._halt_flag)
                if not metadata:
                    metadata = {}
                self._update_payload_metadata(metadata, handle)
                workflow = structures.NODBUploadWorkflow.find_by_name(db, workflow_name)
                if workflow is None:
                    raise CNODCError(f'Workflow [{workflow_name}] not found', 'FILEFLOW', 1002, is_recoverable=True)
                wf_controller = WorkflowController(workflow_name, workflow.configuration, halt_flag=self._halt_flag)
                temp_dir = self.temp_dir()
                local_path = temp_dir / "file"
                handle.download(local_path)
                mod_time = handle.modified_datetime()
                wf_controller.handle_incoming_file(
                    local_path=local_path,
                    metadata=metadata,
                    post_hook=functools.partial(self._db.mark_scanned_item_success, file_path, mod_time),
                    unique_queue_id=file_path,
                    db=db
                )
                if remove_when_finished:
                    self._try_remove_file(handle)
            elif current_status == ScannedFileStatus.PROCESSED and remove_when_finished:
                self._log.info(f"Item {file_path} already processed [result {current_status}], checking for removal")
                handle = self.storage.get_handle(file_path, halt_flag=self._halt_flag)
                self._try_remove_file(handle)
            else:
                self._log.info(f"Item {file_path} already processed [result {current_status}], skipping")

    def _update_payload_metadata(self, metadata: dict, handle):
        metadata['source'] = (self._process_name, self._process_version, self._process_uuid)
        metadata['default-filename'] = handle.name()
        md = handle.modified_datetime()
        if md is None:
            if 'scanned_time' in metadata:
                metadata['last-modified-date'] = metadata['scanned-time']
            else:
                metadata['last-modified-date'] = datetime.datetime.now(datetime.timezone.utc)
        else:
            metadata['last-modified-date'] = md.isoformat()
        if 'correlation_id' not in metadata:
            metadata['correlation_id'] = str(uuid.uuid4())

    def _try_remove_file(self, handle):
        try:
            handle.remove()
        except Exception:
            self._log.exception(f"Error while removing file")
