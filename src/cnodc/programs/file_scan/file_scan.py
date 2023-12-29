import datetime
import pathlib
import tempfile
import uuid
import typing as t

import zrlog

from cnodc.nodb import NODBController
from cnodc.nodb.controller import NODBError, SqlState, ScannedFileStatus, NODBControllerInstance
from cnodc.process.scheduled_task import ScheduledTask
from cnodc.process.queue_worker import QueueWorker
from cnodc.workflow import WorkflowController
from cnodc.storage import StorageController
import cnodc.nodb.structures as structures
from cnodc.storage.base import StorageFileHandle
from cnodc.util import CNODCError, HaltFlag
from autoinject import injector


class FileScanTask(ScheduledTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.file_scanner", **kwargs)
        self._storage: t.Optional[StorageController] = None
        self._nodb: t.Optional[NODBController] = None
        self.set_defaults({
            'scan_target': None,
            'workflow_name': None,
            'pattern': '*',
            'recursive': False,
            'remove_downloaded_files': False,
            'headers': None
        })

    @injector.inject
    def on_start(self, storage: StorageController, nodb: NODBController):
        self._storage = storage
        self._nodb = nodb

    def execute(self):
        scan_target = self.get_config('scan_target')
        if scan_target is None:
            raise CNODCError(f'Scan target is not configured', 'FILE_SCAN', 1000)
        workflow_name = self.get_config('workflow_name')
        if workflow_name is None:
            raise CNODCError(f'Workflow name is not configured', 'FILE_SCAN', 1002)
        queue_name = self.get_config('queue_name')
        if queue_name is None:
            raise CNODCError(f'Queue name is not configured', 'FILE_SCAN', 1003)
        handle = self._storage.get_handle(scan_target, halt_flag=self.halt_flag)
        if handle is None:
            raise CNODCError(f'Scan target [{scan_target}] is not recognized', 'FILE_SCAN', 1001)
        batch_id = str(uuid.uuid4())
        remove_when_complete = bool(self.get_config("remove_scanned_files"))
        headers = self.get_config("headers", {})
        with self._nodb as db:
            for file in handle.search(
                    self.get_config('pattern'),
                    self.get_config('recursive')):
                try:
                    full_path = file.path()
                    status = db.scanned_file_status(full_path)
                    if status == ScannedFileStatus.NOT_PRESENT:
                        db.note_scanned_file(full_path)
                        db.create_queue_item(
                            queue_name,
                            {
                                'target_file': full_path,
                                'workflow_name': workflow_name,
                                'remove_on_completion': remove_when_complete,
                                'headers': headers,
                                '_metadata': {
                                    'process_uuid': self.process_uuid,
                                    'process_name': self.process_name,
                                    'scan_target': scan_target,
                                    'correlation_id': batch_id,
                                    'scanned_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
                                }
                            }
                        )
                        db.commit()
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


class FileScanWorker(QueueWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.file_downloader", **kwargs)

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        if 'target_file' not in item.data or not item.data['target_file']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [target_file]', 'FILEFLOW', 1000)
        if 'workflow_name' not in item.data or not item.data['workflow_name']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [workflow_name]', 'FILEFLOW', 1001)
        wc = FileScanWorkflowController(item.data['workflow_name'], self.halt_flag)
        wc.handle_queued_file(
            item.data['target_file'],
            item.data['headers'] if 'headers' in item.data else {},
            item.data['_metadata'] if '_metadata' in item.data else {},
            bool(item.data['remove_on_completion']) if 'remove_on_completion' in item.data else False
        )
        return structures.QueueItemResult.SUCCESS

    def after_failure(self, item: structures.NODBQueueItem):
        if 'target_file' in item.data and item.data['target_file']:
            self._db.mark_scanned_item_failed(item.data['target_file'])
            self._db.commit()

    def on_success(self, item: structures.NODBQueueItem):
        self._db.mark_scanned_item_success(item.data['target_file'])


class FileScanWorkflowController:

    storage: StorageController = None
    nodb: NODBController = None

    @injector.construct
    def __init__(self, workflow_name: str, halt_flag: HaltFlag = None):
        self.halt_flag = halt_flag
        self.workflow_name = workflow_name
        self._working_file = None
        self._target_file = None
        self._remote_path = None
        self._remote_handle: t.Optional[StorageFileHandle] = None
        self._metadata = {}
        self._temp_dir = None
        self._log = zrlog.get_logger("cnodc.file_scan_intake")

    def get_working_file(self) -> pathlib.Path:
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        if self._working_file is None:
            self._working_file = pathlib.Path(self._temp_dir.name) / "file"
        self._remote_handle.download(self._working_file)
        return self._working_file

    def handle_queued_file(self, file_path: str, headers: dict, metadata: dict, remove_when_finished: bool = False):
        try:
            with self.nodb as db:
                self._remote_path = file_path
                self._remote_handle = self.storage.get_handle(self._remote_path, halt_flag=self.halt_flag)
                current_status = db.scanned_file_status(self._remote_path)
                if current_status == ScannedFileStatus.UNPROCESSED:
                    if headers is None:
                        headers = {}
                        headers['default-filename'] = self._remote_handle.name()
                        headers['last-modified-date'] = self._remote_handle.modified_datetime()
                        if headers['last-modified-date'] is None and 'scanned_time' in metadata:
                            headers['last-modified-date'] = metadata['scanned-time']
                        headers['request-id'] = (
                            metadata['correlation_id']
                            if 'correlation_id' in metadata else
                            str(uuid.uuid4())
                        )
                    workflow = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
                    wf_controller = WorkflowController(self.workflow_name, workflow.configuration, {
                        'source': 'file_scan',
                        'correlation_id': headers['request-id'],
                        'upload_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }, self.halt_flag)
                    wf_controller.handle_incoming_file(
                        local_path=self.get_working_file(),
                        headers=headers,
                        post_hook=self.on_complete,
                        db=db
                    )
                    if remove_when_finished:
                        self._try_remove_file()
                elif current_status == ScannedFileStatus.PROCESSED and remove_when_finished and self._remote_handle.exists():
                    self._try_remove_file()
        finally:
            self._cleanup_request()

    def _try_remove_file(self):
        try:
            self._remote_handle.remove()
        except Exception:
            self._log.exception(f"Error while removing file")

    def _cleanup_request(self):
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
            self._working_file = None
            self._gzip_file = None

    def on_complete(self, db: NODBControllerInstance):
        db.mark_scanned_item_success(self._target_file)
