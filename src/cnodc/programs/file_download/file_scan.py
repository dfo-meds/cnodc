import datetime
import gzip
import pathlib
import shutil
import tempfile
import uuid
import typing as t
from cnodc.nodb import NODBController
from cnodc.process.scheduled_task import ScheduledTask
from cnodc.process.queue_worker import QueueWorker
from cnodc.process.workflows import WorkflowController
from cnodc.storage import StorageController
import cnodc.nodb.structures as structures
from cnodc.util import CNODCError, HaltFlag
from autoinject import injector


class FileScanTask(ScheduledTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.file_scan", **kwargs)
        self._storage: t.Optional[StorageController] = None
        self._nodb: t.Optional[NODBController] = None
        self.set_defaults({
            'scan_target': None,
            'pattern': '*',
            'recursive': False
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
        handle = self._storage.get_handle(scan_target)
        if handle is None:
            raise CNODCError(f'Scan target [{scan_target}] is not recognized', 'FILE_SCAN', 1001)
        batch_id = str(uuid.uuid4())
        with self._nodb as db:
            for file in handle.search(
                    self.get_config('pattern'),
                    self.get_config('recursive'),
                    halt_flag=self.halt_flag):
                full_path = file.path()
                if db.scanned_file_exists(full_path):
                    continue
                db.note_scanned_file(full_path)
                db.create_queue_item(
                    queue_name,
                    {
                        'target_file': full_path,
                        'workflow_name': workflow_name,
                        'headers': {},
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


class FileScanWorker(QueueWorker):

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        if 'target_file' not in item.data or not item.data['target_file']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [target_file]', 'FILEFLOW', 1000)
        if 'workflow_name' not in item.data or not item.data['workflow_name']:
            raise CNODCError(f'Queue item [{item.queue_uuid}] missing [workflow_name]', 'FILEFLOW', 1001)
        wc = FileScanWorkflowController(item.data['workflow_name'], self.halt_flag)
        wc.handle_queued_file(
            item.data['target_file'],
            item.data['headers'] if 'headers' in item.data else {},
            item.data['_metadata'] if '_metadata' in item.data else {}
        )
        return structures.QueueItemResult.SUCCESS

    def after_failure(self, item: structures.NODBQueueItem):
        if 'target_file' in item.data and item.data['target_file']:
            self._db.mark_scanned_item_failed(item.data['target_file'])
            self._db.commit()

    def on_success(self, item: structures.NODBQueueItem):
        self._db.mark_scanned_item_success(item.data['target_file'])


class FileScanWorkflowController(WorkflowController):

    storage: StorageController = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._working_file = None
        self._gzip_file = None
        self._target_file = None
        self._metadata = {}
        self._temp_dir = None

    def get_working_file(self, gzip_working_file: bool = True) -> pathlib.Path:
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        if self._working_file is None:
            self._working_file = pathlib.Path(self._temp_dir.name) / "file"
        handle = self.storage.get_handle(self._target_file)
        handle.download(self._working_file, halt_flag=self._halt_flag)
        if gzip_working_file:
            if self._gzip_file is None:
                self._gzip_file = pathlib.Path(self._temp_dir.name) / "file.gzip"
                with gzip.open(self._gzip_file, "wb") as dest:
                    with open(self._working_file, "rb") as src:
                        shutil.copyfileobj(src, dest)
            return self._gzip_file
        return self._working_file

    def get_queue_metadata(self) -> dict:
        return {
            'source': 'file_scan',
            'correlation_id': self._metadata['correlation_id'] if 'correlation_id' in self._metadata else '',
            'workflow_name': self.workflow_name,
            'user': '',
            'upload_time': self._metadata['scanned_time'] if 'scanned_time' in self._metadata else datetime.datetime.now(datetime.timezone.utc)
        }

    def handle_queued_file(self, file_path: str, headers: dict, metadata: dict):
        try:
            with self.nodb as db:
                workflow = self._load_workflow(db)
                self._target_file = file_path
                self._metadata = metadata
                self._complete_request(db, workflow, headers or {})
        finally:
            self._cleanup_request()

    def _cleanup_request(self):
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
            self._working_file = None
            self._gzip_file = None
