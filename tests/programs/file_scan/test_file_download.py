from nodb import NODBQueueItem, NODBUploadWorkflow
from pipeman.processing.payloads import FilePayload, NewFilePayload
from pipeman.programs.file_scan import FileDownloadWorker
import medsutil.awaretime as awaretime
from medsutil.dynamic import dynamic_name
from tests.helpers.base_test_case import BaseTestCase


class TestFileDownloadWorker(BaseTestCase):

    def test_bad_queue_item_file_missing(self):
        sfp = NewFilePayload(file_path=None, filename=None)
        qi = self.worker_controller.payload_to_queue_item(sfp)
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)

    def test_bad_queue_item_bad_file(self):
        bad_files = [None, '']
        for x in bad_files:
            with self.subTest(bad_file=x):
                sfp = NewFilePayload(file_path=x, filename=None)
                qi = self.worker_controller.payload_to_queue_item(sfp)
                with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
                    self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)

    def test_bad_queue_item_workflow_missing(self):
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        self.assertEqual(1, len(self.db._scanned_files))
        sfp = NewFilePayload.from_path(test_file)
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, self.worker_controller.payload_to_queue_item(sfp))
        self.assertEqual(0, len(self.db._scanned_files))

    def test_bad_queue_item_workflow_empty_workflow(self):
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        bad_workflows = ['', None]
        for x in bad_workflows:
            with self.subTest(bad_workflow=x):
                self.db.reset()
                self.db.note_scanned_file(str(test_file.absolute()), m_time)
                sfp = NewFilePayload.from_path(test_file, workflow_name=x)
                qi = self.worker_controller.payload_to_queue_item(sfp)
                with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
                    self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
                self.assertEqual(0, len(self.db._scanned_files))

    def _build_good_workflow(self):
        subdir = self.temp_dir / 'subdir'
        subdir.mkdir()
        wf = NODBUploadWorkflow()
        wf.workflow_name = 'test'
        wf.set_config({
            'label': {'und': 'test'},
            'working_target': {
                'directory': str(subdir.absolute())
            },
            'processing_steps': {
                'step1': {'order': 1, 'name': 'foobar'}
            }
        })
        self.db.insert_object(wf)

    def test_new_item(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        sfp = NewFilePayload.from_path(test_file, workflow_name='test', remove_when_complete=True)
        qi = self.worker_controller.payload_to_queue_item(sfp)
        worker: FileDownloadWorker = self.worker_controller.test_queue_worker(FileDownloadWorker, {
            'allow_file_deletes': True,
        }, qi)
        self.assertEqual(1, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertTrue((self.temp_dir / 'subdir' / 'file1.txt').exists())
        item: NODBQueueItem = self.db.table(NODBQueueItem.TABLE_NAME)[0]
        file_path = str((self.temp_dir / 'subdir' / 'file1.txt').absolute())
        self.assertDictSimilar({
            '_cls_': dynamic_name(FilePayload),
            'workflow_name': 'test',
            'current_step': 'step1',
            'current_step_done': False,
            'metadata': {
                'last-modified-date': m_time.isoformat(),
                'source': worker.process_id,
                'default-filename': 'file1.txt',
            },
            'file_path': file_path,
            'filename': 'file1.txt',
            'is_gzipped': False,
            'last_modified_date': m_time,
            'worker_config': {},
            'cls_name': dynamic_name(FilePayload),
        }, item.data)
        self.assertFalse(test_file.exists())

    def test_item_already_processed(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        self.db.mark_scanned_item_success(str(test_file.absolute()), m_time)
        sfp = NewFilePayload.from_path(test_file, workflow_name='test')
        qi = self.worker_controller.payload_to_queue_item(sfp)
        with self.assertLogs('cnodc.worker.file_downloader', 'INFO'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertTrue(test_file.exists())

    def test_item_already_processed_remove_no_allow(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        self.db.mark_scanned_item_success(str(test_file.absolute()), m_time)
        sfp = NewFilePayload.from_path(test_file, workflow_name='test')
        qi = self.worker_controller.payload_to_queue_item(sfp)
        with self.assertLogs('cnodc.worker.file_downloader', 'INFO'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {
                'allow_file_deletes': True
            }, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertTrue(test_file.exists())

    def test_item_already_processed_remove(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        self.db.mark_scanned_item_success(str(test_file.absolute()), m_time)
        sfp = NewFilePayload.from_path(test_file, workflow_name='test', remove_when_complete=True)
        qi = self.worker_controller.payload_to_queue_item(sfp)
        with self.assertLogs('cnodc.worker.file_downloader', 'INFO'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {
                'allow_file_deletes': True
            }, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertFalse(test_file.exists())

    def test_item_bad_workflow(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        m_time = awaretime.from_timestamp(test_file.stat().st_mtime)
        self.db.note_scanned_file(str(test_file.absolute()), m_time)
        sfp = NewFilePayload.from_path(test_file, workflow_name='test2')
        qi = self.worker_controller.payload_to_queue_item(sfp)
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))