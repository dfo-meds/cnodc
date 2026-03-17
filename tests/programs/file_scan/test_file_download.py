import hashlib

from cnodc.nodb import NODBQueueItem, NODBUploadWorkflow
from cnodc.programs.file_scan import FileDownloadWorker
import cnodc.util.awaretime as awaretime
from helpers.base_test_case import BaseTestCase


class TestFileDownloadWorker(BaseTestCase):

    def test_bad_queue_item_file_missing(self):
        qi = NODBQueueItem()
        qi.data = {}
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)

    def test_bad_queue_item_bad_file(self):
        bad_files = [None, '']
        for x in bad_files:
            with self.subTest(bad_file=x):
                qi = NODBQueueItem()
                qi.data = {
                    'target_file': x
                }
                with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
                    self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)

    def test_bad_queue_item_workflow_missing(self):
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        self.db.note_scanned_file(str(test_file.absolute()), None)
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute())
        }
        self.assertEqual(1, len(self.db._scanned_files))
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
        self.assertEqual(0, len(self.db._scanned_files))

    def test_bad_queue_item_workflow_empty_workflow(self):
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        bad_workflows = ['', None]
        for x in bad_workflows:
            with self.subTest(bad_workflow=x):
                self.db.reset()
                n = awaretime.utc_now()
                self.db.note_scanned_file(str(test_file.absolute()), n)
                qi = NODBQueueItem()
                qi.data = {
                    'target_file': str(test_file.absolute()),
                    'workflow_name': x,
                    'modified_time': n.isoformat()
                }
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
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute()),
            'workflow_name': 'test',
            'modified_time': m_time.isoformat(),
            'remove_on_completion': True,
        }
        worker: FileDownloadWorker = self.worker_controller.test_queue_worker(FileDownloadWorker, {
            'allow_file_deletes': True,
        }, qi)
        self.assertEqual(1, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertTrue((self.temp_dir / 'subdir' / 'file1.txt').exists())
        item: NODBQueueItem = self.db.table(NODBQueueItem.TABLE_NAME)[0]
        file_path = str((self.temp_dir / 'subdir' / 'file1.txt').absolute())
        self.assertDictSimilar({
            'workflow': {
                'name': 'test',
                'step': 'step1',
                'step_done': False,
            },
            'metadata': {
                'last-modified-date': m_time.isoformat(),
                'unique-item-key': hashlib.md5(file_path.encode('utf-8', errors='replace')).hexdigest(),
                'source': ['file_downloader', '1.0', worker._process_uuid],
                'default-filename': 'file1.txt',
                'correlation-id': item.data['metadata']['correlation-id'],
                'queued-time': item.data['metadata']['queued-time'],
            },
            'file_info': {
                'file_path': file_path,
                'filename': 'file1.txt',
                'is_gzipped': False,
                'mod_date': m_time.isoformat()
            }
        }, item.data)
        self.assertFalse(test_file.exists())

    def test_item_already_processed(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        n = awaretime.utc_now()
        self.db.note_scanned_file(str(test_file.absolute()), n)
        self.db.mark_scanned_item_success(str(test_file.absolute()), n)
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute()),
            'workflow_name': 'test',
            'modified_time': n.isoformat()
        }
        with self.assertLogs('cnodc.worker.file_downloader', 'INFO'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertTrue(test_file.exists())

    def test_item_already_processed_remove_no_allow(self):
        self._build_good_workflow()
        test_file = self.temp_dir / 'file1.txt'
        test_file.touch()
        n = awaretime.utc_now()
        self.db.note_scanned_file(str(test_file.absolute()), n)
        self.db.mark_scanned_item_success(str(test_file.absolute()), n)
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute()),
            'workflow_name': 'test',
            'modified_time': n.isoformat(),
            'remove_on_completion': False
        }
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
        n = awaretime.utc_now()
        self.db.note_scanned_file(str(test_file.absolute()), n)
        self.db.mark_scanned_item_success(str(test_file.absolute()), n)
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute()),
            'workflow_name': 'test',
            'modified_time': n.isoformat(),
            'remove_on_completion': True
        }
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
        n = awaretime.utc_now()
        self.db.note_scanned_file(str(test_file.absolute()), n)
        qi = NODBQueueItem()
        qi.data = {
            'target_file': str(test_file.absolute()),
            'workflow_name': 'test2',
            'modified_time': n.isoformat(),
        }
        with self.assertLogs('cnodc.worker.file_downloader', 'ERROR'):
            self.worker_controller.test_queue_worker(FileDownloadWorker, {}, qi)
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))