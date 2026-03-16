import datetime

from cnodc.nodb import NODBQueueItem
from cnodc.programs.file_scan import FileScanTask
from helpers.base_test_case import BaseTestCase


class TestFileScanTask(BaseTestCase):

    def test_no_scan_target(self):
        x = self.worker_controller.build_test_worker(FileScanTask, {

        })
        with self.assertRaisesCNODCError('FILESCAN-1000'):
            x.on_start()

    def test_no_workflow_name(self):
        x = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir)
        })
        with self.assertRaisesCNODCError('FILESCAN-1002'):
            x.on_start()

    def test_no_queue_name(self):
        x = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'queue_name': '',
        })
        with self.assertRaisesCNODCError('FILESCAN-1003'):
            x.on_start()

    def test_bad_scan_target(self):
        x = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': 'protocol://test/files/',
            'workflow_name': 'test',
        })
        with self.assertRaisesCNODCError('FILESCAN-1001'):
            x.on_start()

    def test_scan_new_files(self):
        (self.temp_dir / 'file1.txt').touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
        })
        x.on_start()
        x.scan_files(self.db)
        self.assertEqual(1, len(self.db.table(NODBQueueItem.TABLE_NAME)))
        self.assertEqual(1, len(self.db._scanned_files))
        self.assertDictSimilar({
            'file_path': str(self.temp_dir / 'file1.txt'),
            'was_processed': False,
            'modified_date': None
        }, self.db._scanned_files[0])

    def test_scan_new_files_for_reprocessing(self):
        file = self.temp_dir / 'file1.txt'
        file.touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'reprocess_updated_files': True
        })
        x.on_start()
        x.scan_files(self.db)
        self.assertEqual(1, len(self.db.table(NODBQueueItem.TABLE_NAME)))
        self.assertEqual(1, len(self.db._scanned_files))
        self.assertDictSimilar({
            'file_path': str(file),
            'was_processed': False,
            'modified_date': datetime.datetime.fromtimestamp(file.stat().st_mtime).astimezone()
        }, self.db._scanned_files[0])

