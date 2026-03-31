import datetime
import functools

from cnodc.nodb import NODBQueueItem
from cnodc.nodb.controller import NODBError, SqlState
from cnodc.programs.file_scan import FileScanTask
from cnodc.util.awaretime import awaretime
from helpers.base_test_case import BaseTestCase


def raise_exception(self, *args, ex, **kwargs):
    raise ex


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
        with self.assertRaisesCNODCError('STORAGE-9000'):
            x.on_start()

    def test_scan_new_files_full(self):
        (self.temp_dir / 'file1.txt').touch()
        self.worker_controller.test_scheduled_task(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': True,
        })
        self.assertEqual(1, len(self.db.table(NODBQueueItem.TABLE_NAME)))
        self.assertEqual(1, len(self.db._scanned_files))
        self.assertDictSimilar({
            'file_path': str(self.temp_dir / 'file1.txt'),
            'was_processed': False,
            'modified_date': None
        }, self.db._scanned_files[0])

    def test_scan_new_files(self):
        (self.temp_dir / 'file1.txt').touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': True,
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
            'reprocess_updated_files': True,
            'delay_seconds': 60,
            'run_on_boot': False,
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

    def test_scan_existing_file_incomplete(self):
        file = self.temp_dir / 'file1.txt'
        file.touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': False,
        })
        x.on_start()
        self.db.note_scanned_file(str(file), None)
        self.assertEqual(1, len(self.db._scanned_files))
        x.scan_files(self.db)
        self.assertEqual(0, len(self.db.table(NODBQueueItem.TABLE_NAME)))
        self.assertEqual(1, len(self.db._scanned_files))
        self.assertDictSimilar({
            'file_path': str(file),
            'was_processed': False,
            'modified_date': None
        }, self.db._scanned_files[0])

    def test_scan_existing_file_complete(self):
        file = self.temp_dir / 'file1.txt'
        file.touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': False,
        })
        x.on_start()
        self.db.note_scanned_file(str(file), None)
        self.db.mark_scanned_item_success(str(file), None)
        self.assertEqual(1, len(self.db._scanned_files))
        x.scan_files(self.db)
        self.assertEqual(0, len(self.db.table(NODBQueueItem.TABLE_NAME)))
        self.assertEqual(1, len(self.db._scanned_files))
        self.assertDictSimilar({
            'file_path': str(file),
            'was_processed': True,
            'modified_date': None
        }, self.db._scanned_files[0])

    def test_scan_new_file_other_error(self):
        file = self.temp_dir / 'file1.txt'
        file.touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': False,
        })
        x.on_start()
        old = self.db.note_scanned_file
        try:
            self.db.note_scanned_file = functools.partial(raise_exception, ex=NODBError('oh no', 999, ''))
            with self.assertRaisesCNODCError('NODB-999'):
                x.scan_files(self.db)
            self.assertEqual(0, len(self.db.table(NODBQueueItem.TABLE_NAME)))
            self.assertEqual(0, len(self.db._scanned_files))
        finally:
            self.db.note_scanned_file = old

    def test_scan_new_file_serialization_error(self):
        file = self.temp_dir / 'file1.txt'
        file.touch()
        x: FileScanTask = self.worker_controller.build_test_worker(FileScanTask, {
            'scan_target': str(self.temp_dir),
            'workflow_name': 'test',
            'delay_seconds': 60,
            'run_on_boot': False,
        })
        x.on_start()
        old = self.db.note_scanned_file
        for code in [SqlState.DEADLOCK_DETECTED, SqlState.SERIALIZATION_FAILURE, SqlState.UNIQUE_VIOLATION]:
            with self.subTest(error_code=code.value):
                try:
                    self.db.note_scanned_file = functools.partial(raise_exception, ex=NODBError('oh no', 998, code.value))
                    with self.assertLogs('cnodc.worker.file_scanner', 'WARNING'):
                        x.scan_files(self.db)
                    self.assertEqual(0, len(self.db.table(NODBQueueItem.TABLE_NAME)))
                    self.assertEqual(0, len(self.db._scanned_files))
                finally:
                    self.db.note_scanned_file = old