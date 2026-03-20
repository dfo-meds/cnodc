import datetime
import typing as t

from cnodc.nodb import NODBBatch, NODBSourceFile, NODBQueueItem, QueueStatus
from cnodc.nodb.controller import NODBError
from cnodc.processing.workers.payload_worker import BatchWorkflowWorker, WorkflowWorker, SourceWorkflowWorker, FileWorkflowWorker
from cnodc.processing.control.base import SaveData
from cnodc.processing.workers.payload_worker import ObservationWorkflowWorker
from cnodc.processing.workers.queue_worker import QueueWorker, QueueItemResult
from cnodc.processing.workers.scheduled_task import ScheduledTask
from cnodc.processing.workflow.payloads import FilePayload, FileInfo, BatchPayload, WorkflowPayload, SourceFilePayload, \
    ObservationPayload
from cnodc.util import CNODCError, HaltInterrupt
from helpers.base_test_case import BaseTestCase


class BoringQueueWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(process_name='test', process_version='1.0', **kwargs)
        self._called_methods = set()
        self._ret_value = None

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        if self._ret_value is None or isinstance(self._ret_value, QueueItemResult):
            return self._ret_value
        raise self._ret_value

    def autocomplete(self, queue_item):
        self._called_methods.add('autocomplete')
        super().autocomplete(queue_item)

    def before_cycle(self):
        self._called_methods.clear()
        self._called_methods.add('before_cycle')
        super().before_cycle()

    def after_cycle(self):
        self._called_methods.add('after_cycle')
        super().after_cycle()

    def on_retry(self, queue_item: NODBQueueItem):
        self._called_methods.add('on_retry')

    def on_failure(self, queue_item: NODBQueueItem):
        self._called_methods.add('on_failure')

    def on_success(self, queue_item: NODBQueueItem):
        self._called_methods.add('on_success')

    def after_retry(self, queue_item: NODBQueueItem):
        self._called_methods.add('after_retry')

    def after_failure(self, queue_item: NODBQueueItem):
        self._called_methods.add('after_failure')

    def after_success(self, queue_item: NODBQueueItem):
        self._called_methods.add('after_success')

class BoringTask(ScheduledTask):

    def __init__(self, **kwargs):
        super().__init__(process_name='test', process_version='1.0', **kwargs)
        self._called_methods = set()
        self._ret_value = None

    def execute(self):
        if self._ret_value is None:
            return self._ret_value
        raise self._ret_value

    def before_cycle(self):
        self._called_methods.clear()
        self._called_methods.add('before_cycle')
        super().before_cycle()

    def after_cycle(self):
        self._called_methods.add('after_cycle')
        super().after_cycle()


class TestScheduledTask(BaseTestCase):

    def test_cron_mode(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'cron'
        })
        with self.assertRaises(CNODCError):
            task.on_start()

    def test_bad_mode(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'foobar'
        })
        with self.assertRaises(CNODCError):
            task.on_start()

    def test_bad_delay_seconds(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 'fifty',
        })
        with self.assertRaises(CNODCError):
            task.on_start()

    def test_bad_delay_fuzz(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': '50',
            'delay_fuzz_milliseconds': 'fifty',
        })
        with self.assertRaises(CNODCError):
            task.on_start()

    def test_execution_delay(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': '50',
            'delay_fuzz_milliseconds': '0',
        })
        for _ in range(0, 50):
            self.assertEqual(50, task._execution_delay().total_seconds())

    def test_execution_delay_fuzz(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': '50',
            'delazy_fuzz_milliseconds': '1000',
        })
        for _ in range(0, 1000):
            dt = task._execution_delay(False).total_seconds()
            self.assertGreaterEqual(dt, 49.0)
            self.assertLessEqual(dt, 51.0)

    def test_execution_delay_on_boot(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': '50',
            'run_on_boot': True,
            'delay_fuzz_milliseconds': '0',
        })
        self.assertEqual(50, task._execution_delay(False).total_seconds())
        self.assertEqual(0, task._execution_delay(True).total_seconds())

    def test_check_execution(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 60,
        })
        task.on_start()
        self.assertTrue(task._check_execution(task._next_execution))
        self.assertTrue(task._check_execution(task._next_execution + datetime.timedelta(seconds=1)))
        self.assertFalse(task._check_execution(task._next_execution - datetime.timedelta(seconds=1)))

    def test_from_start_fresh(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 60,
            'delay_fuzz_milliseconds': '0',
        })
        task.on_start()
        dt = datetime.datetime.now(datetime.timezone.utc)
        self.assertLessEqual(task._sleep_time(dt), 60)

    def test_from_start_saved(self):
        dt = datetime.datetime.fromisoformat('2015-01-02T03:04:05+00:00')
        sf = self.temp_dir / 'save.dat'
        sd = SaveData(sf)
        sd['last_start'] = '2015-01-02T03:04:05+00:00'
        sd.save_file()
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 60,
            'delay_fuzz_milliseconds': '0',
            'save_file': str(sf),
        })
        task.on_start()
        self.assertLessEqual(task._sleep_time(dt), 59.75)

    def test_from_end_fresh(self):
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_completion',
            'delay_seconds': 60,
            'delay_fuzz_milliseconds': '0',
        })
        task.on_start()
        dt = datetime.datetime.now(datetime.timezone.utc)
        self.assertLessEqual(task._sleep_time(dt), 59.75)

    def test_from_end_saved(self):
        dt = datetime.datetime.fromisoformat('2015-01-02T03:04:05+00:00')
        sf = self.temp_dir / 'save.dat'
        sd = SaveData(sf)
        sd['last_end'] = '2015-01-02T03:04:05+00:00'
        sd.save_file()
        task: ScheduledTask = self.worker_controller.build_test_worker(ScheduledTask, {
            'schedule_mode': 'from_completion',
            'delay_seconds': 60,
            'delay_fuzz_milliseconds': '0',
            'save_file': str(sf),
        })
        task.on_start()
        self.assertEqual(task._sleep_time(dt), 59.75)

    def test_basic_run(self):
        sf = self.temp_dir / 'save.dat'
        task: BoringTask = self.worker_controller.build_test_worker(BoringTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 30,
            'delay_fuzz_milliseconds': '0',
            'save_file': str(sf)
        })
        task.on_start()
        dt = datetime.datetime.now(datetime.timezone.utc)
        task._next_execution = dt
        self.assertEqual(0, len(task._called_methods))
        self.assertIsInstance(task._run_scheduled_task(dt), datetime.datetime)
        self.assertEqual(task.save_data['last_start'], dt.isoformat())
        self.assertGreaterEqual(datetime.datetime.fromisoformat(task.save_data['last_end']), dt)
        self.assertNotEqual(task._next_execution, dt)
        self.assertTrue(sf.exists())
        self.assertEqual(2, len(task._called_methods))
        self.assertIn('before_cycle', task._called_methods)
        self.assertIn('after_cycle', task._called_methods)

    def test_raise_recoverable(self):
        sf = self.temp_dir / 'save.dat'
        task: BoringTask = self.worker_controller.build_test_worker(BoringTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 30,
            'delay_fuzz_milliseconds': '0',
            'save_file': str(sf)
        })
        task.on_start()
        task._ret_value = CNODCError('one', 'one', 1, True)
        dt = datetime.datetime.now(datetime.timezone.utc)
        task._next_execution = dt
        self.assertEqual(0, len(task._called_methods))
        with self.assertLogs('cnodc.worker.test', 'ERROR'):
            self.assertIsInstance(task._run_scheduled_task(dt), datetime.datetime)
        self.assertEqual(task.save_data['last_start'], dt.isoformat())
        self.assertGreaterEqual(datetime.datetime.fromisoformat(task.save_data['last_end']), dt)
        self.assertNotEqual(task._next_execution, dt)
        self.assertTrue(sf.exists())
        self.assertEqual(2, len(task._called_methods))
        self.assertIn('before_cycle', task._called_methods)
        self.assertIn('after_cycle', task._called_methods)

    def test_raise_non_recoverable(self):
        sf = self.temp_dir / 'save.dat'
        task: BoringTask = self.worker_controller.build_test_worker(BoringTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 30,
            'delay_fuzz_milliseconds': '0',
            'save_file': str(sf)
        })
        task.on_start()
        task._ret_value = CNODCError('one', 'one', 1, False)
        dt = datetime.datetime.now(datetime.timezone.utc)
        task._next_execution = dt
        self.assertEqual(0, len(task._called_methods))
        with self.assertRaises(CNODCError):
            task._run_scheduled_task(dt)
        self.assertEqual(task.save_data['last_start'], dt.isoformat())
        self.assertGreaterEqual(datetime.datetime.fromisoformat(task.save_data['last_end']), dt)
        self.assertNotEqual(task._next_execution, dt)
        self.assertTrue(sf.exists())
        self.assertEqual(2, len(task._called_methods))
        self.assertIn('before_cycle', task._called_methods)
        self.assertIn('after_cycle', task._called_methods)

    def test_actual_delay(self):
        task: BoringTask = self.worker_controller.build_test_worker(BoringTask, {
            'schedule_mode': 'from_start',
            'delay_seconds': 2,
            'run_on_boot': False,
            'delay_fuzz_milliseconds': '0',
        })
        task.on_start()
        task._ret_value = CNODCError('one', 'one', 1, False)
        self.assertEqual(0, len(task._called_methods))
        with self.assertLogs("cnodc.worker.test", "ERROR"):
            task.run()

class TestQueueWorker(BaseTestCase):

    def test_no_queue_name_error(self):
        worker: QueueWorker = self.worker_controller.build_test_worker(QueueWorker)
        with self.assertRaises(CNODCError):
            worker.on_start()
        worker._config['queue_name'] = 'foobar'
        worker.on_start()
        self.assertIsNotNone(worker._app_id)
        self.assertIsNotNone(worker._current_delay_time)

    def test_delay_time(self):
        worker: QueueWorker = self.worker_controller.build_test_worker(QueueWorker, {
            'delay_time_seconds': 5,
            'delay_factor': 2,
            'max_delay_time_seconds': 30,
            'queue_name': 'foobar'
        })
        worker.on_start()
        self.assertEqual(worker._delay_time(), 5)
        self.assertEqual(worker._current_delay_time, 10)
        self.assertEqual(worker._delay_time(), 10)
        self.assertEqual(worker._current_delay_time, 20)
        self.assertEqual(worker._delay_time(), 20)
        self.assertEqual(worker._current_delay_time, 30)
        self.assertEqual(worker._delay_time(), 30)
        self.assertEqual(worker._delay_time(), 30)
        self.assertEqual(worker._delay_time(), 30)
        self.assertEqual(worker._delay_time(), 30)

    def test_process_queue_result_success(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(5, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('autocomplete', worker._called_methods)
        self.assertIn('on_success', worker._called_methods)
        self.assertIn('after_success', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.COMPLETE)

    def test_process_queue_result_explicit_success(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = QueueItemResult.SUCCESS
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(5, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('autocomplete', worker._called_methods)
        self.assertIn('on_success', worker._called_methods)
        self.assertIn('after_success', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.COMPLETE)

    def test_process_queue_result_explicit_handled(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = QueueItemResult.HANDLED
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_success', worker._called_methods)
        self.assertIn('after_success', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.LOCKED)

    def test_process_queue_result_explicit_retry(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = QueueItemResult.RETRY
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_retry', worker._called_methods)
        self.assertIn('after_retry', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.UNLOCKED)

    def test_process_queue_result_explicit_error(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = QueueItemResult.FAILED
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_failure', worker._called_methods)
        self.assertIn('after_failure', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.ERROR)

    def test_process_error_fetching_item(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        with self.assertLogs('cnodc.worker.test', 'ERROR'):
            worker._process_result(None, QueueItemResult.FAILED, ValueError('Bad Connection'))
        self.assertEqual(0, len(worker._called_methods))

    def test_process_queue_result_halt_interrupt(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = HaltInterrupt
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        with self.assertRaises(HaltInterrupt):
            with self.assertLogs('cnodc.worker.test', 'CRITICAL'):
                worker._process_next_queue_item()
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_retry', worker._called_methods)
        self.assertIn('after_retry', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.UNLOCKED)

    def test_process_queue_result_recoverable(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = CNODCError("test", "TEST", 1, True)
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        with self.assertLogs(f"cnodc.worker.test", 'ERROR'):
            self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_retry', worker._called_methods)
        self.assertIn('after_retry', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.UNLOCKED)

    def test_process_queue_result_unrecoverable(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = CNODCError("test", "TEST", 1, False)
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        with self.assertLogs(f"cnodc.worker.test", 'ERROR'):
            self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_failure', worker._called_methods)
        self.assertIn('after_failure', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.ERROR)

    def test_process_queue_result_rollback(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = NODBError("hello", "1", "2")
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        self.assertFalse(self.db._rolled_back)
        with self.assertLogs(f"cnodc.worker.test", 'ERROR'):
            self.assertTrue(worker._process_next_queue_item())
        self.assertTrue(self.db._rolled_back)
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_failure', worker._called_methods)
        self.assertIn('after_failure', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.ERROR)

    def test_process_queue_result_other_exc(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        worker._ret_value = ValueError("bar")
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        self.assertEqual(0, len(worker._called_methods))
        with self.assertLogs(f"cnodc.worker.test", 'ERROR'):
            self.assertTrue(worker._process_next_queue_item())
        self.assertEqual(4, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('on_failure', worker._called_methods)
        self.assertIn('after_failure', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.ERROR)

    def test_process_queue_result_no_item(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello'
        })
        self.assertEqual(0, len(worker._called_methods))
        self.assertFalse(worker._process_next_queue_item())
        self.assertEqual(2, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)

    def test_run_loop_no_item(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello',
            'delay_time_seconds': 0.25,
            'delay_factor': 2,
        })
        worker.on_start()
        self.assertEqual(worker._current_delay_time, 0.25)
        self.assertEqual(0, len(worker._called_methods))
        worker._run_once()
        self.assertEqual(worker._current_delay_time, 0.5)
        self.assertEqual(2, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)

    def test_run_loop_items(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello',
            'delay_time_seconds': 0.25,
            'delay_factor': 2,
        })
        worker.on_start()
        self.assertEqual(worker._current_delay_time, 0.25)
        self.assertEqual(0, len(worker._called_methods))
        worker._run_once()
        self.assertEqual(worker._current_delay_time, 0.5)
        self.assertEqual(2, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)

        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        obj = self.db.load_object(NODBQueueItem, {'queue_name': 'hello'})
        self.assertIsNotNone(obj)
        worker._run_once()
        self.assertEqual(worker._current_delay_time, 0.25)
        self.assertEqual(5, len(worker._called_methods))
        self.assertIn('before_cycle', worker._called_methods)
        self.assertIn('autocomplete', worker._called_methods)
        self.assertIn('on_success', worker._called_methods)
        self.assertIn('after_success', worker._called_methods)
        self.assertIn('after_cycle', worker._called_methods)
        self.assertEqual(obj.status, QueueStatus.COMPLETE)

    def test_full_run(self):
        worker: BoringQueueWorker = self.worker_controller.build_test_worker(BoringQueueWorker, {
            'queue_name': 'hello',
            'delay_time_seconds': 0.25,
            'delay_factor': 2,
        })
        worker.nodb = self.nodb
        worker._ret_value = HaltInterrupt
        self.db.create_queue_item(data={'foobar': 'hello'}, queue_name='hello')
        with self.assertLogs("cnodc.worker.test", "CRITICAL"):
            with self.assertRaises(HaltInterrupt):
                worker.run()


class TestWorkflowWorker(BaseTestCase):

    def test_build_batch_payload_from_uuid(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        bp = worker.batch_payload_from_uuid('12345')
        self.assertEqual(bp.batch_uuid, '12345')
        self.assertEqual(bp.get_metadata('hello'), 'world')

    def test_build_batch_payload_from_nodb(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        bp = worker.batch_payload_from_nodb(NODBBatch(batch_uuid="123456", is_new=False))
        self.assertEqual(bp.batch_uuid, '123456')
        self.assertEqual(bp.get_metadata('hello'), 'world')

    def test_build_file_payload_from_path(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        fp2 = worker.file_payload_from_path('/hello/world2', datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertEqual(fp2.file_info.file_path, '/hello/world2')
        self.assertEqual(fp2.file_info.last_modified_date, datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertEqual(fp2.get_metadata('hello'), 'world')

    def test_build_source_payload_from_nodb(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        sp = worker.source_payload_from_nodb(NODBSourceFile(source_uuid="23456", received_date=datetime.date(2015, 1, 2)))
        self.assertEqual(sp.source_uuid, '23456')
        self.assertEqual(sp.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(sp.get_metadata('hello'), 'world')

    def test_progress_payload(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        self.assertFalse(worker._skip_autoprogress_payload)
        worker.progress_payload(None, 'hello_world', False)
        self.assertFalse(worker._skip_autoprogress_payload)
        item = self.db.fetch_next_queue_item('hello_world')
        self.assertIsNotNone(item)
        pl = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(pl, FilePayload)
        self.assertEqual(pl.get_metadata('hello'), 'world')
        self.assertEqual(pl.file_info.file_path, '/hello/world')

    def test_progress_payload_new_payload(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        bp = worker.batch_payload_from_uuid('12345')
        self.assertFalse(worker._skip_autoprogress_payload)
        worker.progress_payload(bp, 'hello_world', False)
        self.assertFalse(worker._skip_autoprogress_payload)
        item = self.db.fetch_next_queue_item('hello_world')
        self.assertIsNotNone(item)
        pl = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(pl, BatchPayload)
        self.assertEqual(pl.get_metadata('hello'), 'world')
        self.assertEqual(pl.batch_uuid, '12345')

    def test_progress_payload_skip(self):
        worker = self.worker_controller.build_test_worker(WorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.set_metadata('hello', 'world')
        worker.current_payload = fp
        bp = worker.batch_payload_from_uuid('12345')
        self.assertFalse(worker._skip_autoprogress_payload)
        worker.progress_payload(bp, 'hello_world', True)
        self.assertTrue(worker._skip_autoprogress_payload)
        item = self.db.fetch_next_queue_item('hello_world')
        self.assertIsNotNone(item)
        pl = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(pl, BatchPayload)
        self.assertEqual(pl.get_metadata('hello'), 'world')
        self.assertEqual(pl.batch_uuid, '12345')

class TestBatchWorker(BaseTestCase):

    def test_bad_payload_type(self):
        bw: BatchWorkflowWorker = self.worker_controller.build_test_worker(BatchWorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(CNODCError):
            bw.process_queue_item(qi)

    def test_good_payload_type(self):
        bw: BatchWorkflowWorker = self.worker_controller.build_test_worker(BatchWorkflowWorker)
        fp = BatchPayload('12345')
        fp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(NotImplementedError):
            bw.process_queue_item(qi)

    def test_download_to_temp(self):
        bw: BatchWorkflowWorker = self.worker_controller.build_test_worker(BatchWorkflowWorker)
        fp = BatchPayload('12345')
        bw.current_payload = fp
        try:
            with self.assertRaises(ValueError):
                bw.download_to_temp_file()
        finally:
            bw.after_cycle()


class TestSourceWorker(BaseTestCase):

    def test_bad_payload_type(self):
        bw: SourceWorkflowWorker = self.worker_controller.build_test_worker(SourceWorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(CNODCError):
            bw.process_queue_item(qi)

    def test_good_payload_type(self):
        with open(self.temp_dir / "12345.txt", "w") as h:
            h.write("hello world")
        sw: SourceWorkflowWorker = self.worker_controller.build_test_worker(SourceWorkflowWorker)
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.source_path = str(self.temp_dir / "12345.txt")
        sf.file_name = "hello.txt"
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        sp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(NotImplementedError):
            sw.process_queue_item(qi)

    def test_download_to_temp(self):
        with open(self.temp_dir / "12345.txt", "w") as h:
            h.write("hello world")
        sw: SourceWorkflowWorker = self.worker_controller.build_test_worker(SourceWorkflowWorker)
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.source_path = str(self.temp_dir / "12345.txt")
        sf.file_name = "hello.txt"
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        sw.current_payload = sp
        f = sw.download_to_temp_file()
        self.assertTrue(f.exists())
        with open(f, "r") as h:
            self.assertEqual(h.read(), "hello world")



class TestFileWorker(BaseTestCase):

    def test_good_payload_type(self):
        fw: FileWorkflowWorker = self.worker_controller.build_test_worker(FileWorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(NotImplementedError):
            fw.process_queue_item(qi)

    def test_bad_payload_type(self):
        fw: FileWorkflowWorker = self.worker_controller.build_test_worker(FileWorkflowWorker)
        bp = BatchPayload("12345")
        bp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(CNODCError):
            fw.process_queue_item(qi)

    def test_download_to_temp(self):
        with open(self.temp_dir / "12345.txt", "w") as h:
            h.write("hello world")
        fw: FileWorkflowWorker = self.worker_controller.build_test_worker(FileWorkflowWorker)
        fp = FilePayload(FileInfo(str(self.temp_dir / '12345.txt')))
        fw.current_payload = fp
        f = fw.download_to_temp_file()
        self.assertTrue(f.exists())
        with open(f, "r") as h:
            self.assertEqual(h.read(), "hello world")


class TestObservationWorker(BaseTestCase):

    def test_bad_payload_type(self):
        ow: ObservationWorkflowWorker = self.worker_controller.build_test_worker(ObservationWorkflowWorker)
        fp = FilePayload(FileInfo('/hello/world'))
        fp.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(CNODCError):
            ow.process_queue_item(qi)

    def test_good_payload_type(self):
        ow: ObservationWorkflowWorker = self.worker_controller.build_test_worker(ObservationWorkflowWorker)
        op = ObservationPayload('12345', datetime.date(2015, 1, 2))
        op.enqueue(self.db, 'hello')
        qi = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(qi)
        with self.assertRaises(NotImplementedError):
            ow.process_queue_item(qi)

    def test_download_to_temp(self):
        ow: ObservationWorkflowWorker = self.worker_controller.build_test_worker(ObservationWorkflowWorker)
        op = ObservationPayload('12345', datetime.date(2015, 1, 2))
        ow.current_payload = op
        with self.assertRaises(ValueError):
            ow.download_to_temp_file()
