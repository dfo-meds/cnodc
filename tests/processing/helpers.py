import datetime
import typing as t
import uuid

from cnodc.nodb import NODBQueueItem, QueueStatus
from cnodc.processing import BatchWorkflowWorker, WorkflowWorker, SourceWorkflowWorker, FileWorkflowWorker
from cnodc.processing.workers.payload_worker import ObservationWorkflowWorker
from cnodc.processing.workers.queue_worker import QueueWorker
from cnodc.processing.workers.scheduled_task import ScheduledTask
from cnodc.processing.workflow.payloads import WorkflowPayload
from core import BaseTestCase, InjectableDict
from cnodc.util.halts import DummyHaltFlag


class WorkerTestController:

    def __init__(self, db, halt):
        self._db = db
        self._halt = halt

    def payload_to_queue_item(self,
                              payload: WorkflowPayload,
                              queue_name: str = '',
                              priority: int = 0,
                              subqueue_name: t.Optional[str] = None,
                              unique_item_name: t.Optional[str] = None):
        return NODBQueueItem(
            is_new=False,
            queue_name=queue_name,
            subqueue_name=subqueue_name,
            unique_item_name=unique_item_name,
            data=payload.to_map(),
            priority=priority,
            queue_uuid=str(uuid.uuid4()),
            created_date=datetime.datetime.now(datetime.timezone.utc),
            modified_date=datetime.datetime.now(datetime.timezone.utc),
            status=QueueStatus.LOCKED,
            locked_since=datetime.datetime.now(datetime.timezone.utc),
            locked_by='test',
            escalation_level=0
        )

    def test_queue_worker(self, worker_cls: type, worker_config: dict, queue_item: NODBQueueItem):
        worker: QueueWorker = self.build_test_worker(worker_cls, worker_config)
        return self._test_harness(worker, self._test_queue_worker, queue_item)

    def _test_queue_worker(self, worker, queue_item):
        def retrieve_item():
            return queue_item
        worker._fetch_next_queue_item = retrieve_item
        worker._process_next_queue_item()

    def test_scheduled_task(self, worker_cls: type, worker_config: dict):
        worker: ScheduledTask = self.build_test_worker(worker_cls, worker_config)
        return self._test_harness(worker, self._test_scheduled_task)

    def _test_scheduled_task(self, worker):
        worker._run_scheduled_task(datetime.datetime.now(datetime.timezone.utc))

    def _test_harness(self, worker, action: callable, *args, **kwargs):
        exc = None
        try:
            worker.on_start()
            worker.before_cycle()
            action(worker, *args, **kwargs)
        except Exception as ex:
            exc = ex
            raise ex
        finally:
            worker.after_cycle()
            worker.on_exit(exc)
        return worker

    def build_test_worker(self, worker_cls: type, worker_config: dict = None):
        kwargs = {
            '_halt_flag': self._halt,
            '_end_flag': self._halt,
            '_process_uuid': str(uuid.uuid4()),
            '_config': worker_config or {},
        }
        if worker_cls in (BatchWorkflowWorker, WorkflowWorker, SourceWorkflowWorker, ObservationWorkflowWorker, FileWorkflowWorker, QueueWorker, ScheduledTask):
            kwargs['process_name'] = 'test'
            kwargs['process_version'] = '0.1'
        cls = worker_cls(**kwargs)
        cls._db = self._db
        return cls


class WorkerTestCase(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.halt_flag = DummyHaltFlag()
        cls.worker_controller = WorkerTestController(cls.db, cls.halt_flag)

    def tearDown(self, d: InjectableDict = None):
        super().tearDown()
        self.halt_flag.event.clear()
