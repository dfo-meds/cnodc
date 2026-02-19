import pathlib
import shutil
import tempfile
import uuid
import unittest as ut
import typing as t
from queue import Queue

from cnodc.nodb import QueueStatus
import datetime
from cnodc.nodb.structures import NODBQueueItem
from autoinject import injector

from cnodc.process import BaseWorker, QueueWorker, ScheduledTask
from cnodc.util import HaltFlag
from cnodc.workflow.payloads import WorkflowPayload


@injector.injectable
class InjectableDict:

    def __init__(self):
        self.data = {}


class DatabaseMock:

    def __init__(self):
        self.tables: dict[str, list] = {}

    def reset(self):
        self.tables = {}

    def table(self, table_name: str):
        if table_name not in self.tables:
            self.tables[table_name] = []
        return self.tables[table_name]

    def fast_renew_queue_item(self, queue_uuid):
        return datetime.datetime.now(datetime.timezone.utc)

    def fast_update_queue_status(self, queue_uuid, new_status, release_at, reduce_priority, escalation_level):
        pass

    def stream_objects(self, cls, filters: t.Optional[dict] = None, order_by: t.Optional[t.Union[list[str], str]] = None, **kwargs):
        for idx in self._find_object_indexes(cls.TABLE_NAME, filters or {}, order_by):
            yield self.table(cls.TABLE_NAME)[idx]

    def count_objects(self, cls, filters: t.Optional[dict] = None, **kwargs):
        return len([x for x in self.stream_objects(cls, filters, **kwargs)])

    def bulk_update(self, cls, updates, key_field, key_values):
        raise NotImplementedError

    def update_object(self, obj):
        pass

    def insert_object(self, obj):
        for key in obj.get_primary_keys():
            if 'uuid' in key and getattr(obj, key) is None:
                setattr(obj, key, str(uuid.uuid4()))
        self.table(obj.get_table_name()).append(obj)

    def upsert_object(self, obj):
        if obj.is_new:
            self.insert_object(obj)
        else:
            self.update_object(obj)

    def load_object(self, cls, filters: dict, **kwargs):
        obj_idx = self._find_object_index(cls.TABLE_NAME, filters)
        if obj_idx is None:
            return None
        return self.table(cls.TABLE_NAME)[obj_idx]

    def delete_object(self, obj):
        filters = {
            key: getattr(obj, obj.get_for_db(key))
            for key in obj.get_primary_keys()
        }
        index = self._find_object_index(obj.get_table_name(), filters)
        if index is not None:
            self.table(obj.get_table_name()).pop(index)

    def _find_object_index(self, table_name, filters: dict, order_by=None):
        for idx in self._find_object_indexes(table_name, filters):
            return idx
        return None

    def _find_object_indexes(self, table_name, filters: dict, order_by=None):
        # TODO: handle ordering
        for idx, obj in enumerate(self.table(table_name)):
            for filter_name in filters:
                test_value = obj.get_for_db(filter_name)
                if filters[filter_name] is None and test_value is not None:
                    break
                elif isinstance(filters[filter_name], tuple):
                    if test_value is None:
                        if len(filters[filter_name]) < 3 or not filters[filter_name][2]:
                            break
                    else:
                        if filters[filter_name][1] == '<=' and not test_value <= filters[filter_name]:
                            break
                        elif filters[filter_name][1] == '>=' and not test_value >= filters[filter_name]:
                            break

                elif test_value != filters[filter_name]:
                    break
            else:
                yield idx

    def create_queue_item(self, **kwargs):
        kwargs['queue_uuid'] = str(uuid.uuid4())
        kwargs['created_date'] = datetime.datetime.now()
        kwargs['modified_date'] = datetime.datetime.now()
        kwargs['status'] = QueueStatus.UNLOCKED
        kwargs['locked_by'] = None
        kwargs['locked_since'] = None
        kwargs['escalation_level'] = 0
        if 'priority' not in kwargs:
            kwargs['priority'] = 0
        if 'unique_item_key' not in kwargs:
            kwargs['unique_item_key'] = None
        self.table(NODBQueueItem.TABLE_NAME).append(NODBQueueItem(**kwargs, is_new=False))

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str = 'tests',
                              subqueue_name: t.Optional[str] = None,
                              retries: int = 0):
        for idx, item in enumerate(self.table(NODBQueueItem.TABLE_NAME)):
            if item.queue_name == queue_name and (subqueue_name is None or item.subqueue_name == subqueue_name):
                return self.table(NODBQueueItem.TABLE_NAME).pop(idx)

    def commit(self):
        pass


class WorkerTestController:

    def __init__(self, db):
        self._db = db

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
        worker: QueueWorker = self._build_test_worker(worker_cls, worker_config)
        return self._test_harness(worker, self._test_queue_worker, queue_item)

    def _test_queue_worker(self, worker, queue_item):
        def retrieve_item():
            return queue_item
        worker._db = self._db
        worker._fetch_next_queue_item = retrieve_item
        worker._process_next_queue_item()

    def test_scheduled_task(self, worker_cls: type, worker_config: dict):
        worker: ScheduledTask = self._build_test_worker(worker_cls, worker_config)
        return self._test_harness(worker, self._test_scheduled_task)

    def _test_scheduled_task(self, worker):
        worker._run_scheduled_task(datetime.datetime.now(datetime.timezone.utc))

    def _test_harness(self, worker, action: callable, *args, **kwargs):
        exc = None
        try:
            worker.on_start()
            action(worker, *args, **kwargs)
        except Exception as ex:
            exc = ex
            raise ex
        finally:
            worker.on_exit(exc)
        return worker

    def _build_test_worker(self, worker_cls: type, worker_config: dict):
        return worker_cls(
            _halt_flag=ConstantHaltFlag(True),
            _end_flag=ConstantHaltFlag(True),
            _process_uuid=str(uuid.uuid4()),
            _config=worker_config,
        )



class BaseTestCase(ut.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseMock()
        cls.worker_controller = WorkerTestController(cls.db)

    def setUp(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp()).resolve().absolute()

    @injector.inject
    def tearDown(self, d: InjectableDict = None):
        shutil.rmtree(self.temp_dir)
        self.db.reset()
        d.data = {}

    @classmethod
    def tearDownClass(cls):
        del cls.db


class ConstantHaltFlag(HaltFlag):

    def __init__(self, sc: bool):
        self.should_continue = sc

    def _should_continue(self) -> bool:
        return self.should_continue
