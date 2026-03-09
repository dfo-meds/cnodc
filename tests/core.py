import logging
import pathlib
import shutil
import tempfile
import uuid
import unittest as ut
import typing as t

from prometheus_client.decorator import contextmanager

from cnodc.nodb import QueueStatus
import datetime
from cnodc.nodb.structures import NODBQueueItem
from autoinject import injector


@injector.injectable
class InjectableDict:

    def __init__(self):
        self.data = {}


class DatabaseMock:

    def __init__(self):
        self.tables: dict[str, list] = {}
        self._permissions: dict[str, set[str]] = {}
        self._rolled_back = False

    def grant_permission(self, role_name, perm_name):
        if role_name not in self._permissions:
            self._permissions[role_name] = set()
        self._permissions[role_name].add(perm_name)

    def remove_permission(self, role_name, perm_name):
        if role_name in self._permissions and perm_name in self._permissions[role_name]:
            self._permissions[role_name].remove(perm_name)

    def load_permissions(self, roles: list[str]):
        perms = set()
        for r in roles:
            if r in self._permissions:
                perms.update(self._permissions[r])
        return perms

    def reset(self):
        self.tables = {}
        self._rolled_back = False
        self._permissions = {}

    def table(self, table_name: str):
        if table_name not in self.tables:
            self.tables[table_name] = []
        return self.tables[table_name]

    def fast_renew_queue_item(self, queue_uuid):
        renew = datetime.datetime.now(datetime.timezone.utc)
        return renew

    def fast_update_queue_status(self, queue_uuid, new_status, release_at, reduce_priority, escalation_level):
        pass

    def stream_objects(self, obj_cls, **kwargs):
        for idx in self._find_object_indexes(obj_cls.TABLE_NAME, **kwargs):
            yield self.table(obj_cls.TABLE_NAME)[idx]

    def count_objects(self, obj_cls, filters: t.Optional[dict] = None, **kwargs):
        return sum(1 for _ in self._find_object_indexes(obj_cls.TABLE_NAME, filters))

    def bulk_update(self, cls, updates, key_field, key_values):
        for obj in self.stream_objects(cls, filters={key_field: (key_values, 'IN', False)}):
            for name in updates:
                setattr(obj, name, updates[name])

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

    def _find_object_indexes(self, table_name, filters: dict=None, filter_type=' AND ', **kwargs):
        filters = filters or {}
        # TODO: handle ordering
        for idx, obj in enumerate(self.table(table_name)):
            if filter_type == ' OR ':
                if any(self._check_filter(obj.get_for_db(filter_name), filters[filter_name]) for filter_name in filters):
                    yield idx
            else:
                if all(self._check_filter(obj.get_for_db(filter_name), filters[filter_name]) for filter_name in filters):
                    yield idx

    def _check_filter(self, test_value, filter_info):
        if filter_info is None:
            return test_value is None
        elif isinstance(filter_info, tuple):
            if test_value is None:
                return len(filter_info) > 2 and filter_info[2]
            else:
                if filter_info[1] == '<=':
                    return test_value <= filter_info[0]
                elif filter_info[1] == '>=':
                    return test_value >= filter_info[0]
                elif filter_info[1] == '>':
                    return test_value > filter_info[0]
                elif filter_info[1] == '<':
                    return test_value < filter_info[0]
                elif filter_info[1] == 'IN':
                    return test_value in filter_info[0]
                else:
                    raise ValueError(f'op [{filter_info[1]}] not recognized [mock DB]')
        else:
            return test_value == filter_info

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
                with item._readonly_access():
                    item.status = QueueStatus.LOCKED
                    item.locked_by = 'mock'
                    item.locked_since = datetime.datetime.now()
                return item

    def commit(self):
        pass

    def rollback(self):
        self._rolled_back = True

class DummyNODB:

    def __init__(self, db):
        self._db = db

    def __enter__(self):
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class BaseTestCase(ut.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseMock()
        cls.nodb = DummyNODB(cls.db)
        logging.disable(logging.WARNING)

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

    @contextmanager
    def assertLogs(self, logger=None, level=None):
        old_level = logging.root.disabled
        try:
            if level:
                if isinstance(level, str):
                    logging.disable(getattr(logging, level) - 1)
                else:
                    logging.disable(level - 1)
            else:
                logging.disable(logging.NOTSET)
            with super().assertLogs(logger, level) as h:
                yield h
        finally:
            logging.disable(old_level)

