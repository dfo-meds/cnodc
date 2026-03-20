import datetime
import typing as t
import uuid

from cnodc.nodb import QueueStatus, NODBQueueItem, ScannedFileStatus
from cnodc.nodb.base import NODBBaseObject
from cnodc.util import clean_for_json


class DatabaseMock:

    def __init__(self):
        self.tables: dict[str, list[NODBBaseObject]] = {}
        self._permissions: dict[str, set[str]] = {}
        self._scanned_files: list[dict[str, t.Any]] = []
        self._lookups: dict[str, dict[str, dict[str, list[int]]]] = {}
        self._rolled_back = False

    def _clean_indices(self, table_name, idx):
        if table_name in self._lookups:
            for index_name in self._lookups[table_name]:
                for index_key in self._lookups[table_name][index_name]:
                    index_list = self._lookups[table_name][index_name][index_key]
                    index_list.remove(idx)

    def _update_indices(self, obj: NODBBaseObject, index: int):
        tbl_name = obj.get_table_name()
        if tbl_name not in self._lookups:
            self._lookups[tbl_name] = {}
        for index_keys in obj.get_mock_index_keys():
            index_name = '__'.join(index_keys)
            if index_name not in self._lookups[tbl_name]:
                self._lookups[tbl_name][index_name] = {}
            value = '__'.join(str(clean_for_json(obj.get_for_db(x))) for x in index_keys)
            if value not in self._lookups[tbl_name][index_name]:
                self._lookups[tbl_name][index_name][value] = []
            self._lookups[tbl_name][index_name][value].append(index)

    def scanned_file_status(self, file_path: str, mod_time: t.Optional[datetime.datetime] = None):
        for x in self._scanned_files:
            if x['file_path'] == file_path and x['modified_date'] == mod_time:
                if x['was_processed']:
                    return ScannedFileStatus.PROCESSED
                else:
                    return ScannedFileStatus.UNPROCESSED
        return ScannedFileStatus.NOT_PRESENT

    def note_scanned_file(self, file_path: str, mod_time: t.Optional[datetime.datetime] = None):
        self._scanned_files.append({
            'file_path': str(file_path),
            'was_processed': False,
            'modified_date': mod_time
        })

    def mark_scanned_item_success(self, file_path: str, mod_date: t.Optional[datetime.date] = None):
        file_path = str(file_path)
        if mod_date is None:
            for x in self._scanned_files:
                if x['file_path'] == file_path and x['modified_date'] is None:
                    x['was_processed'] = True
        else:
            found_exact = False
            for x in self._scanned_files:
                if x['file_path'] == file_path:
                    if x['modified_date'] is None or x['modified_date'] <= mod_date:
                        x['was_processed'] = True
                    if x['modified_date'] == mod_date:
                        found_exact = True
            if not found_exact:
                self._scanned_files.append({
                    'file_path': str(file_path),
                    'was_processed': True,
                    'modified_date': mod_date
                })

    def mark_scanned_item_failed(self, file_path: str, mod_date: t.Optional[datetime.date] = None):
        file_path = str(file_path)
        remove_indices = set()
        for idx, x in enumerate(self._scanned_files):
            if x['file_path'] == file_path:
                if x['was_processed']:
                    continue
                if x['modified_date'] is None and mod_date is None:
                    remove_indices.add(idx)
                elif x['modified_date'] is not None and mod_date is not None and x['modified_date'] <= mod_date:
                    remove_indices.add(idx)
        for idx in sorted(remove_indices, reverse=True):
            self._scanned_files.pop(idx)

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
        self._rolled_back = False
        self.tables.clear()
        self._permissions.clear()
        self._scanned_files.clear()
        self._lookups.clear()

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
        index = len(self.table(obj.get_table_name()))
        self.table(obj.get_table_name()).append(obj)
        self._update_indices(obj ,index)

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
            key: obj.get_for_db(key)
            for key in obj.get_primary_keys()
        }
        tbl_name = obj.get_table_name()
        index = self._find_object_index(tbl_name, filters)
        if index is not None:
            self.tables[tbl_name][index] = None
            self._clean_indices(tbl_name, index)

    def _find_object_index(self, table_name, filters: dict, order_by=None) -> t.Optional[int]:
        for idx in self._find_object_indexes(table_name, filters):
            return idx
        return None

    def _find_object_indexes(self, table_name, filters: dict=None, filter_type=' AND ', **kwargs):
        # TOOD: handle ordering
        filters = filters or {}
        limit_set = None
        if filter_type == ' AND ':
            if table_name in self._lookups:
                for index_key in self._lookups[table_name]:
                    keys = index_key.split('__')
                    if any(k not in filters or isinstance(filters[k], tuple) for k in keys):
                        continue
                    lookup_key = '__'.join(str(clean_for_json(filters[k])) for k in keys)
                    if lookup_key in self._lookups[table_name][index_key]:
                        limit_set = self._lookups[table_name][index_key][lookup_key]
                    else:
                        limit_set = []
                        break
        if limit_set is not None:
            for idx in limit_set:
                obj = self.table(table_name)[idx]
                if obj is None:
                    continue
                if all(self._check_filter(obj.get_for_db(filter_name), filters[filter_name]) for filter_name in filters):
                    yield idx
        else:
            for idx, obj in enumerate(self.table(table_name)):
                if obj is None:
                    continue
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
            if item.queue_name == queue_name and (subqueue_name is None or item.subqueue_name == subqueue_name) and item.status == QueueStatus.UNLOCKED:
                with item._readonly_access():
                    item.status = QueueStatus.LOCKED
                    item.locked_by = 'mock'
                    item.locked_since = datetime.datetime.now()
                return item

    def commit(self):
        pass

    def rollback(self):
        self._rolled_back = True

    def rows(self, tbl_name: str):
        if tbl_name in self.tables:
            return len(self.tables[tbl_name])
        return 0


class DummyNODB:

    def __init__(self, db: DatabaseMock):
        self._db = db

    def __enter__(self) -> DatabaseMock:
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
