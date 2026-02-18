import gzip
import pathlib
import shutil
import tempfile
import unittest as ut
import uuid
import typing as t

from cnodc.nodb import QueueStatus
from cnodc.workflow.workflow import FileInfo, WorkflowPayload, FilePayload, BatchPayload, SourceFilePayload, \
    ObservationPayload
import datetime
from cnodc.nodb.structures import NODBQueueItem, NODBSourceFile
from cnodc.util import CNODCError


class DatabaseQueueMock:

    def __init__(self):
        self.tables: dict[str, list] = {
            NODBQueueItem.TABLE_NAME: []
        }

    def stream_objects(self, cls, filters: t.Optional[dict] = None, order_by: t.Optional[t.Union[list[str], str]] = None, **kwargs):
        for idx in self._find_object_indexes(cls.TABLE_NAME, filters or {}, order_by):
            yield self.tables[cls.TABLE_NAME][idx]

    def count_objects(self, cls, filters: t.Optional[dict] = None, **kwargs):
        return len([x for x in self.stream_objects(cls, filters, **kwargs)])

    def bulk_update(self, cls, updates, key_field, key_values):
        pass

    def insert_object(self, obj):
        pass

    def update_object(self, obj):
        pass

    def upsert_object(self, obj):
        if obj.is_new:
            self.insert_object(obj)
        else:
            self.update_object(obj)

    def load_object(self, cls, filters: dict, **kwargs):
        obj_idx = self._find_object_index(cls.TABLE_NAME, filters)
        if obj_idx is None:
            return None
        return self.tables[cls.TABLE_NAME][obj_idx]

    def delete_object(self, obj):
        filters = {
            key: getattr(obj, obj.get_for_db(key))
            for key in obj.get_primary_keys()
        }
        index = self._find_object_index(obj.get_table_name(), filters)
        if index is not None:
            self.tables[obj.get_table_name()].pop(index)

    def _find_object_index(self, table_name, filters: dict, order_by=None):
        for idx in self._find_object_indexes(table_name, filters):
            return idx
        return None

    def _find_object_indexes(self, table_name, filters: dict, order_by=None):
        # TODO: handle ordering
        for idx, obj in enumerate(self.tables[table_name]):
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
        self.tables[NODBQueueItem.TABLE_NAME].append(NODBQueueItem(**kwargs, is_new=False))

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str = 'tests',
                              subqueue_name: t.Optional[str] = None,
                              retries: int = 0):
        for idx, item in enumerate(self.tables[NODBQueueItem.TABLE_NAME]):
            if item.queue_name == queue_name and (subqueue_name is None or item.subqueue_name == subqueue_name):
                return self.tables[NODBQueueItem.TABLE_NAME].pop(idx)


class TestFileInfo(ut.TestCase):

    def test_from_dict(self):
        info = FileInfo.from_map({
            'file_path': '/hello/world/123456.txt',
            'filename': '12345.txt',
            'is_gzipped': False,
            'mod_date': '2015-12-21T01:02:03',
        })
        self.assertEqual(info.file_path, '/hello/world/123456.txt')
        self.assertEqual(info.filename, '12345.txt')
        self.assertFalse(info.is_gzipped)
        self.assertEqual(info.last_modified_date, datetime.datetime(2015, 12, 21, 1, 2, 3))

    def test_from_partial_dict(self):
        info = FileInfo.from_map({
            'file_path': '/hello/world/123456.txt',
        })
        self.assertEqual(info.file_path, '/hello/world/123456.txt')
        self.assertEqual(info.filename, '123456.txt')
        self.assertFalse(info.is_gzipped)
        self.assertIsNone(info.last_modified_date)

    def test_to_map(self):
        info = FileInfo("/hello/world/1234.txt")
        map_ = info.to_map()
        self.assertIn('file_path', map_)
        self.assertIn('filename', map_)
        self.assertIn('is_gzipped', map_)
        self.assertNotIn('mod_date', map_)
        self.assertEqual(map_['file_path'], '/hello/world/1234.txt')
        self.assertEqual(map_['filename'], '1234.txt')
        self.assertFalse(map_['is_gzipped'])

    def test_to_map_with_date(self):
        info = FileInfo("/hello/world/1234.txt", last_modified_date=datetime.datetime(2016, 1, 2, 3, 4, 5))
        map_ = info.to_map()
        self.assertIn('file_path', map_)
        self.assertIn('filename', map_)
        self.assertIn('is_gzipped', map_)
        self.assertIn('mod_date', map_)
        self.assertEqual(map_['file_path'], '/hello/world/1234.txt')
        self.assertEqual(map_['filename'], '1234.txt')
        self.assertFalse(map_['is_gzipped'])
        self.assertEqual(map_['mod_date'], '2016-01-02T03:04:05')

    def test_from_gzip_path_with_lmt(self):
        info = FileInfo("/hello/world/12345.txt.gz", last_modified_date=datetime.datetime(2017, 1, 2, 3, 4, 5))
        self.assertEqual(info.file_path, '/hello/world/12345.txt.gz')
        self.assertEqual(info.filename, '12345.txt.gz')
        self.assertTrue(info.is_gzipped)
        self.assertEqual(info.last_modified_date, datetime.datetime(2017, 1, 2, 3, 4, 5))


class TestWorkflowPayload(ut.TestCase):

    def test_set_get_clear_metadata(self):
        wp = WorkflowPayload()
        wp.set_metadata('hello', 'world')
        self.assertEqual(wp.metadata['hello'], 'world')
        self.assertEqual(wp.get_metadata('hello'), 'world')
        wp.set_metadata('hello', None)
        self.assertNotIn('hello', wp.metadata)

    def test_default_metadata(self):
        wp = WorkflowPayload()
        self.assertEqual(wp.get_metadata('hello', 'world'), 'world')
        wp.set_metadata('hello', 'person')
        self.assertEqual(wp.get_metadata('hello', 'world'), 'person')

    def test_special_metadata(self):
        wp = WorkflowPayload()
        wp.increment_priority()
        self.assertEqual(wp.metadata['queue-priority'], 1)
        wp.increment_priority(5)
        self.assertEqual(wp.metadata['queue-priority'], 6)
        wp.decrement_priority(2)
        self.assertEqual(wp.metadata['queue-priority'], 4)
        wp.set_subqueue_name('test')
        self.assertEqual(wp.metadata['manual-subqueue'], 'test')
        wp.set_unique_key('12345')
        self.assertEqual(wp.metadata['unique-item-key'], '12345')
        wp.set_priority(5)
        self.assertEqual(wp.metadata['queue-priority'], 5)
        wp.set_followup_queue('test2')
        self.assertEqual(wp.metadata['followup-queue'], 'test2')

    def test_from_map(self):
        data = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['workflow'] = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['workflow']['name'] = 'hello'
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['workflow']['step'] = 'step1'
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['workflow']['step_done'] = False
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['batch_info'] = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['batch_info']['uuid'] = '12345'
        bp = WorkflowPayload.from_map(data)
        self.assertIsInstance(bp, BatchPayload)
        self.assertEqual(bp.workflow_name, 'hello')
        self.assertEqual(bp.current_step, 'step1')
        self.assertEqual(bp.current_step_done, False)
        self.assertEqual(bp.batch_uuid, '12345')
        del data['batch_info']
        data['source_info'] = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        source_uuid = str(uuid.uuid4())
        data['source_info']['source_uuid'] = source_uuid
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['source_info']['received'] = '2015-12-01'
        sp = WorkflowPayload.from_map(data)
        self.assertIsInstance(sp, SourceFilePayload)
        self.assertEqual(sp.workflow_name, 'hello')
        self.assertEqual(sp.current_step, 'step1')
        self.assertEqual(sp.current_step_done, False)
        self.assertEqual(sp.source_uuid, source_uuid)
        self.assertEqual(sp.received_date, datetime.date(2015, 12, 1))
        del data['source_info']
        data['item_info'] = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['item_info']['uuid'] = '123456'
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['item_info']['received'] = '2015-12-15'
        op = WorkflowPayload.from_map(data)
        self.assertIsInstance(op, ObservationPayload)
        self.assertEqual(op.workflow_name, 'hello')
        self.assertEqual(op.current_step, 'step1')
        self.assertEqual(op.current_step_done, False)
        self.assertEqual(op.uuid, '123456')
        self.assertEqual(op.received_date, datetime.date(2015, 12, 15))
        del data['item_info']
        data['file_info'] = {}
        self.assertRaises(CNODCError, WorkflowPayload.from_map, data)
        data['file_info']['file_path'] = '/srv/test/1234.txt.gz'
        obj = WorkflowPayload.from_map(data)
        self.assertIsInstance(obj, FilePayload)
        self.assertEqual(obj.workflow_name, 'hello')
        self.assertEqual(obj.current_step, 'step1')
        self.assertEqual(obj.current_step_done, False)
        self.assertEqual(obj.file_info.file_path, '/srv/test/1234.txt.gz')
        self.assertEqual(obj.file_info.filename, '1234.txt.gz')
        self.assertTrue(obj.file_info.is_gzipped)
        self.assertIsNone(obj.file_info.last_modified_date)


class TestFilePayload(ut.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_file_download(self):
        fp = FilePayload.from_path(str(pathlib.Path(__file__).absolute()))
        expected_dir = pathlib.Path(self.temp_dir)
        actual_file = fp.download(expected_dir)
        self.assertTrue(actual_file.exists())

    def test_gzipped_file_download(self):
        expected_dir = pathlib.Path(self.temp_dir)
        gzip_file = expected_dir / "hello.txt.gz"
        with gzip.open(gzip_file, "wb") as h:
            h.write(b"hello world")
        fp = FilePayload(file_info=FileInfo(str(gzip_file), filename="hello2.txt.gz", is_gzipped=True))
        actual_file = fp.download(expected_dir)
        self.assertTrue(actual_file.exists())
        self.assertTrue(actual_file.name, 'hello2.txt')
        with open(actual_file, "rb") as h:
            content = h.read()
            self.assertEqual(content, b"hello world")

    def test_file_no_exist(self):
        fp = FilePayload.from_path(str(pathlib.Path(__file__).parent.absolute() / "not_a_file.ext"))
        self.assertRaises(CNODCError, fp.download, self.temp_dir)

    def test_bad_file_protocol(self):
        fp = FilePayload.from_path("protocol://hello/world.txt")
        self.assertRaises(CNODCError, fp.download, self.temp_dir)

    def test_to_map(self):
        fp = FilePayload.from_path("/test/file.txt.gz")
        fp.workflow_name = 'test'
        map_ = fp.to_map()
        self.assertIn('file_info', map_)
        self.assertIn('file_path', map_['file_info'])
        self.assertIn('filename', map_['file_info'])
        self.assertIn('is_gzipped', map_['file_info'])
        self.assertEqual(map_['file_info']['file_path'], '/test/file.txt.gz')
        self.assertEqual(map_['file_info']['filename'], 'file.txt.gz')
        self.assertTrue(map_['file_info']['is_gzipped'])
        self.assertIn('workflow', map_)
        self.assertIn('name', map_['workflow'])
        self.assertEqual(map_['workflow']['name'], 'test')


class TestSourceFilePayload(ut.TestCase):

    def test_map(self):
        sp = SourceFilePayload(source_file_uuid="12345", received_date=datetime.date(2015, 1, 1), workflow_name='test')
        map_ = sp.to_map()
        self.assertIn('source_info', map_)
        self.assertIn('source_uuid', map_['source_info'])
        self.assertIn('received', map_['source_info'])
        self.assertEqual(map_['source_info']['source_uuid'], '12345')
        self.assertEqual(map_['source_info']['received'], '2015-01-01')
        self.assertIn('workflow', map_)
        self.assertIn('name', map_['workflow'])
        self.assertEqual(map_['workflow']['name'], 'test')

    def test_from_source_file(self):
        sf = NODBSourceFile(is_new=False, source_uuid='12345', received_date=datetime.date(2015, 1, 2))
        self.assertEqual(sf.source_uuid, '12345')
        self.assertEqual(sf.received_date, datetime.date(2015, 1, 2))
        sp = SourceFilePayload.from_source_file(sf)
        self.assertEqual(sp.source_uuid, '12345')
        self.assertTrue(sp.received_date, datetime.date(2015, 1, 2))


class TestBatchPayload(ut.TestCase):

    def test_independent_copy(self):
        wp = BatchPayload("12345")
        wp.set_metadata('hello', 'world')
        wp2 = wp.clone()
        self.assertEqual(wp2.get_metadata('hello'), 'world')
        self.assertFalse(wp.metadata is wp2.metadata)

    def test_enqueue_dequeue_batch(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock, 'hello')
        next_item = db_mock.fetch_next_queue_item('something_else')
        self.assertIsNone(next_item)
        next_item = db_mock.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        bp2 = WorkflowPayload.from_queue_item(next_item)
        self.assertIsInstance(bp2, BatchPayload)
        self.assertEqual(bp2.metadata, bp.metadata)
        self.assertEqual(bp2.workflow_name, bp.workflow_name)
        self.assertEqual(bp2.current_step, bp.current_step)
        self.assertEqual(bp2.current_step_done, bp.current_step_done)

    def test_enqueue_override_priority(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock, 'hello', 27)
        next_item = db_mock.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.priority, 27)

    def test_enqueue_manual_priority(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_priority(29)
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock, 'hello')
        next_item = db_mock.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.priority, 29)

    def test_enqueue_subqueue(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_subqueue_name('world')
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock, 'hello')
        next_item = db_mock.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.subqueue_name, 'world')

    def test_enqueue_unique_item_key(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_unique_key('my_luggage')
        self.assertEqual(bp.metadata['unique-item-key'], 'my_luggage')
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock, 'hello')
        next_item = db_mock.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.unique_item_name, 'my_luggage')

    def test_enqueue_followup(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_followup_queue('world')
        db_mock = DatabaseQueueMock()
        bp.enqueue(db_mock)
        next_item = db_mock.fetch_next_queue_item('world')
        self.assertIsNotNone(next_item)

    def test_copy_details_from(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        sp = SourceFilePayload("123456", datetime.date(2015, 1, 2))
        bp.metadata['test'] = 'case'
        self.assertIsNone(sp.workflow_name)
        self.assertIsNone(sp.current_step)
        sp.copy_details_from(bp)
        self.assertIn('test', sp.metadata)
        self.assertEqual(sp.metadata['test'], 'case')

    def test_copy_details_with_next_step(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        sp = SourceFilePayload("123456", datetime.date(2015, 1, 2))
        self.assertIsNone(sp.workflow_name)
        self.assertIsNone(sp.current_step)
        self.assertFalse(bp.current_step_done)
        self.assertFalse(sp.current_step_done)
        sp.copy_details_from(bp, True)
        self.assertTrue(sp.current_step_done)






