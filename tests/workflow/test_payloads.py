import unittest as ut
import uuid

from cnodc.nodb import QueueStatus
from cnodc.workflow.workflow import FileInfo, WorkflowPayload, FilePayload, BatchPayload, SourceFilePayload, \
    ObservationPayload
import datetime
from cnodc.nodb.structures import NODBQueueItem
from cnodc.util import CNODCError


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


class DatabaseQueueMock:

    def __init__(self):
        self.queue: list[NODBQueueItem] = []

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
        self.queue.append(NODBQueueItem(**kwargs, is_new=False))

    def pop_queue_item(self, queue_name):
        for idx, item in enumerate(self.queue):
            if item.queue_name == queue_name:
                return self.queue.pop(idx)


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
        self.assertEqual(wp.metadata['post-review-queue'], 'test2')

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
        next_item = db_mock.pop_queue_item('something_else')
        self.assertIsNone(next_item)
        next_item = db_mock.pop_queue_item('hello')
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
        next_item = db_mock.pop_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.priority, 27)

    def test_copy_details_from(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        sp = SourceFilePayload("123456", datetime.date(2015, 1, 2))
        self.assertIsNone(sp.workflow_name)
        self.assertIsNone(sp.current_step)
        sp.copy_details_from(bp)
        self.assertEqual(sp.workflow_name, 'hello')
        self.assertEqual(sp.current_step, 'step1')








