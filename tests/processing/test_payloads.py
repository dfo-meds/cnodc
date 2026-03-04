import gzip
import pathlib
import uuid
import datetime

from cnodc.nodb import NODBBatch, NODBObservation, NODBObservationData, NODBUploadWorkflow
from cnodc.processing.workflow.payloads import FileInfo, WorkflowPayload, FilePayload, SourceFilePayload, BatchPayload, ObservationPayload
from cnodc.nodb.structures import NODBSourceFile
from cnodc.util import CNODCError

from core import BaseTestCase


class TestFileInfo(BaseTestCase):

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


class TestWorkflowPayload(BaseTestCase):

    def test_load_workflow(self):
        workflow = NODBUploadWorkflow(is_new=True)
        workflow.workflow_name = 'test'
        workflow.configuration = {}
        self.db.insert_object(workflow)
        wp = WorkflowPayload(workflow_name='test')
        workflow = wp.load_workflow(self.db)
        self.assertIsNotNone(workflow)
        self.assertEqual(workflow.name, 'test')

    def test_load_bad_workflow(self):
        wp = WorkflowPayload(workflow_name='test')
        self.assertRaises(CNODCError, wp.load_workflow, self.db)

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


class TestFilePayload(BaseTestCase):

    def test_file_download(self):
        fp = FilePayload.from_path(str(pathlib.Path(__file__).absolute()))
        actual_file = fp.download(self.temp_dir)
        self.assertTrue(actual_file.exists())

    def test_file_download_as_str(self):
        fp = FilePayload.from_path(str(pathlib.Path(__file__).absolute()))
        actual_file = fp.download(str(self.temp_dir))
        self.assertTrue(actual_file.exists())


    def test_gzipped_file_download(self):
        gzip_file = self.temp_dir / "hello.txt.gz"
        with gzip.open(gzip_file, "wb") as h:
            h.write(b"hello world")
        fp = FilePayload(file_info=FileInfo(str(gzip_file), filename="hello2.txt.gz", is_gzipped=True))
        actual_file = fp.download(self.temp_dir)
        self.assertTrue(actual_file.exists())
        self.assertTrue(actual_file.name, 'hello2.txt')
        with open(actual_file, "rb") as h:
            content = h.read()
            self.assertEqual(content, b"hello world")

    def test_gzipped_file_download_no_name(self):
        gzip_file = self.temp_dir / "hello.txt"
        with gzip.open(gzip_file, "wb") as h:
            h.write(b"hello world")
        fp = FilePayload(file_info=FileInfo(str(gzip_file), filename="hello2.txt", is_gzipped=True))
        actual_file = fp.download(self.temp_dir)
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


class TestSourceFilePayload(BaseTestCase):

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
        sf = NODBSourceFile(is_new=True, source_uuid='12345', received_date=datetime.date(2015, 1, 2))
        self.assertEqual(sf.source_uuid, '12345')
        self.assertEqual(sf.received_date, datetime.date(2015, 1, 2))
        sp = SourceFilePayload.from_source_file(sf)
        self.assertEqual(sp.source_uuid, '12345')
        self.assertTrue(sp.received_date, datetime.date(2015, 1, 2))
        self.db.insert_object(sf)
        retrieved_sf = sp.load_source_file(self.db)
        self.assertIsNotNone(retrieved_sf)
        self.assertEqual(retrieved_sf.source_uuid, '12345')
        self.assertEqual(retrieved_sf.received_date, datetime.date(2015, 1, 2))

    def test_download(self):
        file = pathlib.Path(self.temp_dir) / "test.txt"
        with open(file, "w") as h:
            h.write("hello world")
        sf = NODBSourceFile(is_new=True, source_uuid='12345', received_date=datetime.date(2015, 2, 3))
        sf.source_path = str(file)
        sf.file_name = 'test2.txt'
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        actual_file = sp.download(self.db, self.temp_dir)
        self.assertTrue(actual_file.exists())
        self.assertEqual(actual_file.name, 'test2.txt')
        with open(actual_file, 'r') as h:
            content = h.read()
            self.assertEqual(content, 'hello world')

    def test_no_source_file(self):
        sp = SourceFilePayload(source_file_uuid='123456', received_date=datetime.date(2015, 1, 2))
        self.assertRaises(CNODCError, sp.load_source_file, self.db)


class TestBatchPayload(BaseTestCase):

    def test_independent_copy(self):
        wp = BatchPayload("12345")
        wp.set_metadata('hello', 'world')
        wp2 = wp.clone()
        self.assertEqual(wp2.get_metadata('hello'), 'world')
        self.assertFalse(wp.metadata is wp2.metadata)

    def test_enqueue_dequeue_batch(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.enqueue(self.db, 'hello')
        next_item = self.db.fetch_next_queue_item('something_else')
        self.assertIsNone(next_item)
        next_item = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        bp2 = WorkflowPayload.from_queue_item(next_item)
        self.assertIsInstance(bp2, BatchPayload)
        self.assertEqual(bp2.metadata, bp.metadata)
        self.assertEqual(bp2.workflow_name, bp.workflow_name)
        self.assertEqual(bp2.current_step, bp.current_step)
        self.assertEqual(bp2.current_step_done, bp.current_step_done)

    def test_enqueue_override_priority(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.enqueue(self.db, 'hello', 27)
        next_item = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.priority, 27)

    def test_enqueue_manual_priority(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_priority(29)
        bp.enqueue(self.db, 'hello')
        next_item = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.priority, 29)

    def test_enqueue_subqueue(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_subqueue_name('world')
        bp.enqueue(self.db,'hello')
        next_item = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.subqueue_name, 'world')

    def test_enqueue_unique_item_key(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_unique_key('my_luggage')
        self.assertEqual(bp.metadata['unique-item-key'], 'my_luggage')
        bp.enqueue(self.db, 'hello')
        next_item = self.db.fetch_next_queue_item('hello')
        self.assertIsNotNone(next_item)
        self.assertEqual(next_item.unique_item_name, 'my_luggage')

    def test_enqueue_followup(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        bp.set_followup_queue('world')
        bp.enqueue(self.db)
        next_item = self.db.fetch_next_queue_item('world')
        self.assertIsNotNone(next_item)

    def test_enqueue_error(self):
        bp = BatchPayload("12345", workflow_name='hello', current_step='step1')
        self.assertRaises(CNODCError, bp.enqueue, self.db)

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

    def test_from_batch_object(self):
        batch = NODBBatch(is_new=True, batch_uuid='1234567')
        self.db.insert_object(batch)
        bp = BatchPayload.from_batch(batch, workflow_name='hello')
        self.assertEqual(bp.batch_uuid, '1234567')
        self.assertEqual(bp.workflow_name, 'hello')
        load_batch = bp.load_batch(self.db)
        self.assertIsNotNone(load_batch)
        self.assertEqual(load_batch.batch_uuid, '1234567')

    def test_bad_batch(self):
        bp = BatchPayload(batch_uuid='12345')
        self.assertRaises(CNODCError, bp.load_batch, self.db)


class TestObservationPayload(BaseTestCase):

    def test_to_map(self):
        op = ObservationPayload(item_uuid='12345', item_received=datetime.date(2015, 1, 2), workflow_name='test')
        self.assertEqual(op.workflow_name, 'test')
        self.assertEqual(op.uuid, '12345')
        self.assertEqual(op.received_date, datetime.date(2015, 1, 2))
        map_ = op.to_map()
        self.assertIn('item_info', map_)
        self.assertIn('uuid', map_['item_info'])
        self.assertIn('received', map_['item_info'])
        self.assertEqual(map_['item_info']['uuid'], '12345')
        self.assertEqual(map_['item_info']['received'], '2015-01-02')

    def test_load(self):
        obs = NODBObservation(is_new=True, obs_uuid='12345', received_date=datetime.date(2015, 1, 2), platform_uuid='test')
        self.db.insert_object(obs)
        obs_data = NODBObservationData(is_new=True, obs_uuid='12345', received_date=datetime.date(2015, 1, 2), message_idx=5)
        self.db.insert_object(obs_data)
        op = ObservationPayload.from_observation(obs, workflow_name='test')
        self.assertEqual(op.uuid, '12345')
        self.assertEqual(op.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(op.workflow_name, 'test')
        loaded_obs = op.load_observation(self.db)
        self.assertIsNotNone(loaded_obs)
        self.assertEqual(loaded_obs.platform_uuid, 'test')
        loaded_obs_data = op.load_observation_data(self.db)
        self.assertIsNotNone(loaded_obs_data)
        self.assertEqual(loaded_obs_data.message_idx, 5)

    def test_bad_load(self):
        op = ObservationPayload(item_uuid='12345', item_received=datetime.date(2015, 1, 2))
        self.assertEqual(op.uuid, '12345')
        self.assertEqual(op.received_date, datetime.date(2015, 1, 2))
        self.assertRaises(CNODCError, op.load_observation, self.db)
        self.assertRaises(CNODCError, op.load_observation_data, self.db)
