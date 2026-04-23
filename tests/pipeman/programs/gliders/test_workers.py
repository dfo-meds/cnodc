import datetime
import json
import uuid
import functools
import zirconium as zr
from autoinject import injector
from nodb import NODBSourceFile, SourceFileStatus, NODBQueueItem, NODBPlatform, NODBMission
from pipeman.processing.payloads import SourceFilePayload, WorkflowPayload, FilePayload
from pipeman.programs.glider.workers import GliderConversionWorker, GliderMetadataUploadWorker, \
    add_glider_mission_platform_info
from tests.helpers.base_test_case import BaseTestCase
from tests.helpers.mock_requests import MockResponse
import medsutil.ocproc2 as ocproc2


class TestGliderMissionPlatformInfo(BaseTestCase):

    class FakeWorker:
        def __init__(self, db):
            self.db = db
            self.memory = {}

    def test_add_mission_platform_info(self):
        record1 = ocproc2.ParentRecord()
        record1.metadata['WMOID'] = '12345'
        record1.metadata['CruiseID'] = '23456'
        record2 = ocproc2.ParentRecord()
        record2.metadata['WMOID'] = '12345'
        record2.metadata['CruiseID'] = '23456'
        w = TestGliderMissionPlatformInfo.FakeWorker(self.db)
        add_glider_mission_platform_info(w, record1)
        add_glider_mission_platform_info(w, record2)
        self.assertEqual(1, self.db.rows(NODBPlatform.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBMission.TABLE_NAME))
        mission = self.db.table(NODBMission.TABLE_NAME)[0]
        platform = self.db.table(NODBPlatform.TABLE_NAME)[0]
        self.assertEqual(record1.metadata['CNODCPlatform'].value, platform.platform_uuid)
        self.assertEqual(record1.metadata['CNODCMission'].value, mission.mission_uuid)
        self.assertEqual(record2.metadata['CNODCPlatform'].value, platform.platform_uuid)
        self.assertEqual(record2.metadata['CNODCMission'].value, mission.mission_uuid)

    def test_use_existing_mission_platform_info(self):
        mission = NODBMission()
        mission.mission_id = '23456'
        mission.mission_uuid = str(uuid.uuid4())
        self.db.insert_object(mission)
        platform = NODBPlatform()
        platform.wmo_id = '12345'
        platform.platform_uuid = str(uuid.uuid4())
        self.db.insert_object(platform)
        record1 = ocproc2.ParentRecord()
        record1.metadata['WMOID'] = '12345'
        record1.metadata['CruiseID'] = '23456'
        record2 = ocproc2.ParentRecord()
        record2.metadata['WMOID'] = '12345'
        record2.metadata['CruiseID'] = '23456'
        mem = {}
        w = TestGliderMissionPlatformInfo.FakeWorker(self.db)
        add_glider_mission_platform_info(w, record1)
        add_glider_mission_platform_info(w, record2)
        self.assertEqual(1, self.db.rows(NODBPlatform.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBMission.TABLE_NAME))
        self.assertEqual(record1.metadata['CNODCPlatform'].value, platform.platform_uuid)
        self.assertEqual(record1.metadata['CNODCMission'].value, mission.mission_uuid)
        self.assertEqual(record2.metadata['CNODCPlatform'].value, platform.platform_uuid)
        self.assertEqual(record2.metadata['CNODCMission'].value, mission.mission_uuid)



class GliderConversionWorkerTest(BaseTestCase):

    def test_missing_og_directory(self):
        worker: GliderConversionWorker = self.worker_controller.build_test_worker(
            GliderConversionWorker, {}
        )
        with self.assertRaisesCNODCError('STORAGE-9000'):
            worker.on_start()

    def test_bad_og_directory(self):
        worker: GliderConversionWorker = self.worker_controller.build_test_worker(
            GliderConversionWorker, {
                'openglider_directory': self.temp_dir / 'foobar'
            }
        )
        with self.assertRaisesCNODCError('GLIDER-CONVERT-1001'):
            worker.on_start()

    def test_missing_og_erddap_directory(self):
        worker: GliderConversionWorker = self.worker_controller.build_test_worker(
            GliderConversionWorker, {
                'openglider_directory': self.temp_dir,
            }
        )
        with self.assertRaisesCNODCError('STORAGE-9000'):
            worker.on_start()

    def test_bad_og_erddap_directory(self):
        worker: GliderConversionWorker = self.worker_controller.build_test_worker(
            GliderConversionWorker, {
                'openglider_directory': self.temp_dir,
                'openglider_erddap_directory': self.temp_dir / 'foobar'
            }
        )
        with self.assertRaisesCNODCError('GLIDER-CONVERT-1003'):
            worker.on_start()

    def test_new_file(self):
        input_file = self.data_file_path('glider_ego/SEA032_20250606_R.nc')
        sf = NODBSourceFile()
        sf.file_name = input_file.name
        sf.source_path = str(input_file)
        sf.received_date = datetime.date(2015, 1, 2)
        sf.source_uuid = str(uuid.uuid4())
        sf.status = SourceFileStatus.NEW
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        sp.workflow_name = 'test'
        sp.current_step = 'glider_ego_conversion'
        og_dir = self.temp_dir / 'og'
        og_dir.mkdir()
        og_file = og_dir / (input_file.name + '.gz')
        og_erddap_dir = self.temp_dir / 'erddap'
        og_erddap_dir.mkdir()
        og_erddap_file = og_erddap_dir / input_file.name.lower()[:-3] / (input_file.name + ".gz")
        self.assertFalse(og_file.exists())
        self.assertFalse(og_erddap_file.exists())
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.worker_controller.test_queue_worker(
                GliderConversionWorker,
                {
                    'openglider_directory' : str(og_dir),
                    'openglider_erddap_directory': str(og_erddap_dir),
                },
                self.worker_controller.payload_to_queue_item(sp, 'glider_ego_conversion')
            )
        self.assertTrue(og_file.exists())
        self.assertTrue(og_erddap_file.exists())
        self.assertEqual(2, self.db.rows(NODBQueueItem.TABLE_NAME))
        item = WorkflowPayload.from_queue_item(self.db.table(NODBQueueItem.TABLE_NAME)[0])
        self.assertIsInstance(item, FilePayload)
        self.assertEqual(item.file_path, str(og_file).replace("\\", "/"))
        item2 = WorkflowPayload.from_queue_item(self.db.table(NODBQueueItem.TABLE_NAME)[1])
        self.assertIsInstance(item2, SourceFilePayload)
        self.assertEqual(item2.load_source_file(self.db).source_uuid, sf.source_uuid)

    def test_existing_file(self):
        input_file = self.data_file_path('glider_ego/SEA032_20250606_R.nc')
        sf = NODBSourceFile()
        sf.file_name = input_file.name
        sf.source_path = str(input_file)
        sf.received_date = datetime.date(2015, 1, 2)
        sf.source_uuid = str(uuid.uuid4())
        sf.status = SourceFileStatus.NEW
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        sp.workflow_name = 'test'
        sp.current_step = 'glider_ego_conversion'
        og_dir = self.temp_dir / 'og'
        og_dir.mkdir()
        og_file = og_dir / (input_file.name + ".gz")
        og_file.touch()
        og_erddap_dir = self.temp_dir / 'erddap'
        og_erddap_dir.mkdir()
        og_erddap_file = og_erddap_dir / input_file.name.lower()[:-3] / input_file.name
        og_erddap_file.parent.mkdir()
        og_erddap_file.touch()
        self.assertTrue(og_file.exists())
        self.assertTrue(og_erddap_file.exists())
        self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.worker_controller.test_queue_worker(
                GliderConversionWorker,
                {
                    'openglider_directory' : str(og_dir),
                    'openglider_erddap_directory': str(og_erddap_dir),
                    'gzip_erddap': False,
                },
                self.worker_controller.payload_to_queue_item(sp, 'glider_ego_conversion')
            )
        self.assertTrue(og_file.exists())
        self.assertTrue(og_erddap_file.exists())
        self.assertEqual(3, self.db.rows(NODBQueueItem.TABLE_NAME))
        item0: NODBQueueItem = self.db.table(NODBQueueItem.TABLE_NAME)[0]
        self.assertDictSimilar(item0.data, {
            'dataset_id': input_file.name[:-3],
        })
        self.assertEqual(item0.unique_item_name, input_file.name[:-3])
        self.assertEqual(item0.queue_name, 'erddap_reload')
        item = WorkflowPayload.from_queue_item(self.db.table(NODBQueueItem.TABLE_NAME)[1])
        self.assertIsInstance(item, FilePayload)
        self.assertEqual(item.file_path, str(og_file).replace("\\", "/"))
        item2 = WorkflowPayload.from_queue_item(self.db.table(NODBQueueItem.TABLE_NAME)[2])
        self.assertIsInstance(item2, SourceFilePayload)
        self.assertEqual(item2.load_source_file(self.db).source_uuid, sf.source_uuid)



def with_security(cb):
    @functools.wraps(cb)
    def _inner(method, url, **kwargs):
        h = kwargs.pop('headers', {})
        if 'Authorization' not in h:
            return MockResponse(b"Forbidden", 403)
        if h['Authorization'] != '12345':
            return MockResponse(b"Forbidden", 403)
        return cb(method, url, **kwargs)
    return _inner

@with_security
def upsert_dataset(method, url, data, **kwargs):
    data.append(kwargs.pop('json'))
    return json.dumps({'guid': '23456'})



class TestGliderMetadataUploadWorker(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._web_test_data = []
        cls.web('http://test/api/upsert-dataset', 'POST')(
            functools.partial(upsert_dataset, data=cls._web_test_data)
        )

    def setUp(self):
        super().setUp()
        self._web_test_data.clear()

    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def test_upload_metadata(self):
        fp = FilePayload.from_path(
            str(self.data_file_path('glider_openglider/SEA032_20190116_R.nc'))
        )
        with self.mock_web_test():
            self.worker_controller.test_queue_worker(
                GliderMetadataUploadWorker,
                {},
                self.worker_controller.payload_to_queue_item(fp)
            )
            self.assertEqual(1, len(self._web_test_data))
            self.assertEqual(0, self.db.rows(NODBQueueItem.TABLE_NAME))






