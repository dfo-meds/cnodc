import datetime

from cnodc.nodb import NODBObservationData, NODBObservation, NODBMission, NODBPlatform
from cnodc.nodb.observations import NODBWorkingRecord
from cnodc.programs.nodb.record_manager import NODBRecordManager
from core import BaseTestCase
import cnodc.ocproc2 as ocproc2


class TestRecordManager(BaseTestCase):

    def test_finalize_value(self):
        se = ocproc2.SingleElement(5)
        se.metadata['WorkingQuality'] = 5
        rm = NODBRecordManager()
        rm._finalize_value(se)
        self.assertNotIn('WorkingQuality', se)
        self.assertEqual(se.metadata['Quality'].value, 5)

    def test_finalize_multiple_value(self):
        se = ocproc2.SingleElement(5)
        se.metadata['WorkingQuality'] = 5
        se2 = ocproc2.SingleElement(5)
        se2.metadata['WorkingQuality'] = 4
        me = ocproc2.MultiElement([se, se2])
        rm = NODBRecordManager()
        rm._finalize_value(me)
        self.assertNotIn('WorkingQuality', se)
        self.assertEqual(se.metadata['Quality'].value, 5)
        self.assertNotIn('WorkingQuality', se2)
        self.assertEqual(se2.metadata['Quality'].value, 4)

    def test_finalize(self):
        record = ocproc2.ParentRecord()
        record.coordinates['Longitude'] = 54
        record.coordinates['Longitude'].metadata['WorkingQuality'] = 1
        record.metadata['Test'] = 12
        record.metadata['Test'].metadata['WorkingQuality'] = 3
        record.parameters['Temperature'] = 2.3
        record.parameters['Temperature'].metadata['WorkingQuality'] = 4
        sr = ocproc2.ChildRecord()
        sr.parameters['Temperature'] = 3.4
        sr.parameters['Temperature'].metadata['WorkingQuality'] = 2
        record.subrecords.append_to_record_set('PROFILE', 0, sr)
        rm = NODBRecordManager()
        rm.finalize(record, True)
        self.assertEqual(record.metadata['CNODCLevel'].value, 'UNKNOWN')
        self.assertEqual(record.metadata['Test'].metadata['Quality'].value, 3)
        self.assertEqual(record.coordinates['Longitude'].metadata['Quality'].value, 1)
        self.assertEqual(record.parameters['Temperature'].metadata['Quality'].value, 4)
        self.assertEqual(record.subrecords['PROFILE'][0].records[0].parameters['Temperature'].metadata['Quality'].value, 2)

    def test_build_nodb_entry(self):
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        obs, obs_data = rm.build_nodb_entry(record, '12345', datetime.date(2015, 1, 2), 0, 1)
        self.assertEqual(obs.obs_uuid, obs_data.obs_uuid)
        self.assertEqual(obs.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(obs.received_date, obs_data.received_date)
        self.assertIsNotNone(record.metadata.best('CNODCID'))
        self.assertEqual(obs_data.message_idx, 0)
        self.assertEqual(obs_data.record_idx, 1)
        self.assertEqual(obs_data.source_file_uuid, '12345')
        self.assertIsNotNone(obs_data.data_record)

    def test_build_nodb_working_entry(self):
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        we = rm.build_nodb_working_entry(record, '12345', datetime.date(2015, 1, 2), 0, 1)
        self.assertEqual(we.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(we.message_idx, 0)
        self.assertEqual(we.record_idx, 1)
        self.assertEqual(we.source_file_uuid, '12345')
        self.assertIsNotNone(we.data_record)

    def test_create_working_entry(self):
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        self.assertTrue(rm.create_working_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1))
        we = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        self.assertEqual(we.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(we.message_idx, 0)
        self.assertEqual(we.record_idx, 1)
        self.assertEqual(we.source_file_uuid, '12345')
        self.assertIsNotNone(we.data_record)

    def test_create_existing_working_entry(self):
        og = NODBWorkingRecord()
        og.received_date = datetime.date(2015, 1, 2)
        og.message_idx = 0
        og.record_idx = 1
        og.source_file_uuid = '12345'
        og.working_uuid = '123456'
        self.db.insert_object(og)
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        self.assertFalse(rm.create_working_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1))

    def test_create_completed_entry(self):
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        self.assertEqual(we.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(we.message_idx, 0)
        self.assertEqual(we.record_idx, 1)
        self.assertEqual(we.source_file_uuid, '12345')
        self.assertIsNotNone(we.data_record)
        obs = we.find_observation(self.db)
        self.assertIsNotNone(obs)

    def test_create_completed_entry_no_duplicate(self):
        record = ocproc2.ParentRecord()
        rm = NODBRecordManager()
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        self.assertEqual(1, len(self.db.table(NODBObservationData.TABLE_NAME)))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        self.assertEqual(we.received_date, datetime.date(2015, 1, 2))
        self.assertEqual(we.message_idx, 0)
        self.assertEqual(we.record_idx, 1)
        self.assertEqual(we.source_file_uuid, '12345')
        self.assertIsNotNone(we.data_record)
        obs = we.find_observation(self.db)
        self.assertIsNotNone(obs)
        self.assertFalse(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        self.assertEqual(1, len(self.db.table(NODBObservationData.TABLE_NAME)))

    def test_prune_mission_data(self):
        mission = NODBMission()
        mission.metadata = {
            'PlatformFinalStatus': 'lost',
            'OperatingInstitution': 'dfo',
        }
        mission.mission_uuid = '123'
        self.db.insert_object(mission)
        record = ocproc2.ParentRecord()
        record.metadata['CNODCMission'] = '123'
        record.metadata['PlatformFinalStatus'] = 'lost'
        record.metadata['OperatingInstitution'] = 'cproof'
        record.metadata['PlatformMissionNumber'] = 97
        rm = NODBRecordManager()
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        record2 = we.record
        self.assertNotIn('PlatformFinalStatus', record2.metadata)
        self.assertNotIn('PlatformMissionNumber', record2.metadata)
        self.assertIn('OperatingInstitution', record2.metadata)
        miss2 = NODBMission.find_by_uuid(self.db, '123')
        self.assertEqual(miss2.get_metadata('PlatformMissionNumber'), 97)

    def test_prune_platform_data(self):
        platform = NODBPlatform()
        platform.platform_uuid = '1234'
        platform.metadata = {
            'BatteryDescription': 'foo',
            'BatteryType': 'lithium_ion',
        }
        self.db.insert_object(platform)
        record = ocproc2.ParentRecord()
        record.metadata['CNODCPlatform'] = '1234'
        record.metadata['BatteryDescription'] = 'foobar'
        record.metadata['BatteryType'] = 'lithium_ion'
        record.metadata['IMONumber'] = 99
        rm = NODBRecordManager()
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        record2 = we.record
        self.assertNotIn('BatteryType', record2.metadata)
        self.assertNotIn('IMONumber', record2.metadata)
        self.assertIn('BatteryDescription', record2.metadata)
        plat2 = NODBPlatform.find_by_uuid(self.db, '1234')
        self.assertEqual(plat2.get_metadata('IMONumber'), 99)

    def test_prune_mission_data_no_mission(self):
        record = ocproc2.ParentRecord()
        record.metadata['CNODCMission'] = '123'
        record.metadata['PlatformFinalStatus'] = 'lost'
        record.metadata['OperatingInstitution'] = 'cproof'
        record.metadata['PlatformMissionNumber'] = 97
        rm = NODBRecordManager()
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, {}))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        record2 = we.record
        self.assertIn('PlatformFinalStatus', record2.metadata)
        self.assertIn('PlatformMissionNumber', record2.metadata)
        self.assertIn('OperatingInstitution', record2.metadata)

    def test_prune_multiple_mission_data(self):
        mission = NODBMission()
        mission.metadata = {
            'PlatformFinalStatus': 'lost',
            'OperatingInstitution': 'dfo',
        }
        mission.mission_uuid = '123'
        self.db.insert_object(mission)
        record = ocproc2.ParentRecord()
        record.metadata['CNODCMission'] = '123'
        record.metadata['PlatformFinalStatus'] = 'lost'
        record.metadata['OperatingInstitution'] = 'cproof'
        record.metadata['PlatformMissionNumber'] = 97
        record2 = ocproc2.ParentRecord()
        record2.metadata['CNODCMission'] = '123'
        record2.metadata['PlatformFinalStatus'] = 'lost'
        rm = NODBRecordManager()
        memory = {}
        self.assertTrue(rm.create_completed_entry(self.db, record2, '12345', datetime.date(2015, 1, 2), 0, 2, memory))
        self.assertTrue(rm.create_completed_entry(self.db, record, '12345', datetime.date(2015, 1, 2), 0, 1, memory))
        we: NODBObservationData = NODBObservationData.find_by_source_info(self.db, '12345', '2015-01-02', 0, 1)
        self.assertIsNotNone(we)
        record2 = we.record
        self.assertNotIn('PlatformFinalStatus', record2.metadata)
        self.assertNotIn('PlatformMissionNumber', record2.metadata)
        self.assertIn('OperatingInstitution', record2.metadata)
        miss2 = NODBMission.find_by_uuid(self.db, '123')
        self.assertEqual(miss2.get_metadata('PlatformMissionNumber'), 97)