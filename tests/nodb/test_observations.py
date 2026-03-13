import datetime
import enum

from cnodc.nodb import NODBSourceFile, NODBObservation, NODBObservationData, NODBMission, NODBPlatform, PlatformStatus, \
    NODBBatch, ProcessingLevel, ObservationType, ObservationStatus
from cnodc.nodb.observations import NODBWorkingRecord, BatchStatus
from cnodc.ocproc2 import ParentRecord, ChildRecord, QCResult
from cnodc.ocproc2.codecs import OCProc2BinCodec
from helpers.base_test_case import BaseTestCase


class TestSourceFile(BaseTestCase):

    def test_report_error(self):
        sf = NODBSourceFile()
        self.assertIsNone(sf.history)
        sf.report_error('ohno', 'one', 'two', 'three')
        self.assertEqual(1, len(sf.history))
        h = sf.history[0]
        self.assertEqual('ohno', h['msg'])
        self.assertEqual('one', h['src'])
        self.assertEqual('two', h['ver'])
        self.assertEqual('three', h['ins'])
        self.assertEqual('ERROR', h['lvl'])
        self.assertIsNotNone(h['rpt'])
        self.assertIn('history', sf.modified_values)

    def test_report_warning(self):
        sf = NODBSourceFile()
        self.assertIsNone(sf.history)
        sf.report_warning('ohno', 'one', 'two', 'three')
        self.assertEqual(1, len(sf.history))
        h = sf.history[0]
        self.assertEqual('ohno', h['msg'])
        self.assertEqual('one', h['src'])
        self.assertEqual('two', h['ver'])
        self.assertEqual('three', h['ins'])
        self.assertEqual('WARNING', h['lvl'])
        self.assertIsNotNone(h['rpt'])
        self.assertIn('history', sf.modified_values)

    def test_add_history(self):
        sf = NODBSourceFile()
        self.assertIsNone(sf.history)
        sf.add_history('ohno', 'one', 'two', 'three')
        self.assertEqual(1, len(sf.history))
        h = sf.history[0]
        self.assertEqual('ohno', h['msg'])
        self.assertEqual('one', h['src'])
        self.assertEqual('two', h['ver'])
        self.assertEqual('three', h['ins'])
        self.assertEqual('INFO', h['lvl'])
        self.assertIsNotNone(h['rpt'])
        self.assertIn('history', sf.modified_values)

    def test_search(self):
        sf = NODBSourceFile(
            source_uuid='12345',
            received_date=datetime.date(2015, 1, 2),
            source_path='/test/path',
            original_uuid='23456',
            original_idx=5
        )
        self.db.insert_object(sf)
        with self.subTest(msg="by uuid"):
            sf2 = NODBSourceFile.find_by_uuid(self.db, '12345', '2015-01-02')
            self.assertIs(sf2, sf)
        with self.subTest(msg="by path"):
            sf3 = NODBSourceFile.find_by_source_path(self.db, '/test/path')
            self.assertIs(sf3, sf)
        with self.subTest(msg="by original info"):
            sf4 = NODBSourceFile.find_by_original_info(self.db, '23456', '2015-01-02', 5)
            self.assertIs(sf4, sf)

    def test_stream(self):
        sf = NODBSourceFile(
            source_uuid='12345',
            received_date=datetime.date(2015, 1, 2),
            source_path='/test/path',
            original_uuid='23456',
            original_idx=5
        )
        self.db.insert_object(sf)
        self.db.insert_object(NODBObservationData(
            source_file_uuid='123465',
            received_date=datetime.date(2015, 1, 2)
        ))
        self.db.insert_object(NODBWorkingRecord(
            source_file_uuid='123456',
            received_date=datetime.date(2015, 1, 2)
        ))
        obs_data = NODBObservationData(
            source_file_uuid='12345',
            received_date=datetime.date(2015, 1, 2)
        )
        self.db.insert_object(obs_data)
        working = NODBWorkingRecord(
            source_file_uuid='12345',
            received_date=datetime.date(2015, 1, 2)
        )
        self.db.insert_object(working)
        with self.subTest(msg="stream_obs_data"):
            x = [x for x in sf.stream_observation_data(self.db)]
            self.assertEqual(1, len(x))
            self.assertIs(x[0], obs_data)
        with self.subTest(msg="stream_working"):
            x = [x for x in sf.stream_working_records(self.db)]
            self.assertEqual(1, len(x))
            self.assertIs(x[0], working)

    def test_metadata(self):
        sf = NODBSourceFile()
        sf.delete_metadata("foo")
        sf.set_metadata('foo', 'bar')
        sf.set_metadata('fizz', 'buzz')
        self.assertEqual(sf.get_metadata('foo'), 'bar')
        self.assertIsNone(sf.get_metadata('bar'))
        sf.delete_metadata('foo')
        sf.delete_metadata("bar")
        self.assertIsNone(sf.get_metadata('foo'))
        self.assertEqual(sf.get_metadata('fizz'), 'buzz')


class TestMission(BaseTestCase):

    def test_by_kwargs(self):
        m = NODBMission(
            mission_uuid='12345',
            mission_id='CRUISE NO 2',
            start_date=datetime.datetime.fromisoformat("2015-01-02T00:00:00+00:00"),
            end_date=datetime.datetime.fromisoformat("2015-01-04T00:00:00+00:00"),
        )
        self.assertEqual(m.mission_uuid, '12345')
        self.assertEqual(m.mission_id, 'CRUISE NO 2')
        self.assertEqual(m.start_date, datetime.datetime(2015, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc))
        self.assertEqual(m.end_date, datetime.datetime(2015, 1, 4, 0, 0, 0, tzinfo=datetime.timezone.utc))

    def test_find_by_uuid(self):
        m = NODBMission(mission_uuid='12345')
        self.db.insert_object(m)
        x = NODBMission.find_by_uuid(self.db, '12345')
        self.assertIs(x, m)

    def test_search(self):
        m = NODBMission(mission_uuid='12345', mission_id='CRUISE NO 2')
        self.db.insert_object(m)
        x = [x for x in NODBMission.search(self.db, mission_id='CRUISE NO 2')]
        self.assertEqual(1, len(x))
        self.assertIs(x[0], m)

    def test_search_nothing(self):
        m = NODBMission(mission_uuid='12345', mission_id='CRUISE NO 2')
        self.db.insert_object(m)
        x = [x for x in NODBMission.search(self.db)]
        self.assertEqual(0, len(x))


class TestPlatform(BaseTestCase):

    def test_kwargs(self):
        p = NODBPlatform(
            platform_uuid='12345',
            wmo_id='1',
            wigos_id='2',
            platform_name='SHARK',
            platform_id='SHRK',
            platform_type='boat',
            service_start_date='2015-01-02T00:00:00+00:00',
            service_end_date='2015-02-02T00:00:00+00:00',
            instrumentation=["one", "two", "three"],
            status=PlatformStatus.ACTIVE.value,
            embargo_data_days=2
        )
        self.assertEqual(p.platform_uuid, '12345')
        self.assertEqual(p.wmo_id, '1')
        self.assertEqual(p.wigos_id, '2')
        self.assertEqual(p.platform_name, 'SHARK')
        self.assertEqual(p.platform_type, 'boat')
        self.assertEqual(p.service_start_date, datetime.datetime(2015, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc))
        self.assertEqual(p.service_end_date, datetime.datetime(2015, 2, 2, 0, 0, 0, tzinfo=datetime.timezone.utc))
        self.assertEqual(p.instrumentation, ["one", "two", "three"])
        self.assertEqual(p.status, PlatformStatus.ACTIVE)
        self.assertEqual(p.embargo_data_days, 2)

    def test_find_by_uuid(self):
        p = NODBPlatform(platform_uuid='1')
        self.db.insert_object(p)
        p2 = NODBPlatform.find_by_uuid(self.db, '1')
        self.assertIs(p2, p)
        x = [x for x in NODBPlatform.find_all_raw(self.db)]
        self.assertEqual(1, len(x))

    def _build_search_data(self):
        self.db.insert_object(NODBPlatform(platform_uuid='1', wmo_id='5'))
        self.db.insert_object(NODBPlatform(platform_uuid='2', wmo_id='6'))
        self.db.insert_object(NODBPlatform(platform_uuid='3', wigos_id='7'))
        self.db.insert_object(NODBPlatform(platform_uuid='4', platform_name='BOATY'))
        self.db.insert_object(NODBPlatform(platform_uuid='5', platform_id='MCBT'))
        self.db.insert_object(NODBPlatform(platform_uuid='6', wmo_id='8', service_start_date='2015-01-01T00:00:00+00:00', service_end_date='2015-12-31T23:59:59+00:00'))
        self.db.insert_object(NODBPlatform(platform_uuid='7', wmo_id='8', service_start_date='2016-01-01T00:00:00+00:00', service_end_date='2016-12-31T23:59:59+00:00'))

    def test_search_many_results(self):
        self._build_search_data()
        x = [x.platform_uuid for x in NODBPlatform.search(self.db, wmo_id='5', wigos_id='7')]
        self.assertEqual(2, len(x))
        self.assertIn('1', x)
        self.assertIn('3', x)

    def test_search_by_wmo_id(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wmo_id='5')]
        self.assertEqual(1, len(x))
        self.assertEqual('1', x[0].platform_uuid)

    def test_search_by_wigos_id(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wigos_id='7')]
        self.assertEqual(1, len(x))
        self.assertEqual('3', x[0].platform_uuid)

    def test_search_by_platform_name(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, platform_name='BOATY')]
        self.assertEqual(1, len(x))
        self.assertEqual('4', x[0].platform_uuid)

    def test_search_by_platform_id(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, platform_id='MCBT')]
        self.assertEqual(1, len(x))
        self.assertEqual('5', x[0].platform_uuid)

    def test_search_by_service_date(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wmo_id='8', in_service_time=datetime.datetime(2015, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc))]
        self.assertEqual(1, len(x))
        self.assertEqual('6', x[0].platform_uuid)

    def test_search_by_service_date2(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wmo_id='8', in_service_time=datetime.datetime(2016, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc))]
        self.assertEqual(1, len(x))
        self.assertEqual('7', x[0].platform_uuid)

    def test_search_by_service_early_edge(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wmo_id='8', in_service_time=datetime.datetime(2016, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc))]
        self.assertEqual(1, len(x))
        self.assertEqual('7', x[0].platform_uuid)

    def test_search_by_service_late_edge(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, wmo_id='8', in_service_time=datetime.datetime(2016, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc))]
        self.assertEqual(1, len(x))
        self.assertEqual('7', x[0].platform_uuid)

    def test_search_by_nothing(self):
        self._build_search_data()
        x = [x for x in NODBPlatform.search(self.db, in_service_time=datetime.datetime(2016, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc))]
        self.assertEqual(0, len(x))


class TestBatch(BaseTestCase):

    def test_from_kwargs(self):
        b = NODBBatch(batch_uuid='12345', status=BatchStatus.NEW.value)
        self.assertEqual(b.batch_uuid, '12345')
        self.assertEqual(b.status, BatchStatus.NEW)

    def test_find_by_uuid(self):
        b = NODBBatch(batch_uuid='12345')
        self.db.insert_object(b)
        b2 = NODBBatch.find_by_uuid(self.db, '12345')
        self.assertIs(b, b2)

    def test_count_working_by_uuid(self):
        self.db.insert_object(NODBWorkingRecord(working_uuid='1', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='2', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='3', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='4', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='5', qc_batch_id='123457'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='6', qc_batch_id='123456'))
        self.assertEqual(4, NODBBatch.count_working_by_uuid(self.db, '12345'))
        self.assertEqual(1, NODBBatch.count_working_by_uuid(self.db, '123457'))
        self.assertEqual(1, NODBBatch.count_working_by_uuid(self.db, '123456'))
        self.assertEqual(0, NODBBatch.count_working_by_uuid(self.db, '1234562'))
        b = NODBBatch(batch_uuid='12345')
        self.assertEqual(4, b.count_working_records(self.db))

    def test_stream_working(self):
        self.db.insert_object(NODBWorkingRecord(working_uuid='1', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='2', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='3', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='4', qc_batch_id='12345'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='5', qc_batch_id='123457'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='6', qc_batch_id='123456'))
        b = NODBBatch(batch_uuid='12345')
        wr_uuids = [x.working_uuid for x in b.stream_working_records(self.db)]
        self.assertEqual(4, len(wr_uuids))
        self.assertIn('1', wr_uuids)
        self.assertIn('2', wr_uuids)
        self.assertIn('3', wr_uuids)
        self.assertIn('4', wr_uuids)


class TestObservation(BaseTestCase):

    def test_find_by_uuid(self):
        obs = NODBObservation(obs_uuid='12345', received_date=datetime.date(2015, 1, 2))
        self.db.insert_object(obs)
        obs2 = NODBObservation.find_by_uuid(self.db, '12345', datetime.date(2015, 1, 2))
        self.assertIs(obs, obs2)
        self.assertIsNone(NODBObservation.find_by_uuid(self.db, '123456', datetime.date(2015, 1, 2)))
        self.assertIsNone(NODBObservation.find_by_uuid(self.db, '12345', datetime.date(2015, 1, 3)))

    def test_update_simple_attributes_from_record(self):
        tests = [
            ('metadata/CNODCProgram', 'program_name', 'HelloWorld'),
            ('metadata/CNODCSource', 'source_name', 'HelloWorld2'),
            ('metadata/CNODCMission', 'mission_uuid', '12345'),
            ('metadata/CNODCPlatform', 'platform_uuid', '123456'),
            ('metadata/CNODCEmbargoUntil', 'embargo_date', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
            ('coordinates/Time', 'obs_time', datetime.datetime(2014, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
            ('metadata/CNODCLevel', 'processing_level', ProcessingLevel.REAL_TIME),
        ]
        for test_property, test_name, test_val in tests:
            with self.subTest(test_name=test_name):
                obs = NODBObservation()
                self.assertIsNone(getattr(obs, test_name))
                record = ParentRecord()
                if isinstance(test_val, enum.Enum):
                    record.set(test_property, test_val.value)
                else:
                    record.set(test_property, test_val)
                obs.update_from_record(record)
                self.assertEqual(getattr(obs, test_name), test_val)

    def test_update_location_from_record(self):
        obs = NODBObservation()
        self.assertIsNone(obs.location)
        self.assertIsNone(obs.observation_type)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        obs.update_from_record(record)
        self.assertEqual(obs.location, 'POINT (-123.12345 45.12346)')
        self.assertIs(obs.observation_type, ObservationType.SURFACE)

    def test_update_depth_from_top(self):
        obs = NODBObservation()
        self.assertIsNone(obs.max_depth)
        self.assertIsNone(obs.min_depth)
        self.assertIsNone(obs.observation_type)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.coordinates['Depth'] = 45.1
        obs.update_from_record(record)
        self.assertEqual(obs.min_depth, 45.1)
        self.assertEqual(obs.max_depth, 45.1)
        self.assertIs(obs.observation_type, ObservationType.AT_DEPTH)

    def test_update_depth_from_top_in_km(self):
        obs = NODBObservation()
        self.assertIsNone(obs.max_depth)
        self.assertIsNone(obs.min_depth)
        self.assertIsNone(obs.observation_type)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.coordinates['Depth'] = 0.0451
        record.coordinates['Depth'].metadata['Units'] = 'km'
        obs.update_from_record(record)
        self.assertEqual(obs.min_depth, 45.1)
        self.assertEqual(obs.max_depth, 45.1)
        self.assertIs(obs.observation_type, ObservationType.AT_DEPTH)

    def test_update_depth_from_top_in_pa(self):
        obs = NODBObservation()
        self.assertIsNone(obs.max_depth)
        self.assertIsNone(obs.min_depth)
        self.assertIsNone(obs.observation_type)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.coordinates['Pressure'] = 200000
        record.coordinates['Pressure'].metadata['Units'] = 'Pa'
        obs.update_from_record(record)
        self.assertAlmostEqual(obs.min_depth, 19.8365, 4)
        self.assertAlmostEqual(obs.max_depth, 19.8365, 4)
        self.assertIs(obs.observation_type, ObservationType.AT_DEPTH)

    def test_surface_parameters(self):
        obs = NODBObservation()
        self.assertIsNone(obs.surface_parameters)
        self.assertIsNone(obs.profile_parameters)
        self.assertIsNone(obs.observation_type)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.coordinates['Depth'] = 0
        record.coordinates['Depth'].metadata['Units'] = 'm'
        record.parameters['Temperature'] = 5
        record.parameters['PracticalSalinity'] = 12.123
        obs.update_from_record(record)
        self.assertIn('Temperature', obs.surface_parameters)
        self.assertIn('PracticalSalinity', obs.surface_parameters)
        self.assertNotIn('Temperature', obs.profile_parameters)
        self.assertNotIn('PracticalSalinity', obs.profile_parameters)
        self.assertIs(obs.observation_type, ObservationType.SURFACE)

    def test_profile_stuff(self):
        obs = NODBObservation()
        self.assertIsNone(obs.surface_parameters)
        self.assertIsNone(obs.profile_parameters)
        self.assertIsNone(obs.observation_type)
        self.assertIsNone(obs.max_depth)
        self.assertIsNone(obs.min_depth)
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.coordinates['Depth'] = 0
        record.coordinates['Depth'].metadata['Units'] = 'm'
        record.parameters['Temperature'] = 5
        record.parameters['PracticalSalinity'] = 12.123
        record.parameters['AirTemperature'] = 5
        subrecord = ChildRecord()
        subrecord.coordinates.set('Depth', 25, Units='m')
        subrecord.parameters.set('Temperature', 6, Units='K')
        subrecord.parameters.set('PracticalSalinity', 12.312, Units='0.001')
        subrecord.parameters.set('Conductivity', 23.12)
        record.subrecords.append_to_record_set('PROFILE', 0, subrecord)
        subrecord = ChildRecord()
        subrecord.coordinates.set('Depth', 50000, Units='mm')
        subrecord.parameters.set('Temperature', 6, Units='K')
        subrecord.parameters.set('PracticalSalinity', 12.312, Units='0.001')
        subrecord.parameters.set('Conductivity', 23.12)
        record.subrecords.append_to_record_set('PROFILE', 0, subrecord)
        subrecord = ChildRecord()
        subrecord.coordinates.set('Depth', 7500, Units='cm')
        subrecord.parameters.set('Temperature', 6, Units='K')
        subrecord.parameters.set('PracticalSalinity', 12.312, Units='0.001')
        subrecord.parameters.set('Conductivity', 23.12)
        record.subrecords.append_to_record_set('PROFILE', 0, subrecord)
        obs.update_from_record(record)
        self.assertIn('Temperature', obs.surface_parameters)
        self.assertIn('PracticalSalinity', obs.surface_parameters)
        self.assertIn('AirTemperature', obs.surface_parameters)
        self.assertNotIn('Conductivity', obs.surface_parameters)
        self.assertIn('Temperature', obs.profile_parameters)
        self.assertIn('PracticalSalinity', obs.profile_parameters)
        self.assertIn('Conductivity', obs.profile_parameters)
        self.assertNotIn('AirTemperature', obs.profile_parameters)
        self.assertIs(obs.observation_type, ObservationType.PROFILE)
        self.assertEqual(obs.min_depth, 0)
        self.assertEqual(obs.max_depth, 75)

    def test_find_observation_data(self):
        obs = NODBObservation()
        obs.obs_uuid = '12345'
        obs.received_date = datetime.date(2015, 1, 2)
        obs_data = NODBObservationData()
        obs_data.obs_uuid = '12345'
        obs_data.received_date = datetime.date(2015, 1, 2)
        self.db.insert_object(obs_data)
        self.assertIs(obs.find_observation_data(self.db), obs_data)

class TestObservationData(BaseTestCase):

    def test_record(self):
        record = ParentRecord()
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.parameters.set('AirTemperature', 5, Units="degrees_C")
        obs_data = NODBObservationData()
        self.assertIsNone(obs_data.data_record)
        self.assertIsNone(obs_data.record)
        obs_data.record = record
        self.assertIs(obs_data.record, record)
        decoder = OCProc2BinCodec()
        self.assertIsNotNone(obs_data.data_record)
        self.assertIsInstance(obs_data.data_record, bytearray)
        records = [x for x in decoder.load(obs_data.data_record)]
        self.assertEqual(1, len(records))
        self.assertEqual(records[0].coordinates['Time'].value, '2015-01-02T00:00:00+00:00')
        obs_data.clear_cache('loaded_record')
        self.assertEqual(obs_data.record.coordinates['Time'].value, '2015-01-02T00:00:00+00:00')
        obs_data.record = None
        self.assertIsNone(obs_data.record)
        self.assertIsNone(obs_data.data_record)

    def test_update_record(self):
        record = ParentRecord()
        record.record_qc_test_result(
            test_name='bogus_qc',
            test_version='1.2',
            outcome=QCResult.FAIL,
            messages=[]
        )
        record.record_qc_test_result(
            test_name='bogus_qc',
            test_version='1.3',
            outcome=QCResult.PASS,
            messages=[]
        )
        record.record_qc_test_result(
            test_name='bogus_qc2',
            test_version='1.4',
            outcome=QCResult.FAIL,
            messages=[]
        )
        record.record_qc_test_result(
            test_name='bogus_qc3',
            test_version='1.4',
            outcome=QCResult.SKIP,
            messages=[]
        )
        record.metadata['CNODCDuplicateId'] = '12345'
        record.metadata['CNODCDuplicateDate'] = '2015-10-20'
        record.metadata['CNODCStatus'] = ObservationStatus.DUBIOUS.value
        record.metadata['CNODCLevel'] = ProcessingLevel.REAL_TIME.value
        record.coordinates['Time'] = '2015-01-02T00:00:00+00:00'
        record.coordinates['Latitude'] = 45.123456
        record.coordinates['Longitude'] = -123.12345478
        record.parameters.set('AirTemperature', 5, Units="degrees_C")
        obs_data = NODBObservationData()
        self.assertIsNone(obs_data.duplicate_uuid)
        self.assertIsNone(obs_data.duplicate_received_date)
        self.assertIsNone(obs_data.status)
        self.assertIsNone(obs_data.processing_level)
        self.assertIsNone(obs_data.qc_tests)
        obs_data.record = record
        with self.subTest(msg='duplicate info'):
            self.assertEqual(obs_data.duplicate_uuid, '12345')
            self.assertEqual(obs_data.duplicate_received_date, datetime.date(2015, 10, 20))
        with self.subTest(msg='status'):
            self.assertIs(obs_data.status, ObservationStatus.DUBIOUS)
        with self.subTest(msg='processing level'):
            self.assertIs(obs_data.processing_level, ProcessingLevel.REAL_TIME)
        with self.subTest(msg='qc tests'):
            self.assertIn('bogus_qc', obs_data.qc_tests)
            self.assertEqual('1.3', obs_data.qc_tests['bogus_qc']['version'])
            self.assertEqual(QCResult.PASS.value, obs_data.qc_tests['bogus_qc']['result'])
            self.assertIsInstance(obs_data.qc_tests['bogus_qc']['date_run'], str)
            self.assertIn('bogus_qc2', obs_data.qc_tests)
            self.assertEqual('1.4', obs_data.qc_tests['bogus_qc2']['version'])
            self.assertEqual(QCResult.FAIL.value, obs_data.qc_tests['bogus_qc2']['result'])
            self.assertIsInstance(obs_data.qc_tests['bogus_qc2']['date_run'], str)
            self.assertIn('bogus_qc3', obs_data.qc_tests)
            self.assertEqual('1.4', obs_data.qc_tests['bogus_qc3']['version'])
            self.assertEqual(QCResult.SKIP.value, obs_data.qc_tests['bogus_qc3']['result'])
            self.assertIsInstance(obs_data.qc_tests['bogus_qc3']['date_run'], str)

    def test_find_by_uuid(self):
        obs_data = NODBObservationData(
            obs_uuid='12345',
            received_date=datetime.date(2015, 1, 2),
            source_file_uuid='23456',
            message_idx=5,
            record_idx=10,
            processing_level=ProcessingLevel.REAL_TIME.value
        )
        obs_data2 = NODBObservationData(
            obs_uuid='123456',
            received_date=datetime.date(2015, 1, 2),
            source_file_uuid='23456',
            message_idx=5,
            record_idx=10,
            processing_level=ProcessingLevel.DELAYED_MODE.value
        )
        self.db.insert_object(obs_data)
        self.db.insert_object(obs_data2)
        od2 = NODBObservationData.find_by_uuid(self.db, '12345', '2015-01-02')
        self.assertIs(obs_data, od2)
        s1 = NODBObservationData.find_by_source_info(self.db, '23456', '2015-01-02', 5, 10, ProcessingLevel.REAL_TIME)
        self.assertIs(obs_data, s1)
        s2 = NODBObservationData.find_by_source_info(self.db, '23456', '2015-01-02', 5, 10, ProcessingLevel.DELAYED_MODE)
        self.assertIs(obs_data2, s2)
        s3 = NODBObservationData.find_by_source_info(self.db, '23456', '2015-01-02', 5, 10)
        self.assertIsNone(s3)

    def test_find_observation(self):
        obs = NODBObservation()
        obs.obs_uuid = '12345'
        obs.received_date = datetime.date(2015, 1, 2)
        self.db.insert_object(obs)
        obs_data = NODBObservationData()
        obs_data.obs_uuid = '12345'
        obs_data.received_date = datetime.date(2015, 1, 2)
        self.assertIs(obs_data.find_observation(self.db), obs)


class TestWorkingRecord(BaseTestCase):

    def test_find_by_uuid(self):
        wr = NODBWorkingRecord(working_uuid='1')
        self.db.insert_object(wr)
        wr2 = NODBWorkingRecord.find_by_uuid(self.db, '1')
        self.assertIs(wr, wr2)

    def test_find_by_source_info(self):
        wr = NODBWorkingRecord(
            working_uuid='1',
            source_file_uuid='2',
            received_date='2015-10-11',
            message_idx=5,
            record_idx=6
        )
        self.db.insert_object(wr)
        wr2 = NODBWorkingRecord.find_by_source_info(self.db, '2', '2015-10-11', 5, 6)
        self.assertIs(wr, wr2)

    def test_bulk_update_batch_uuid(self):
        self.db.insert_object(NODBWorkingRecord(working_uuid='1', qc_batch_id='1'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='2', qc_batch_id='1'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='3', qc_batch_id='2'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='4', qc_batch_id='1'))
        self.db.insert_object(NODBWorkingRecord(working_uuid='5', qc_batch_id='2'))
        NODBWorkingRecord.bulk_set_batch_uuid(self.db, ['1', '2', '5'], '12345')
        self.assertEqual(NODBWorkingRecord.find_by_uuid(self.db, '1').qc_batch_id, '12345')
        self.assertEqual(NODBWorkingRecord.find_by_uuid(self.db, '2').qc_batch_id, '12345')
        self.assertEqual(NODBWorkingRecord.find_by_uuid(self.db, '3').qc_batch_id, '2')
        self.assertEqual(NODBWorkingRecord.find_by_uuid(self.db, '4').qc_batch_id, '1')
        self.assertEqual(NODBWorkingRecord.find_by_uuid(self.db, '5').qc_batch_id, '12345')







