import logging

from medsutil.ocproc2 import ParentRecord, MultiElement, SingleElement
from pipeman.programs.glider.ego_convert import ego_sensor_info
from medsutil.sanitize import netcdf_string_to_bytes
from tests.helpers.base_test_case import BaseTestCase
import netCDF4 as nc
import typing as t
from pipeman.programs.glider.ego_decode import GliderEGOMapper


class TestGliderDecodeTools(BaseTestCase):

    def test_isoformat_ego_date(self):
        self.assertEqual('2015-01-02', GliderEGOMapper._isoformat_ego_date(None, '20150102', None))
        self.assertEqual('2015-01-02T03:00:00', GliderEGOMapper._isoformat_ego_date(None, '2015010203', None))
        self.assertEqual('2015-01-02T03:04:00', GliderEGOMapper._isoformat_ego_date(None, '201501020304', None))
        self.assertEqual('2015-01-02T03:04:05', GliderEGOMapper._isoformat_ego_date(None, '20150102030405', None))
        bad = ['2015010', '2015', '201501', '201501020', '20150102030', '2015010203040', '20150102030405000000',
               '20150102030405+00:00', '20150102030405Z']
        for test in bad:
            with self.subTest(bad_date=test):
                with self.assertRaisesCNODCError('EGO-DECODE-1000'):
                    GliderEGOMapper._isoformat_ego_date(None, test, None)

    def test_old_ego_sensor_mapping_missing(self):
        with nc.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT')
            self._add_old_ego_sensor_info(ds, [
                ('PRES', 'seabird electronics ctd 41cp', '12345'),
                ('TEMP', 'seabird electronics ctd 41cp', '12345'),
                ('COND', 'seabird electronics ctd 41cp', '23456'),
                ('DOXY', 'aanderaa oxy 5013', '34567'),
                ('MOLDOXY', 'aanderaa oxy 5013', '34567'),
                ('OTHER', '', '12345'),
                ('MISSING_SERIAL', 'flbbcd', ''),
                ('BBP700', 'flbbcd', '45678'),
                ('TEST', 'wetlabs flbbcdslc', '56789')
            ])
            with self.assertRaisesCNODCError('GLIDER-1000'):
                sensor_info, param_map = ego_sensor_info(ds, {
                    'seabird electronics ctd 41cp': {
                        'make': 'SeaBird Electronics', 'model': 'CTD 41CP', 'type': 'CTD',
                    },
                    'flbbcd': {
                        'make': 'Unknown', 'model': 'Unknown', 'type': 'FLUOROMETER',
                    },
                    'wetlabs flbbcdslc': {
                        'make': 'Wetlabs', 'model': 'FLBBCD-SLC', 'type': 'FLUOROMETER',
                    }
                }, {}, {})

    def test_old_ego_sensor_mapping(self):
        with nc.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT')
            self._add_old_ego_sensor_info(ds, [
                ('PRES', 'seabird electronics ctd 41cp', '12345'),
                ('TEMP', 'seabird electronics ctd 41cp', '12345'),
                ('COND', 'seabird electronics ctd 41cp', '23456'),
                ('DOXY', 'aanderaa oxy 5013', '34567'),
                ('MOLDOXY', 'aanderaa oxy 5013', '34567'),
                ('OTHER', '', '12345'),
                ('MISSING_SERIAL', 'flbbcd', ''),
                ('BBP700', 'flbbcd', '45678'),
                ('TEST', 'wetlabs flbbcdslc', '56789')
            ])
            sensor_info, param_map = ego_sensor_info(ds, {
                'seabird electronics ctd 41cp': {
                    'make': 'SeaBird Electronics', 'model': 'CTD 41CP', 'type': 'CTD',
                },
                'aanderaa oxy 5013': {
                    'make': 'Aanderaa', 'model': 'Oxygen Optode 5013', 'type': 'DOXY',
                },
                'flbbcd': {
                    'make': 'Unknown', 'model': 'Unknown', 'type': 'FLUOROMETER',
                },
                'wetlabs flbbcdslc': {
                    'make': 'Wetlabs', 'model': 'FLBBCD-SLC', 'type': 'FLUOROMETER',
                }
            }, {}, {})
            with self.subTest(msg='ctd12345'):
                self.assertIn('SENSOR_CTD_12345', sensor_info)
                self.assertEqual('CTD', sensor_info['SENSOR_CTD_12345']['type'])
                self.assertEqual('SeaBird Electronics', sensor_info['SENSOR_CTD_12345']['make'])
                self.assertEqual('CTD 41CP', sensor_info['SENSOR_CTD_12345']['model'])
                self.assertEqual('12345', sensor_info['SENSOR_CTD_12345']['serial'])
                self.assertIn('PRES', param_map)
                self.assertEqual(param_map['PRES'], 'SENSOR_CTD_12345')
                self.assertIn('TEMP', param_map)
                self.assertEqual(param_map['TEMP'], 'SENSOR_CTD_12345')
            with self.subTest(msg='ctd23456'):
                self.assertIn('SENSOR_CTD_23456', sensor_info)
                self.assertEqual('CTD', sensor_info['SENSOR_CTD_23456']['type'])
                self.assertEqual('SeaBird Electronics', sensor_info['SENSOR_CTD_23456']['make'])
                self.assertEqual('CTD 41CP', sensor_info['SENSOR_CTD_23456']['model'])
                self.assertEqual('23456', sensor_info['SENSOR_CTD_23456']['serial'])
                self.assertIn('COND', param_map)
                self.assertEqual(param_map['COND'], 'SENSOR_CTD_23456')
            with self.subTest(msg='doxy34567'):
                self.assertIn('SENSOR_DOXY_34567', sensor_info)
                self.assertEqual('DOXY', sensor_info['SENSOR_DOXY_34567']['type'])
                self.assertEqual('Aanderaa', sensor_info['SENSOR_DOXY_34567']['make'])
                self.assertEqual('Oxygen Optode 5013', sensor_info['SENSOR_DOXY_34567']['model'])
                self.assertEqual('34567', sensor_info['SENSOR_DOXY_34567']['serial'])
                self.assertIn('DOXY', param_map)
                self.assertEqual(param_map['DOXY'], 'SENSOR_DOXY_34567')
                self.assertIn('MOLDOXY', param_map)
                self.assertEqual(param_map['MOLDOXY'], 'SENSOR_DOXY_34567')
            with self.subTest(msg='fluoroUnknown'):
                self.assertIn('SENSOR_FLUOROMETER_unknown', sensor_info)
                self.assertEqual('FLUOROMETER', sensor_info['SENSOR_FLUOROMETER_unknown']['type'])
                self.assertEqual('Unknown', sensor_info['SENSOR_FLUOROMETER_unknown']['make'])
                self.assertEqual('Unknown', sensor_info['SENSOR_FLUOROMETER_unknown']['model'])
                self.assertEqual('unknown', sensor_info['SENSOR_FLUOROMETER_unknown']['serial'])
                self.assertIn('MISSING_SERIAL', param_map)
                self.assertEqual(param_map['MISSING_SERIAL'], 'SENSOR_FLUOROMETER_unknown')
            with self.subTest(msg='fluoro45678'):
                self.assertIn('SENSOR_FLUOROMETER_45678', sensor_info)
                self.assertEqual('FLUOROMETER', sensor_info['SENSOR_FLUOROMETER_45678']['type'])
                self.assertEqual('Unknown', sensor_info['SENSOR_FLUOROMETER_45678']['make'])
                self.assertEqual('Unknown', sensor_info['SENSOR_FLUOROMETER_45678']['model'])
                self.assertEqual('45678', sensor_info['SENSOR_FLUOROMETER_45678']['serial'])
                self.assertIn('BBP700', param_map)
                self.assertEqual(param_map['BBP700'], 'SENSOR_FLUOROMETER_45678')
            with self.subTest(msg='fluoro56789'):
                self.assertIn('SENSOR_FLUOROMETER_56789', sensor_info)
                self.assertEqual('FLUOROMETER', sensor_info['SENSOR_FLUOROMETER_56789']['type'])
                self.assertEqual('Wetlabs', sensor_info['SENSOR_FLUOROMETER_56789']['make'])
                self.assertEqual('FLBBCD-SLC', sensor_info['SENSOR_FLUOROMETER_56789']['model'])
                self.assertEqual('56789', sensor_info['SENSOR_FLUOROMETER_56789']['serial'])
                self.assertIn('TEST', param_map)
                self.assertEqual(param_map['TEST'], 'SENSOR_FLUOROMETER_56789')
            self.assertNotIn('OTHER', param_map)

    def test_new_ego_sensor_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            self._add_new_ego_sensor_info(ds, [
                ('CTD_PRES', "Company B", "Model X", "12345", "", ""),
                ('CTD_TEMP', "Company B", "Model X", "12345", "", ""),
                ('CTD_COND', "Company B", "Model X", "12345", "", ""),
                ('FLUOROMETER_24', "Company A", "Model Y", "678", "", ""),
                ('OPTODE_DOXY', "Company C", "Model Z", "90", "", "")
            ], [
                ('PRES', 'CTD_PRES'),
                ('DOXY', 'OPTODE_DOXY'),
                ('BBP700', 'FLUOROMETER_24'),
                ('TEMP', 'CTD_TEMP'),
                ('COND', 'CTD_COND'),
                ('TEMPCOUNT', 'CTD_TEMP'),
                ('MOLDOXY', 'OPTODE_DOXY'),
                ('OTHER', ''),
            ])
            sensor_info, param_map = ego_sensor_info(ds, {}, {}, {})
            self.assertEqual(len(sensor_info), 3)
            with self.subTest(msg='CTD'):
                self.assertIn('SENSOR_CTD_12345', sensor_info)
                self.assertEqual('CTD', sensor_info['SENSOR_CTD_12345']['type'])
                self.assertEqual('Company B', sensor_info['SENSOR_CTD_12345']['make'])
                self.assertEqual('Model X', sensor_info['SENSOR_CTD_12345']['model'])
                self.assertEqual('12345', sensor_info['SENSOR_CTD_12345']['serial'])
            with self.subTest(msg='FLUOROMETER'):
                self.assertIn('SENSOR_FLUOROMETER_678', sensor_info)
                self.assertEqual('FLUOROMETER', sensor_info['SENSOR_FLUOROMETER_678']['type'])
                self.assertEqual('Company A', sensor_info['SENSOR_FLUOROMETER_678']['make'])
                self.assertEqual('Model Y', sensor_info['SENSOR_FLUOROMETER_678']['model'])
                self.assertEqual('678', sensor_info['SENSOR_FLUOROMETER_678']['serial'])
            with self.subTest(msg='DOXY'):
                self.assertIn('SENSOR_DOXY_90', sensor_info)
                self.assertEqual('DOXY', sensor_info['SENSOR_DOXY_90']['type'])
                self.assertEqual('Company C', sensor_info['SENSOR_DOXY_90']['make'])
                self.assertEqual('Model Z', sensor_info['SENSOR_DOXY_90']['model'])
                self.assertEqual('90', sensor_info['SENSOR_DOXY_90']['serial'])
            with self.subTest(msg='PRES'):
                self.assertIn('PRES', param_map)
                self.assertEqual('SENSOR_CTD_12345', param_map['PRES'])
            with self.subTest(msg='DOXY'):
                self.assertIn('DOXY', param_map)
                self.assertEqual('SENSOR_DOXY_90', param_map['DOXY'])
            with self.subTest(msg='BBP700'):
                self.assertIn('BBP700', param_map)
                self.assertEqual('SENSOR_FLUOROMETER_678', param_map['BBP700'])
            with self.subTest(msg='TEMP'):
                self.assertIn('TEMP', param_map)
                self.assertEqual('SENSOR_CTD_12345', param_map['TEMP'])
            with self.subTest(msg='COND'):
                self.assertIn('COND', param_map)
                self.assertEqual('SENSOR_CTD_12345', param_map['COND'])
            with self.subTest(msg='TEMPCOUNT'):
                self.assertIn('TEMPCOUNT', param_map)
                self.assertEqual('SENSOR_CTD_12345', param_map['TEMPCOUNT'])
            with self.subTest(msg='MOLDOXY'):
                self.assertIn('MOLDOXY', param_map)
                self.assertEqual('SENSOR_DOXY_90', param_map['MOLDOXY'])
            self.assertNotIn('OTHER', param_map)

    def test_bad_new_sensor_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            self._add_new_ego_sensor_info(ds, [
                ('CTD_PRES', "Company B", "Model X", "12345", "", ""),
                ('CTD_TEMP', "Company B", "Model X", "12345", "", ""),
                ('CTD_COND', "Company B", "Model X", "12345", "", ""),
                ('FLUOROMETER_24', "Company A", "Model Y", "678", "", ""),
                ('NEW_SENSOR', "Company C", "Model Z", "90", "", "")
            ], [
                ('PRES', 'CTD_PRES'),
                ('DOXY', 'OPTODE_DOXY'),
                ('BBP700', 'FLUOROMETER_24'),
                ('TEMP', 'CTD_TEMP'),
                ('COND', 'CTD_COND'),
                ('TEMPCOUNT', 'CTD_TEMP'),
                ('MOLDOXY', 'OPTODE_DOXY'),
                ('OTHER', ''),
            ])
            with self.assertRaisesCNODCError('GLIDER-1001'):
                sensor_info, param_map = ego_sensor_info(ds, {}, {}, {})

    @staticmethod
    def _add_old_ego_sensor_info(ds: nc.Dataset, param_info: t.Sequence[tuple[str, str, str]]):
        for param in param_info:
            v = ds.createVariable(param[0], 'f8', ('N_COUNT',))
            if param[1]:
                v.setncattr('sensor_name', param[1])
            if param[2]:
                v.setncattr('sensor_serial_number', param[2])

    @staticmethod
    def _add_new_ego_sensor_info(ds: nc.Dataset,
                                 sensor_info: t.Sequence[tuple[str, str, str, str, str, str]],
                                 parameter_info: t.Sequence[tuple[str, str]]):
            ds.createDimension('N_SENSORS', len(sensor_info))
            ds.createDimension('N_PARAMS', len(parameter_info))
            ds.createDimension('STRING256', 256)
            var_names = ['SENSOR', 'SENSOR_MAKER', 'SENSOR_MODEL', 'SENSOR_SERIAL_NO', 'SENSOR_MOUNT', 'SENSOR_ORIENTATION']
            for idx, var_name in enumerate(var_names):
                v = ds.createVariable(var_name, 'S1', ('N_SENSORS', 'STRING256',))
                v[:] = netcdf_string_to_bytes([x[idx] for x in sensor_info], 256)
            var_names = ['PARAMETER', 'PARAMETER_SENSOR']
            for idx, var_name in enumerate(var_names):
                v = ds.createVariable(var_name, 'S1', ('N_PARAMS', 'STRING256', ))
                v[:] = netcdf_string_to_bytes([x[idx] for x in parameter_info], 256)

    @staticmethod
    def _add_ego_parameter(ds: nc.Dataset, parameter_name, data, qc_data=None, units=None, qc_var_name=None):
        pres = ds.createVariable(parameter_name, 'f8', ('N_COUNT',))
        if units:
            pres.setncattr('units', units)
        pres[:] = data
        if qc_data:
            pres_qc = ds.createVariable(qc_var_name or f'{parameter_name}_QC', 'f8', ('N_COUNT',))
            pres_qc[:] = qc_data

    @staticmethod
    def _add_ego_variable_value(ds: nc.Dataset, var_name: str, value):
        if value is None:
            v = ds.createVariable(var_name, 'S1', ('STRING256',))
            v[:] = netcdf_string_to_bytes("", 256)
        if isinstance(value, str):
            v = ds.createVariable(var_name, 'S1', ('STRING256',))
            v[:] = netcdf_string_to_bytes(value, 256)
        elif isinstance(value, int):
            v = ds.createVariable(var_name, 'i4')
            v[:] = [value]
        elif isinstance(value, float):
            v = ds.createVariable(var_name, 'f8')
            v[:] = [value]

class TestGliderDecodeFull(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            TestGliderDecodeTools._add_new_ego_sensor_info(ds, [
                ('CTD_PRES', 'Company A', 'Model X', '12345' ,'', '')
            ], [
                ('PRES', 'CTD_PRES')
            ])
            ds.setncattr('doi', '10.x.x.x/abc')
            ds.setncattr('area', 'Ottawa')
            ds.setncattr('project', 'Testing')
            ds.setncattr('program', 'Modernization')
            ds.setncattr('deployment_code', '12345')
            ds.setncattr('wmo_platform_code', '12345678')
            ds.setncattr('platform_code', 'LD50')
            ds.setncattr('platform_name', 'SEA000')
            ds.setncattr('abstract', 'abstract')
            ds.setncattr('summary', 'summary')
            ds.setncattr('observatory', 'MEDS;DFO')
            ds.setncattr('institution', 'BIO;DFO')
            ds.setncattr('principal_investigator', 'Erin Turnbull; Anh Tran')
            ds.setncattr('data_mode', 'R')
            ds.setncattr('update_interval', 'daily')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_DATE', '20151009')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_LATITUDE', 45.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_LONGITUDE', 129.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_DATE', '20150909')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_LATITUDE', 44.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_LONGITUDE', 128.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_REFERENCE_STATION_ID', 'stuff')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'BATTERY_PACKS', '4xlithiumion in parallel')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'GLIDER_MANUAL_VERSION', 'v4.1.2')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'CUSTOMIZATION', 'we built a portal to the underworld in the glider')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'SPECIAL_FEATURES', 'underworld portal')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'GLIDER_SERIAL_NO', '54321')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'PLATFORM_FAMILY', 'COASTAL_GLIDER')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_QC', 1)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_QC', 5)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'FIRMWARE_VERSION', 'v2.3.4')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'BATTERY_TYPE', 'Lithium\x00ION')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'ANOMALY', 'the glider accidently summoned a demon')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'OPERATING_INSTITUTION', 'DFO;C-PROOF')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'GLIDER_OWNER', 'NAFC')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'PLATFORM_TYPE', 'SLOCUM_SG2')
            ds.createDimension('N_COUNT',)
            pq = ds.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
            pq[:] = [1, 1, 1]
            TestGliderDecodeTools._add_ego_parameter(ds, 'LATITUDE', [12, 24, 36], units='degree_north')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LONGITUDE', [13, 25, 37], units='degreeE')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LATITUDE_GPS', [12.1, 24.1, 36.1], [1, 1, 1], units='degree_north')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LONGITUDE_GPS', [13.1, 25.1, 37.1], [1, 1, 1], units='degreeE')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TIME_GPS', [5.01, 10.01, 15.01], [2, 2, 2], 'days since 1950-01-01T00:00:00')
            TestGliderDecodeTools._add_ego_parameter(ds, 'JULD', [5, 10, 15], [2, 2, 2], 'days since 1950-01-01T00:00:00', 'TIME_QC')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PRES', [1.2, 2.4, 3.6], [1, 1, 1], 'decibar')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CHLA', [1.3, 2.5, 3.7], [1, 1, 1], 'mg/m3')
            TestGliderDecodeTools._add_ego_parameter(ds, 'FLUORESCENCE_CHLA', [2, 3, 4], [1, 1, 1], '1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PSAL', [24.9, 25.1, 29.1], [1, 1, 1], 'psu')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TEMP', [11.1, 22.2, 11.2], [1, 1, 1], 'degree_Celsius')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CDOM', [0.5, 0.1, 0.7], [1, 1, 1], 'ppb')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PHASE', [2, 3, 2])
            TestGliderDecodeTools._add_ego_parameter(ds, 'PHASE_NUMBER', [0, 0, 1])
            TestGliderDecodeTools._add_ego_parameter(ds, 'DOXY', [12.1, 13.1, 14.1], [1, 1, 1], 'micromol/kg')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CNDC', [12.2, 13.3, 14.4], [1, 1, 1], 'mhos/m')
            TestGliderDecodeTools._add_ego_parameter(ds, 'MOLAR_DOXY', [5.1, 6.1, 7.1], [1, 1, 1], 'micromol/l')
            TestGliderDecodeTools._add_ego_parameter(ds, 'FREQUENCY_DOXY', [5.2, 6.2, 7.1], [1, 1, 1], 's-1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TURBIDITY', [2, 3, 4], [1, 1, 1], '1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'BBP700', [6.1, 7.1, 8.1], [1, 1, 1], 'm-1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'DPHASE_DOXY', [10, 11, 12], [1, 1, 1], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'RPHASE_DOXY', [11, 12, 13], [1, 1, 1], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'BPHASE_DOXY', [12, 13, 14], [1, 1, 1], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TEMP_DOXY', [13, 14, 15], [1, 1, 1], 'degree_Celsius')

            mapper = GliderEGOMapper(ds, GliderEGOMapper.DEFAULT_MAPPING_FILE)
            logging.disable(logging.WARNING)
            cls.decoded_records = [x for x in mapper.build_records()]
            logging.disable(logging.NOTSET)

    def test_record_length(self):
        self.assertEqual(3, len(self.decoded_records))
        for x in self.decoded_records:
            self.assertIsInstance(x, ParentRecord)

    def test_latitude(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Latitude'], MultiElement)
        self.assertEqual(2, len(record.coordinates['Latitude'].value))
        by_type: dict[str, SingleElement] = {}
        for x in record.coordinates['Latitude'].value:
            by_type[x.metadata.best('SensorType')] = x
        self.assertIn('interpolated', by_type)
        self.assertIn('satnav', by_type)
        self.assertEqual(12, by_type['interpolated'].value)
        self.assertEqual(1, by_type['interpolated'].metadata['Quality'].value)
        self.assertEqual('interpolated', by_type['interpolated'].metadata['SensorType'].value)
        self.assertEqual('degrees_north', by_type['interpolated'].metadata['Units'].value)
        self.assertEqual(12.1, by_type['satnav'].value)
        self.assertEqual(1, by_type['satnav'].metadata['Quality'].value)
        self.assertEqual('satnav', by_type['satnav'].metadata['SensorType'].value)
        self.assertEqual('degrees_north', by_type['satnav'].metadata['Units'].value)

    def test_longitude(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Longitude'], MultiElement)
        self.assertEqual(2, len(record.coordinates['Longitude'].value))
        by_type: dict[str, SingleElement] = {}
        for x in record.coordinates['Longitude'].value:
            by_type[x.metadata.best('SensorType')] = x
        self.assertIn('interpolated', by_type)
        self.assertIn('satnav', by_type)
        self.assertEqual(13, by_type['interpolated'].value)
        self.assertEqual(1, by_type['interpolated'].metadata['Quality'].value)
        self.assertEqual('interpolated', by_type['interpolated'].metadata['SensorType'].value)
        self.assertEqual('degrees_east', by_type['interpolated'].metadata['Units'].value)
        self.assertEqual(13.1, by_type['satnav'].value)
        self.assertEqual(1, by_type['satnav'].metadata['Quality'].value)
        self.assertEqual('satnav', by_type['satnav'].metadata['SensorType'].value)
        self.assertEqual('degrees_east', by_type['satnav'].metadata['Units'].value)

    def test_time(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Time'], MultiElement)
        self.assertEqual(2, len(record.coordinates['Time'].value))
        by_type: dict[str, SingleElement] = {}
        for x in record.coordinates['Time'].value:
            by_type[x.metadata.best('SensorType')] = x
        self.assertIn('interpolated', by_type)
        self.assertIn('satnav', by_type)
        self.assertEqual('1950-01-06T00:00:00+00:00', by_type['interpolated'].value)
        self.assertEqual(2, by_type['interpolated'].metadata['Quality'].value)
        self.assertEqual('interpolated', by_type['interpolated'].metadata['SensorType'].value)
        self.assertEqual('1950-01-06T00:14:24+00:00', by_type['satnav'].value)
        self.assertEqual(2, by_type['satnav'].metadata['Quality'].value)
        self.assertEqual('satnav', by_type['satnav'].metadata['SensorType'].value)

    def test_pressure(self):
        record = self.decoded_records[0]
        self.assertEqual(1.2, record.coordinates.best('Pressure'))
        self.assertEqual(1, record.coordinates['Pressure'].metadata.best('Quality'))
        self.assertEqual('dbar', record.coordinates['Pressure'].metadata.best('Units'))
        self.assertEqual('ctd', record.coordinates['Pressure'].metadata.best('SensorType'))
        self.assertEqual('Company A', record.coordinates['Pressure'].metadata.best('SensorMake'))
        self.assertEqual('Model X', record.coordinates['Pressure'].metadata.best('SensorModel'))
        self.assertEqual('12345', record.coordinates['Pressure'].metadata.best('SensorSerial'))

    def test_simple_parameters(self):
        record = self.decoded_records[0]
        tests = [
            ('ChlorophyllA', 1.3, 1, 'mg m-3', None),
            ('ChlorophyllAFluorescence', 2, 1, '1', None),
            ('PracticalSalinity', 24.9, 1, "0.001", None),
            ('Temperature', 11.1, 1, 'degrees_Celsius', {'TemperatureScale': 'ITS-90'}),
            ('ColoredDissolvedOrganicMatter', 0.5, 1, 'ppb', None),
            ('DissolvedOxygen', 12.1, 1, 'umol kg-1', None),
            ('DissolvedOxygenFrequency', 5.2, 1, 's-1', None),
            ('DissolvedOxygenMolar', 5.1, 1, 'umol L-1', None),
            ('Turbidity', 2, 1, '1', None),
            ('ParticleBackscatter', 6.1, 1, 'm-1', {'BackscatterWavelength': 700}),
        ]
        for pcode, val, qf, units, metadata in tests:
            with self.subTest(pcode=pcode):
                self.assertIn(pcode, record.parameters, pcode)
                param = record.parameters[pcode]
                self.assertIsInstance(param, SingleElement)
                self.assertEqual(val, param.value)
                self.assertEqual(param.metadata['Quality'].value, qf)
                if units is not None:
                    self.assertEqual(param.metadata['Units'].value, units)
                if metadata:
                    for key in metadata:
                        self.assertIn(key, param.metadata)
                        self.assertEqual(param.metadata[key].value, metadata[key])

    def test_simple_metadata(self):
        record = self.decoded_records[0]
        tests = [
            ('DOI', '10.x.x.x/abc', None),
            ('Abstract', 'abstract', None),
            ('Area', 'Ottawa', None),
            ('Project', 'Testing', None),
            ('Program', 'Modernization', None),
            ('DeploymentCruiseID', '12345', None),
            ('WMOID', '12345678', None),
            ('PlatformID', 'LD50', None),
            ('PlatformName', 'SEA000', None),
            ('Summary', 'summary', None),
            ('Observatory', ['meds', 'dfo'], None),
            ('Institution', ['bio', 'dfo'], None),
            ('PrincipalInvestigator', ['Erin Turnbull', 'Anh Tran'], None),
            ('CruiseID', 'SEA000_20150909', None),
            ('CNODCLevel', 'REAL_TIME', None),
            ('DataUpdateInterval', 'daily', None),
            ('EndDate', '2015-10-09', {'Quality': 5}),
            ('EndLatitude', 45.12, {'Quality': 5}),
            ('EndLongitude', 129.12, {'Quality': 5}),
            ('StartDate', '2015-09-09', {'Quality': 1}),
            ('StartLatitude', 44.12, {'Quality': 1}),
            ('StartLongitude', 128.12, {'Quality': 1}),
            ('ReferenceStationNames', 'stuff', None),
            ('BatteryDescription', '4xlithiumion in parallel', None),
            ('DocumentationVersion', 'v4.1.2', None),
            ('PlatformCustomization', 'we built a portal to the underworld in the glider', None),
            ('PlatformDetails', 'underworld portal', None),
            ('PlatformSerial', '54321', None),
            ('PlatformCategory', 'glider_coastal', None),
            ('FirmwareVersion', 'v2.3.4', {'FirmwareType': 'science'}),
            ('BatteryType', 'lithium', None),
            ('Notes', 'the glider accidently summoned a demon', None),
            ('Operator', ['dfo', 'c-proof'], None),
            ('PlatformOwner', 'nafc', None),
            ('GliderPhaseCode', 2, {'GliderPhaseCodeSource': 'ego'}),
            ('GliderPhaseNumber', 0, None),
            ('PlatformModel', 'Slocum G2', None),
            ('PlatformMake', 'Teledyne Webb Research', None),
        ]
        for prop, value, prop_md in tests:
            with self.subTest(property=prop):
                self.assertIn(prop, record.metadata)
                if isinstance(value, (list, set, tuple)):
                    self.assertIsInstance(record.metadata[prop], MultiElement)
                    values = [x.value for x in record.metadata[prop].value]
                    for n in value:
                        self.assertIn(n, values)
                else:
                    self.assertIsInstance(record.metadata[prop], SingleElement)
                    self.assertEqual(record.metadata[prop].value, value)
                    if prop_md:
                        for subprop in prop_md:
                            self.assertIn(subprop, record.metadata[prop].metadata)
                            self.assertEqual(prop_md[subprop], record.metadata[prop].metadata[subprop].value)

class TestGliderDecodeMinimum(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.setncattr('deployment_code', '12345')
            ds.setncattr('wmo_platform_code', '12345678')
            ds.setncattr('platform_code', 'LD50')
            ds.setncattr('platform_name', 'SEA000')
            ds.setncattr('institution', 'BIO;DFO')
            ds.setncattr('data_mode', 'R')
            ds.createDimension('STRING256', 256)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_DATE', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_LATITUDE', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_LONGITUDE', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_DATE', '20150909')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_LATITUDE', 44.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_LONGITUDE', 128.12)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_REFERENCE_STATION_ID', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'BATTERY_PACKS', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'CUSTOMIZATION', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'GLIDER_SERIAL_NO', '54321')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'PLATFORM_FAMILY', 'COASTAL_GLIDER')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_START_QC', 1)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'DEPLOYMENT_END_QC', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'BATTERY_TYPE', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'ANOMALY', None)
            TestGliderDecodeTools._add_ego_variable_value(ds, 'OPERATING_INSTITUTION', 'DFO')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'GLIDER_OWNER', 'DFO')
            TestGliderDecodeTools._add_ego_variable_value(ds, 'PLATFORM_TYPE', 'SLOCUM_SG2')
            ds.createDimension('N_COUNT',)
            pq = ds.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
            pq[:] = [1, 1, 1]
            TestGliderDecodeTools._add_ego_parameter(ds, 'LATITUDE', [12, None, 36], units='degree_north')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LONGITUDE', [13, None, 37], units='degreeE')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LATITUDE_GPS', [None, None, None], [None, None, None], units='degree_north')
            TestGliderDecodeTools._add_ego_parameter(ds, 'LONGITUDE_GPS', [None, None, None], [None, None, None], units='degreeE')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TIME_GPS', [None, None, None], [None, None, None], 'days since 1950-01-01T00:00:00')
            TestGliderDecodeTools._add_ego_parameter(ds, 'JULD', [5, None, 15], [2, None, 2], 'days since 1950-01-01T00:00:00', 'TIME_QC')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PRES', [1.2, None, 3.6], [1, None, 1], 'decibar')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CHLA', [None, None, None], [None, None, None], 'mg/m3')
            TestGliderDecodeTools._add_ego_parameter(ds, 'FLUORESCENCE_CHLA', [None, None, None], [None, None, None], '1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PSAL', [24.9, None, 29.1], [1, None, 1], 'psu')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TEMP', [11.1, None, 11.2], [1, None, 1], 'degree_Celsius')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CDOM', [None, None, None], [None, None, None], 'ppb')
            TestGliderDecodeTools._add_ego_parameter(ds, 'PHASE', [2, 3, 2])
            TestGliderDecodeTools._add_ego_parameter(ds, 'PHASE_NUMBER', [0, 0, 1])
            TestGliderDecodeTools._add_ego_parameter(ds, 'DOXY', [None, None, None], [None, None, None], 'micromol/kg')
            TestGliderDecodeTools._add_ego_parameter(ds, 'CNDC', [None, None, None], [None, None, None], 'mhos/m')
            TestGliderDecodeTools._add_ego_parameter(ds, 'MOLAR_DOXY', [None, None, None], [None, None, None], 'micromol/l')
            TestGliderDecodeTools._add_ego_parameter(ds, 'FREQUENCY_DOXY', [None, None, None], [None, None, None], 's-1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TURBIDITY', [None, None, None], [None, None, None], '1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'BBP700', [None, None, None], [None, None, None], 'm-1')
            TestGliderDecodeTools._add_ego_parameter(ds, 'DPHASE_DOXY', [None, None, None], [None, None, None], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'RPHASE_DOXY', [None, None, None], [None, None, None], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'BPHASE_DOXY', [None, None, None], [None, None, None], 'degree')
            TestGliderDecodeTools._add_ego_parameter(ds, 'TEMP_DOXY', [None, None, None], [None, None, None], 'degree_Celsius')

            mapper = GliderEGOMapper(ds, GliderEGOMapper.DEFAULT_MAPPING_FILE)
            logging.disable(logging.WARNING)
            cls.decoded_records = [x for x in mapper.build_records()]
            logging.disable(logging.NOTSET)

    def test_record_length(self):
        self.assertEqual(3, len(self.decoded_records))
        for x in self.decoded_records:
            self.assertIsInstance(x, ParentRecord)

    def test_latitude(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Latitude'], SingleElement)
        lat = record.coordinates['Latitude']
        self.assertEqual(12, lat.value)
        self.assertEqual(1, lat.metadata['Quality'].value)
        self.assertEqual('interpolated', lat.metadata['SensorType'].value)
        self.assertEqual('degrees_north', lat.metadata['Units'].value)

    def test_longitude(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Longitude'], SingleElement)
        lon = record.coordinates['Longitude']
        self.assertEqual(13, lon.value)
        self.assertEqual(1, lon.metadata['Quality'].value)
        self.assertEqual('interpolated', lon.metadata['SensorType'].value)
        self.assertEqual('degrees_east', lon.metadata['Units'].value)

    def test_time(self):
        record = self.decoded_records[0]
        self.assertIsInstance(record.coordinates['Time'], SingleElement)
        time = record.coordinates['Time']
        self.assertEqual('1950-01-06T00:00:00+00:00', time.value)
        self.assertEqual(2, time.metadata['Quality'].value)
        self.assertEqual('interpolated', time.metadata['SensorType'].value)

    def test_pressure(self):
        record = self.decoded_records[0]
        self.assertEqual(1.2, record.coordinates.best('Pressure'))
        self.assertEqual(1, record.coordinates['Pressure'].metadata.best('Quality'))
        self.assertEqual('dbar', record.coordinates['Pressure'].metadata.best('Units'))

    def test_coordinates2(self):
        record = self.decoded_records[1]
        self.assertNotIn('Pressure', record.coordinates)
        self.assertNotIn('Time', record.coordinates)
        self.assertNotIn('Latitude', record.coordinates)
        self.assertNotIn('Longitude', record.coordinates)

    def test_simple_parameters(self):
        record = self.decoded_records[0]
        test_missing = ['ChlorophyllA', 'ChlorophyllAFluorescence', 'ColoredDissolvedOrganicMatter', 'DissolvedOxygen',
                        'DissolvedOxygenFrequency', 'DissolvedOxygenMolar', 'Turbidity', 'ParticleBackscatter']
        tests = [
            ('PracticalSalinity', 24.9, 1, "0.001", None),
            ('Temperature', 11.1, 1, 'degrees_Celsius', {'TemperatureScale': 'ITS-90'}),
        ]
        for pcode, val, qf, units, metadata in tests:
            with self.subTest(pcode=pcode):
                self.assertIn(pcode, record.parameters)
                param = record.parameters[pcode]
                self.assertIsInstance(param, SingleElement)
                self.assertEqual(val, param.value)
                self.assertEqual(param.metadata['Quality'].value, qf)
                if units is not None:
                    self.assertEqual(param.metadata['Units'].value, units)
                if metadata:
                    for key in metadata:
                        self.assertIn(key, param.metadata)
                        self.assertEqual(param.metadata[key].value, metadata[key])
        for test in test_missing:
            with self.subTest(missing=test):
                self.assertNotIn(test, record.parameters)

    def test_simple_parameters_record2(self):
        record = self.decoded_records[1]
        test_missing = ['ChlorophyllA', 'ChlorophyllAFluorescence', 'ColoredDissolvedOrganicMatter', 'DissolvedOxygen',
                        'DissolvedOxygenFrequency', 'DissolvedOxygenMolar', 'Turbidity', 'ParticleBackscatter',
                        'PracticalSalinity', 'Temperature']
        for test in test_missing:
            with self.subTest(missing=test):
                self.assertNotIn(test, record.parameters)

    def test_simple_metadata(self):
        record = self.decoded_records[0]
        missing = ['DOI', 'Abstract', 'Area', 'Project', 'Program', 'Summary', 'Observatory', 'PrincipalInvestigator',
                   'DataUpdateInterval', 'EndDate', 'EndLatitude', 'EndLongitude', 'ReferenceStationNames',
                   'BatteryDescription', 'DocumentationVersion', 'PlatformCustomization', 'PlatformDetails',
                   'FirmwareVersion', 'Notes', 'BatteryType']
        tests = [
            ('DeploymentCruiseID', '12345', None),
            ('WMOID', '12345678', None),
            ('PlatformID', 'LD50', None),
            ('PlatformName', 'SEA000', None),
            ('CruiseID', 'SEA000_20150909', None),
            ('CNODCLevel', 'REAL_TIME', None),
            ('StartDate', '2015-09-09', {'Quality': 1}),
            ('StartLatitude', 44.12, {'Quality': 1}),
            ('StartLongitude', 128.12, {'Quality': 1}),
            ('PlatformSerial', '54321', None),
            ('PlatformCategory', 'glider_coastal', None),
            ('Operator', 'dfo', None),
            ('PlatformOwner', 'dfo', None),
            ('GliderPhaseCode', 2, {'GliderPhaseCodeSource': 'ego'}),
            ('GliderPhaseNumber', 0, None),
            ('PlatformModel', 'Slocum G2', None),
            ('PlatformMake', 'Teledyne Webb Research', None),
        ]
        for prop in missing:
            with self.subTest(missing=prop):
                self.assertNotIn(prop, record.metadata)
        for prop, value, prop_md in tests:
            with self.subTest(property=prop):
                self.assertIn(prop, record.metadata)
                if isinstance(value, (list, set, tuple)):
                    self.assertIsInstance(record.metadata[prop], MultiElement)
                    values = [x.value for x in record.metadata[prop].value]
                    for n in value:
                        self.assertIn(n, values)
                else:
                    self.assertIsInstance(record.metadata[prop], SingleElement)
                    self.assertEqual(record.metadata[prop].value, value)
                    if prop_md:
                        for subprop in prop_md:
                            self.assertIn(subprop, record.metadata[prop].metadata)
                            self.assertEqual(prop_md[subprop], record.metadata[prop].metadata[subprop].value)


