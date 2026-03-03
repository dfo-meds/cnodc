import json
import pathlib

from cnodc.ocproc2 import ParentRecord, MultiElement
from cnodc.programs.glider.ego_convert import ego_sensor_info
from cnodc.util.sanitize import netcdf_bytes_to_string, str_to_netcdf_vlen, str_to_netcdf
from core import BaseTestCase
import netCDF4 as nc
import typing as t
from cnodc.programs.glider.ego_decode import GliderEGOMapper


class TestGliderDecode(BaseTestCase):

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
            })
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

    def _add_old_ego_sensor_info(self, ds: nc.Dataset, param_info: t.Sequence[tuple[str, str, str]]):
        for param in param_info:
            v = ds.createVariable(param[0], 'f8', ('N_COUNT',))
            if param[1]:
                v.setncattr('sensor_name', param[1])
            if param[2]:
                v.setncattr('sensor_serial_number', param[2])

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
            sensor_info, param_map = ego_sensor_info(ds, {})
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

    def _add_new_ego_sensor_info(self,
                                 ds: nc.Dataset,
                                 sensor_info: t.Sequence[tuple[str, str, str, str, str, str]],
                                 parameter_info: t.Sequence[tuple[str, str]]):
            ds.createDimension('N_SENSORS', len(sensor_info))
            ds.createDimension('N_PARAMS', len(parameter_info))
            ds.createDimension('STRING256', 256)
            var_names = ['SENSOR', 'SENSOR_MAKER', 'SENSOR_MODEL', 'SENSOR_SERIAL_NO', 'SENSOR_MOUNT', 'SENSOR_ORIENTATION']
            for idx, var_name in enumerate(var_names):
                v = ds.createVariable(var_name, 'S1', ('N_SENSORS', 'STRING256',))
                v[:] = str_to_netcdf([x[idx] for x in sensor_info], 256)
            var_names = ['PARAMETER', 'PARAMETER_SENSOR']
            for idx, var_name in enumerate(var_names):
                v = ds.createVariable(var_name, 'S1', ('N_PARAMS', 'STRING256', ))
                v[:] = str_to_netcdf([x[idx] for x in parameter_info], 256)

    def _add_ego_parameter(self, ds: nc.Dataset, parameter_name, data, qc_data=None, units=None, qc_var_name=None):
        pres = ds.createVariable(parameter_name, 'f8', ('N_COUNT',))
        if units:
            pres.setncattr('units', units)
        pres[:] = data
        if qc_data:
            pres_qc = ds.createVariable(qc_var_name or f'{parameter_name}_QC', 'f8', ('N_COUNT',))
            pres_qc[:] = qc_data

    def _add_ego_variable_value(self, ds: nc.Dataset, var_name: str, value):
        if isinstance(value, str):
            v = ds.createVariable(var_name, 'S1', ('STRING256',))
            v[:] = str_to_netcdf(value, 256)
        elif isinstance(value, int):
            v = ds.createVariable(var_name, 'i4')
            v[:] = [value]
        elif isinstance(value, float):
            v = ds.createVariable(var_name, 'f8')
            v[:] = [value]

    def test_ego_decode_features(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            self._add_new_ego_sensor_info(ds, [
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
            self._add_ego_variable_value(ds, 'DEPLOYMENT_END_DATE', '20151009')
            self._add_ego_variable_value(ds, 'DEPLOYMENT_END_LATITUDE', 45.12)
            self._add_ego_variable_value(ds, 'DEPLOYMENT_END_LONGITUDE', 129.12)
            self._add_ego_variable_value(ds, 'DEPLOYMENT_START_DATE', '20150909')
            self._add_ego_variable_value(ds, 'DEPLOYMENT_START_LATITUDE', 44.12)
            self._add_ego_variable_value(ds, 'DEPLOYMENT_START_LONGITUDE', 128.12)
            self._add_ego_variable_value(ds, 'DEPLOYMENT_REFERENCE_STATION_ID', 'stuff')
            self._add_ego_variable_value(ds, 'BATTERY_PACKS', '4xlithiumion in parallel')
            self._add_ego_variable_value(ds, 'GLIDER_MANUAL_VERSION', 'v4.1.2')
            self._add_ego_variable_value(ds, 'CUSTOMIZATION', 'we built a portal to the underworld in the glider')
            self._add_ego_variable_value(ds, 'SPECIAL_FEATURES', 'underworld portal')
            self._add_ego_variable_value(ds, 'GLIDER_SERIAL_NO', '54321')
            self._add_ego_variable_value(ds, 'PLATFORM_FAMILY', 'COASTAL_GLIDER')
            self._add_ego_variable_value(ds, 'DEPLOYMENT_START_QC', 1)
            self._add_ego_variable_value(ds, 'DEPLOYMENT_END_QC', 5)
            self._add_ego_variable_value(ds, 'FIRMWARE_VERSION', 'v2.3.4')
            self._add_ego_variable_value(ds, 'BATTERY_TYPE', 'Lithium\x00ION')
            self._add_ego_variable_value(ds, 'ANOMALY', 'the glider accidently summoned a demon')
            self._add_ego_variable_value(ds, 'OPERATING_INSTITUTION', 'DFO;C-PROOF')
            self._add_ego_variable_value(ds, 'GLIDER_OWNER', 'NAFC')
            self._add_ego_variable_value(ds, 'PLATFORM_TYPE', 'SLOCUM_SG2')
            ds.createDimension('N_COUNT',)
            pq = ds.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
            pq[:] = [1, 1, 1]
            self._add_ego_parameter(ds, 'LATITUDE', [12, 24, 36], units='degree_north')
            self._add_ego_parameter(ds, 'LONGITUDE', [13, 25, 37], units='degreeE')
            self._add_ego_parameter(ds, 'LATITUDE_GPS', [12.1, 24.1, 36.1], [1, 1, 1], units='degree_north')
            self._add_ego_parameter(ds, 'LONGITUDE_GPS', [13.1, 25.1, 37.1], [1, 1, 1], units='degreeE')
            self._add_ego_parameter(ds, 'TIME_GPS', [5.01, 10.01, 15.01], [2, 2, 2], 'days since 1950-01-01T00:00:00')
            self._add_ego_parameter(ds, 'JULD', [5, 10, 15], [2, 2, 2], 'days since 1950-01-01T00:00:00', 'TIME_QC')
            self._add_ego_parameter(ds, 'PRES', [1.2, 2.4, 3.6], [1, 1, 1], 'decibar')
            self._add_ego_parameter(ds, 'CHLA', [1.3, 2.5, 3.7], [1, 1, 1], 'mg/m3')
            self._add_ego_parameter(ds, 'FLUORESCENCE_CHLA', [2, 3, 4], [1, 1, 1], '1')
            self._add_ego_parameter(ds, 'PSAL', [24.9, 25.1, 29.1], [1, 1, 1], 'psu')
            self._add_ego_parameter(ds, 'TEMP', [11.1, 22.2, 11.2], [1, 1, 1], 'degree_Celsius')
            self._add_ego_parameter(ds, 'CDOM', [0.5, 0.1, 0.7], [1, 1, 1], 'ppb')
            self._add_ego_parameter(ds, 'PHASE', [2, 3, 2])
            self._add_ego_parameter(ds, 'PHASE_NUMBER', [0, 0, 1])
            self._add_ego_parameter(ds, 'DOXY', [12.1, 13.1, 14.1], [1, 1, 1], 'micromol/kg')
            self._add_ego_parameter(ds, 'CNDC', [12.2, 13.3, 14.4], [1, 1, 1], 'mhos/m')
            self._add_ego_parameter(ds, 'MOLAR_DOXY', [5.1, 6.1, 7.1], [1, 1, 1], 'micromol/l')
            self._add_ego_parameter(ds, 'FREQUENCY_DOXY', [5.2, 6.2, 7.1], [1, 1, 1], 's-1')
            self._add_ego_parameter(ds, 'TURBIDITY', [2, 3, 4], [1, 1, 1], '1')
            self._add_ego_parameter(ds, 'BBP700', [6.1, 7.1, 8.1], [1, 1, 1], 'm-1')
            self._add_ego_parameter(ds, 'DPHASE_DOXY', [10, 11, 12], [1, 1, 1], 'degree')
            self._add_ego_parameter(ds, 'RPHASE_DOXY', [11, 12, 13], [1, 1, 1], 'degree')
            self._add_ego_parameter(ds, 'BPHASE_DOXY', [12, 13, 14], [1, 1, 1], 'degree')
            self._add_ego_parameter(ds, 'TEMP_DOXY', [13, 14, 15], [1, 1, 1], 'degree_Celsius')

            mapper = GliderEGOMapper(ds, GliderEGOMapper.DEFAULT_MAPPING_FILE)
            records = [x for x in mapper.build_records()]
            self.assertEqual(3, len(records))
            record = records[0]
            self.assertIsInstance(record, ParentRecord)
            with self.subTest(attr="pis"):
                self.assertIsInstance(record.metadata['PrincipalInvestigator'], MultiElement)
            # needs to be adjusted for the GPS values
            #with self.subTest(attr='latitude'):
            #    self.assertEqual(12, record.coordinates.best_value('Latitude'))
            #    self.assertEqual(1, record.coordinates['Latitude'].metadata.best_value('Quality'))
            #    self.assertEqual('interpolated', record.coordinates['Latitude'].metadata.best_value('SensorType'))
            #    self.assertEqual('degrees_north', record.coordinates['Latitude'].metadata.best_value('Units'))
            #with self.subTest(attr='longitude'):
            #    self.assertEqual(13, record.coordinates.best_value('Longitude'))
            #    self.assertEqual(1, record.coordinates['Longitude'].metadata.best_value('Quality'))
            #    self.assertEqual('interpolated', record.coordinates['Longitude'].metadata.best_value('SensorType'))
            #    self.assertEqual('degrees_east', record.coordinates['Longitude'].metadata.best_value('Units'))
            #with self.subTest(attr='time'):
            #    self.assertEqual('1950-01-06T00:00:00', record.coordinates.best_value('Time'))
            #    self.assertEqual(2, record.coordinates['Time'].metadata.best_value('Quality'))
            #    self.assertEqual('interpolated', record.coordinates['Time'].metadata.best_value('SensorType'))
            with self.subTest(attr='pressure'):
                self.assertEqual(1.2, record.coordinates.best('Pressure'))
                self.assertEqual(1, record.coordinates['Pressure'].metadata.best('Quality'))
                self.assertEqual('dbar', record.coordinates['Pressure'].metadata.best('Units'))
                self.assertEqual('ctd', record.coordinates['Pressure'].metadata.best('SensorType'))
                self.assertEqual('Company A', record.coordinates['Pressure'].metadata.best('SensorMake'))
                self.assertEqual('Model X', record.coordinates['Pressure'].metadata.best('SensorModel'))
                self.assertEqual('12345', record.coordinates['Pressure'].metadata.best('SensorSerial'))
            with self.subTest(attr="chla"):
                self.assertEqual(1.3, record.parameters.best('ChlorophyllA'))
                self.assertEqual(1, record.parameters['ChlorophyllA'].metadata.best('Quality'))
                self.assertEqual("mg m-3", record.parameters['ChlorophyllA'].metadata.best('Units'))
            with self.subTest(attr="fchla"):
                self.assertEqual(2, record.parameters.best('ChlorophyllAFluorescence'))
                self.assertEqual(1, record.parameters['ChlorophyllAFluorescence'].metadata.best('Quality'))
                self.assertEqual("1", record.parameters['ChlorophyllAFluorescence'].metadata.best('Units'))
            with self.subTest(attr="psal"):
                self.assertEqual(24.9, record.parameters.best('PracticalSalinity'))
                self.assertEqual(1, record.parameters['PracticalSalinity'].metadata.best('Quality'))
                self.assertEqual("0.001", record.parameters['PracticalSalinity'].metadata.best('Units'))
            with self.subTest(attr="temp"):
                self.assertEqual(11.1, record.parameters.best('Temperature'))
                self.assertEqual(1, record.parameters['Temperature'].metadata.best('Quality'))
                self.assertEqual("degrees_Celsius", record.parameters['Temperature'].metadata.best('Units'))
            with self.subTest(attr="doi"):
                self.assertEqual('10.x.x.x/abc', record.metadata.best('DOI'))
