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

