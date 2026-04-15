import pathlib
import typing as t

import netCDF4

from pipeman.programs.glider.ego_convert import OpenGliderConverter
from medsutil.sanitize import netcdf_string_to_bytes, netcdf_bytes_to_string
from tests.helpers.base_test_case import BaseTestCase, InjectableDict


class GliderBaseTest(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.old_file = cls.class_temp_dir / cls.filename()
        with netCDF4.Dataset(cls.old_file, 'w') as old:
            cls.run_converter = cls.setup_ego_file(old)
        cls.old_handle = netCDF4.Dataset(cls.old_file, 'r')
        cls.converter = OpenGliderConverter.build(None, cls.halt_flag)
        cls.version = 0
        cls.new_file: pathlib.Path = None
        cls.new_handle: netCDF4.Dataset = None
        cls.rebuild_new_file()
        if cls.run_converter is True:
            cls.converter._convert(cls.new_handle, cls.old_handle, cls.old_file.name)

    @classmethod
    def rebuild_new_file(cls):
        if cls.new_handle and cls.new_handle.isopen():
            cls.new_handle.close()
        if cls.new_file:
            cls.new_file.unlink(True)
        subdir = cls.class_temp_dir / str(cls.version)
        subdir.mkdir()
        cls.version += 1
        cls.new_file: pathlib.Path = subdir / cls.filename()
        cls.new_handle: netCDF4.Dataset = netCDF4.Dataset(cls.new_file, 'r+')

    @classmethod
    def filename(cls) -> str:
        return 'TEST001_20151001_R.nc'

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset) -> bool | None:
        return None

    def tearDown(self, d: InjectableDict = None):
        if self.run_converter is not True:
            self.rebuild_new_file()

    @classmethod
    def tearDownClass(cls):
        if cls.old_handle and cls.old_handle.isopen():
            cls.old_handle.close()
            del cls.old_handle
        if cls.new_handle and cls.new_handle.isopen():
            cls.new_handle.close()
            del cls.new_handle
        if cls.new_file:
            cls.new_file.unlink(True)
            del cls.new_file
        if cls.old_file:
            cls.old_file.unlink(True)
            del cls.old_file
        super().tearDownClass()

    @staticmethod
    def _add_old_ego_sensor_info(ds, param_info: t.Sequence[tuple[str, str, str]]):
        for param in param_info:
            v = ds.createVariable(param[0], 'f8', ('N_COUNT',))
            if param[1]:
                v.setncattr('sensor_name', param[1])
            if param[2]:
                v.setncattr('sensor_serial_number', param[2])

    @staticmethod
    def _add_new_ego_sensor_info(ds,
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

    def assertHasVariableAttribute(self, var_name: str, attr_name: str, value: t.Any):
        self.assertIn(var_name, self.new_handle.variables.keys(), msg=f'Missing variable {var_name}')
        self.assertTrue(hasattr(self.new_handle.variables[var_name], attr_name), msg=f'Variable {var_name} is missing attribute {attr_name}')
        self.assertEqual(value, self.new_handle.variables[var_name].getncattr(attr_name))

    def assertHasAttribute(self, name: str, value: t.Any):
        self.assertTrue(hasattr(self.new_handle, name), msg=f'Attribute {name} is missing')
        self.assertEqual(value, self.new_handle.getncattr(name), msg=f'Attribute {name} expected to be {value}, actually {self.new_handle.getncattr(name)}')

    def assertHasVariable(self, name: str):
        self.assertIn(name, self.new_handle.variables.keys(), msg=f'Variable {name} is missing')

    def assertDoesNotHaveVariable(self, name: str):
        self.assertNotIn(name, self.new_handle.variables.keys(), msg=f'Variable {name} is present, but shold not be')

    def assertDoesNotHaveAttribute(self, name: str):
        self.assertFalse(hasattr(self.new_handle, name), msg=f"Attribute {name} is present, but should not be")

    def assertStringVariableEqual(self, name: str, value: str):
        self.assertEqual(netcdf_bytes_to_string(self.new_handle.variables[name][:]), value)
