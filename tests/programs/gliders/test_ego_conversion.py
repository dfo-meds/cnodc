import math
import pathlib

import netCDF4
import numpy
from zarr.core.dtype import dtype

from cnodc.programs.glider.ego_convert import OpenGliderConverter
from cnodc.util import unnumpy
from cnodc.util.sanitize import netcdf_bytes_to_string, str_to_netcdf, str_to_netcdf_vlen
from helpers.base_test_case import BaseTestCase, InjectableDict
import typing as t

class GliderConversionTestcase(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.old_file = self.temp_dir / 'ego.nc'
        self.new_file = self.temp_dir / 'og.nc'
        self.converter = OpenGliderConverter.build(None, self.halt_flag)
        with netCDF4.Dataset(self.old_file, 'w') as old:
            self.setup_ego_file(old)
        self.old_handle = netCDF4.Dataset(self.old_file, 'r')
        self.new_handle = netCDF4.Dataset(self.new_file, 'r+')

    def setup_ego_file(self, old: netCDF4.Dataset):
        pass

    def tearDown(self):
        self.old_handle.close()
        self.new_handle.close()
        super().tearDown()

    def _add_old_ego_sensor_info(self, ds, param_info: t.Sequence[tuple[str, str, str]]):
        for param in param_info:
            v = ds.createVariable(param[0], 'f8', ('N_COUNT',))
            if param[1]:
                v.setncattr('sensor_name', param[1])
            if param[2]:
                v.setncattr('sensor_serial_number', param[2])

    def _add_new_ego_sensor_info(self,
                                 ds,
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



class TestEmptyGliderConversion(GliderConversionTestcase):

    def test_can_make_dimensions(self):
        self.converter._create_dimensions(self.new_handle)
        self.assertIn('N_MEASUREMENTS', self.new_handle.dimensions)

    def test_can_set_static_dimensions(self):
        self.converter._map_static_metadata(self.new_handle)
        self.assertHasAttribute('Conventions', 'CF-1.0,ACDD-1.3,OG-1.0,CNODC-1.0')
        self.assertHasAttribute('featureType', 'trajectory')
        self.assertHasAttribute('naming_authority', 'ca.dfo-mpo.cnodc-cndoc')
        self.assertHasAttribute('platform_vocabulary', 'https://vocab.nerc.ac.uk/collection/L06/current/27/')
        self.assertHasAttribute('standard_name_vocab', 'CF 1.13')
        self.assertHasAttribute('platform', 'sub-surface gliders')
        self.assertHasAttribute('institution', 'DFO-MPO')
        self.assertHasAttribute('contributor_role_vocabulary', 'https://standards.iso.org/iso/19115/resources/Codelists/cat/codelists.xml#CI_RoleCode')
        self.assertHasAttribute('default_locale', 'en-CA')
        self.assertHasAttribute('locales', '_fr: fr-CA')
        self.assertHasAttribute('geospatial_bounds_crs', 'EPSG:4326')
        self.assertHasAttribute('geospatial_bounds_vertical_crs', 'EPSG:5831')
        self.assertHasAttribute('geospatial_vertical_positive', 'down')

    def test_can_set_filename_metadata(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'R')
        self.assertHasAttribute('rtqc_method', 'Coriolis MATLAB Toolbox')
        self.assertHasAttribute('summary', 'Real-time data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données en temps réel de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Real-Time)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (temps réel)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_R')

    def test_can_set_filename_metadata_preliminary(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'P')
        self.assertHasAttribute('rtqc_method', 'Coriolis MATLAB Toolbox')
        self.assertHasAttribute('summary', 'Preliminary data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données préliminaire de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Preliminary)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (préliminaire)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_P')

    def test_can_set_filename_metadata_adjusted(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'A')
        self.assertHasAttribute('summary', 'Adjusted data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données ajustées de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Adjusted)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (ajustées)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_A')

    def test_can_set_filename_metadata_delayed(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'D')
        self.assertHasAttribute('summary', 'Delayed-mode data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données en temps différé de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Delayed-Mode)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (différé)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_D')

    def test_can_set_filename_metadata_mixed(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'M')
        self.assertHasAttribute('summary', 'Mixed data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données mixte de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Mixed)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (mixte)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_M')

    def test_can_set_filename_metadata_bad(self):
        with self.assertRaisesCNODCError('GLIDER-2019'):
            self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'Z')

    def test_copy_metadata(self):
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._copy_metadata(self.new_handle, self.old_handle)
        self.assertDoesNotHaveAttribute('internal_mission_identifier')
        self.assertDoesNotHaveAttribute('program')
        self.assertDoesNotHaveAttribute('project')
        self.assertDoesNotHaveAttribute('comment')
        self.assertDoesNotHaveAttribute('network')

    def test_set_sensor_metadata(self):
        self.converter._set_sensor_metadata(self.new_handle, self.old_handle)
        self.assertFalse(any(v.startswith('SENSOR_') for v in self.new_handle.variables))

    def test_build_variables(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        for var_name in ('TIME', 'DEPTH', 'TIME_GPS', 'TIME_GPS_QC', 'LONGITUDE_GPS', 'LONGITUDE_GPS_QC',
                         'LATITUDE_GPS_QC', 'TRAJECTORY', 'DEPLOYMENT_TIME', 'WMO_IDENTIFIER', 'PLATFORM_MODEL',
                         'PLATFORM_SERIAL_NUMBER', 'PLATFORM_NAME', 'PLATFORM_MAKER', 'BATTERY_TYPE', 'TELECOM_TYPE',
                         'TRACKING_SYSTEM', 'PHASE', 'PHASE_QC'):
            with self.subTest(var_exists=var_name):
                self.assertHasVariable(var_name)
        for var_name, attr_name, expected_value in (
            ('TIME', 'long_name', 'time of measurement'),
            ('TIME', 'units', 'seconds since 1970-01-01T00:00:00+00:00'),
            ('TIME', 'standard_name', 'time'),
            ('TIME', 'calendar', 'gregorian'),
            ('TIME', 'axis', 'T'),
            ('TIME', 'ancillary_variables', 'TIME_QC'),
            ('TIME', 'missing_value', -1.0),
            ('TIME', 'cnodc_standard_name', 'Time'),
            ('TIME', 'valid_min', 1000000000.0),
            ('TIME', 'valid_max', 4000000000.0),
            ('TIME', 'long_name_fr', 'moment de la mesure'),
        ):
            with self.subTest(var_name=var_name, attr_name=attr_name):
                self.assertHasVariableAttribute(var_name, attr_name, expected_value)
        for var_name in ('TIME_QC', 'LATITUDE', 'LATITUDE_QC', 'LONGITUDE', 'LONGITUDE_QC', 'DEPLOYMENT_LATITUDE',
                         'DEPLOYMENT_LONGITUDE',):
            with self.subTest(var_missing=var_name):
                self.assertDoesNotHaveVariable(var_name)

    def test_build_parameters(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_parameters(self.new_handle, self.old_handle, {})
        for var_name in ('PRES', 'PSAL', 'TEMP', 'CNDC', 'MOLDOXY', 'FREQDOXY', 'DOXY', 'FLUOCHLA', 'CHLA', 'TURB',
                         'BBP700', 'CDOM', 'DPHDOXY', 'RPHDOXY', 'BPHDOXY', 'TEMPDOXY'):
            with self.subTest(var_missing=var_name):
                self.assertDoesNotHaveVariable(var_name)
                self.assertDoesNotHaveVariable(var_name + "_QC")

    def test_build_contact_info(self):
        self.assertEqual((
            'Erin Turnbull',
            '0009-0004-9696-0758',
            'erin.turnbull@dfo-mpo.gc.ca',
            'contributor'
        ), self.converter._build_contact_info('ERIN TURNBULL', 'contributor'))

    def test_build_no_contact_info(self):
        self.assertEqual((
            'John William Turnbull',
            '',
            '',
            'editor'
        ), self.converter._build_contact_info('John William Turnbull', 'editor'))

    def test_build_phase_info(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_phase_info(self.new_handle, self.old_handle)

    def test_cannot_build_contributors(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'R')
        with self.assertRaisesCNODCError('GLIDER-2012'):
            self.converter._build_contributors(self.new_handle, self.old_handle)

    def test_cannot_build_deployment_info(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        with self.assertRaisesCNODCError('GLIDER-2014'):
            self.converter._build_deployment_info(self.new_handle, self.old_handle, 'TEST001', '20150102030405')

    def test_cannot_build_glider_info(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        with self.assertRaisesCNODCError('GLIDER-2009'):
            self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_cannot_build_bounds(self):
        with self.assertRaisesCNODCError('GLIDER-2000'):
            self.converter._set_geospatial_bounds_metadata(self.new_handle, self.old_handle)

    def test_cannot_build_depths(self):
        with self.assertRaisesCNODCError('GLIDER-2020'):
            self.converter._build_depths(self.new_handle, self.old_handle)

    def test_cannot_build_times(self):
        with self.assertRaisesCNODCError('GLIDER-2005'):
            self.converter._build_times(self.new_handle, self.old_handle)


class TestPartialBadConversion(GliderConversionTestcase):

    def setup_ego_file(self, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('JULD', 'f8', ('N_COUNT',))
        old.createVariable('DEPLOYMENT_START_DATE', str, ())
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')
        old.createVariable('PHASE', 'i2', ('N_COUNT',))

    def test_cannot_build_bounds(self):
        with self.assertRaisesCNODCError('GLIDER-2001'):
            self.converter._set_geospatial_bounds_metadata(self.new_handle, self.old_handle)

    def test_cannot_build_depths(self):
        with self.assertRaisesCNODCError('GLIDER-2002'):
            self.converter._build_depths(self.new_handle, self.old_handle)

    def test_cannot_build_time(self):
        with self.assertRaisesCNODCError('GLIDER-2006'):
            self.converter._build_times(self.new_handle, self.old_handle)

    def test_cannot_build_contributors(self):
        with self.assertRaisesCNODCError('GLIDER-2013'):
            self.converter._build_contributors(self.new_handle, self.old_handle)

    def test_cannot_build_deployment_info(self):
        with self.assertRaisesCNODCError('GLIDER-2008'):
            self.converter._build_deployment_info(self.new_handle, self.old_handle, 'TEST001', '20150102030405')

    def test_cannot_build_glider_info(self):
        with self.assertRaisesCNODCError('GLIDER-2010'):
            self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')



class TestPartial2BadConversion(GliderConversionTestcase):

    def setup_ego_file(self, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'bad units epoch'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = str_to_netcdf_vlen('20150102')
        old.createVariable('PLATFORM_TYPE', str, ())
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')

    def test_cannot_build_glider_info(self):
        with self.assertRaisesCNODCError('GLIDER-2015'):
            self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_cannot_build_time(self):
        with self.assertRaisesCNODCError('GLIDER-2006'):
            self.converter._build_times(self.new_handle, self.old_handle)

    def test_cannot_build_depths(self):
        with self.assertRaisesCNODCError('GLIDER-2003'):
            self.converter._build_depths(self.new_handle, self.old_handle)

    def test_can_build_blank_deployment_info(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        self.converter._build_deployment_info(self.new_handle, self.old_handle, 'TEST001', '20150102030405')
        self.assertHasAttribute('start_date', '2015-01-02T00:00:00+00:00')
        self.assertEqual(self.new_handle.variables['DEPLOYMENT_TIME'][:].item(), 1420156800.0)
        self.assertStringVariableEqual('TRAJECTORY', 'TEST001_20150102030405')

    def test_can_build_blank_bounds(self):
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._set_geospatial_bounds_metadata(self.new_handle, self.old_handle)
        self.assertDoesNotHaveAttribute('geospatial_lat_min')
        self.assertDoesNotHaveAttribute('geospatial_lat_max')
        self.assertDoesNotHaveAttribute('geospatial_lon_min')
        self.assertDoesNotHaveAttribute('geospatial_lon_max')

    def test_can_build_blank_contributors(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '201501020304', 'R')
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_contributors(self.new_handle, self.old_handle)


class TestPartial3BadConversion(GliderConversionTestcase):

    def setup_ego_file(self, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = str_to_netcdf_vlen('20150102')
        old.createVariable('PLATFORM_TYPE', str, ())
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('GLIDER_SERIAL_NO', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')

    def test_can_build_blank_times(self):
        self.converter._create_dimensions(self.new_handle)
        self.converter._build_variables(self.new_handle, self.old_handle)
        self.converter._build_times(self.new_handle, self.old_handle)
        self.assertDoesNotHaveAttribute('time_coverage_start')
        self.assertDoesNotHaveAttribute('time_coverage_end')

    def test_cannot_build_glider_info(self):
        with self.assertRaisesCNODCError('GLIDER-2011'):
            self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_cannot_build_depths(self):
        with self.assertRaisesCNODCError('GLIDER-2004'):
            self.converter._build_depths(self.new_handle, self.old_handle)


class TestMinimalEmptyConversion(GliderConversionTestcase):

    def setup_ego_file(self, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = str_to_netcdf_vlen('20150102')
        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = str_to_netcdf_vlen('SLOCUM_G2')
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('GLIDER_SERIAL_NO', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')

    def test_can_build_blank_glider_info(self):
        self.converter._create_dimensions(self.new_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_variables(self.new_handle, self.old_handle)
            self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')
        self.assertStringVariableEqual('PLATFORM_NAME', 'TEST001')
        self.assertStringVariableEqual('WMO_IDENTIFIER', '12345')
        self.assertStringVariableEqual('PLATFORM_MODEL', 'Teledyne Webb Research Slocum G2 glider')
        self.assertStringVariableEqual('PLATFORM_SERIAL_NUMBER', 'unit_')
        self.assertStringVariableEqual('PLATFORM_MAKER', 'Teledyne Webb Research')

    def test_can_build_blank_depths(self):
        self.converter._create_dimensions(self.new_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_variables(self.new_handle, self.old_handle)
        self.converter._build_depths(self.new_handle, self.old_handle)
        self.assertDoesNotHaveAttribute('geospatial_vertical_min')
        self.assertDoesNotHaveAttribute('geospatial_vertical_max')

class TestMinimalConversion(GliderConversionTestcase):

    def setup_ego_file(self, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createDimension('N_TRANS', 1)
        old.createDimension('N_POS', 3)

        lats = old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        lats.setncattr('units', 'degrees_north')
        lats[:] = [45.1, 46.1, 47.1]

        longs = old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        longs.setncattr('units', 'degrees_east')
        longs[:] = [-126.1, -134.1, -119.2]

        pres = old.createVariable('PRES', 'f8', ('N_COUNT',))
        pres.setncattr('units', 'dbar')
        pres[:] = [1234, 1235, 1236]

        pres_qc = old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        pres_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5])
        pres_qc.setncattr('flag_meanings', '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')
        pres_qc[:] = [1, 1, 4]

        temp = old.createVariable('TEMP', 'f8', 'N_COUNT',)
        temp.setncattr('units', 'degrees_Celsius')
        temp[:] = [math.nan, math.nan, math.nan]

        temp_qc = old.createVariable('TEMP_QC', 'i2', ('N_COUNT',))
        temp_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5, 9])
        temp_qc.setncattr('flag_meanings', '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')
        temp_qc[:] = [9, 9, 9]

        pos_qc = old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        pos_qc[:] = [1, 1, 1]
        pos_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5])
        pos_qc.setncattr('flag_meanings', '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')

        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 1950-01-01T00:00:00+00:00'
        juld[:] = [27830.1, 27830.2, math.nan]

        time_qc = old.createVariable('TIME_QC', 'i2', ('N_COUNT',))
        time_qc[:] = [1, 1, 1]
        time_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5])
        time_qc.setncattr('flag_meanings', '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')

        dsl = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dsl[:] = [44.9]

        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        dslon[:] = [-125.9]

        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = str_to_netcdf_vlen('SLOCUM_G2')

        oi = old.createVariable('OPERATING_INSTITUTION', str, ())
        oi[:] = str_to_netcdf_vlen('C-PROOF')

        gsn = old.createVariable('GLIDER_SERIAL_NO', str, ())
        gsn[:] = str_to_netcdf_vlen('123456')

        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = str_to_netcdf_vlen('20150102')

        phases = old.createVariable('PHASE', 'i2', ('N_COUNT',))
        phases[:] = [1, 2, 2]

        bt = old.createVariable('BATTERY_TYPE', str, ())
        bt[:] = str_to_netcdf_vlen('LithiumION')


        ts = old.createVariable('TRANS_SYSTEM', str, ('N_TRANS',))
        ts[:] = str_to_netcdf_vlen(['iridium'])

        ps = old.createVariable('POSITIONING_SYSTEM', str, ('N_POS',))
        ps[:] = str_to_netcdf_vlen(['gps', 'argos', 'iridium'])


        go = old.createVariable('GLIDER_OWNER', str, ())
        go[:] = str_to_netcdf_vlen('BIO;NAFC;DFO')

        self._add_new_ego_sensor_info(old, [
                ('CTD_PRES', 'Company A', 'Model X', '12345' ,'', '')
            ], [
                ('PRES', 'CTD_PRES')
            ])

        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')
        old.setncattr('deployment_code', 'My Mission')
        old.setncattr('program', 'Program')
        old.setncattr('project', 'Project')
        old.setncattr('network', 'Network')

    def setUp(self):
        super().setUp()
        self.converter._convert(self.new_handle, self.old_handle, 'TEST001_20150102030405_R.nc')

    def test_dimensions(self):
        self.assertIn('N_MEASUREMENTS', self.new_handle.dimensions)

    def test_static_metadata(self):
        self.assertHasAttribute('institution', 'DFO-MPO')

    def test_copy_metadata(self):
        self.assertHasAttribute('internal_mission_identifier', 'My Mission')
        self.assertHasAttribute('program', 'Program')
        self.assertHasAttribute('project', 'Project')
        self.assertHasAttribute('network', 'Network')

    def test_set_geospatial_latlon_metadata(self):
        self.assertHasAttribute('geospatial_lat_min', 45.1)
        self.assertHasAttribute('geospatial_lat_max', 47.1)
        self.assertHasAttribute('geospatial_lon_min', -134.1)
        self.assertHasAttribute('geospatial_lon_max', -119.2)

    def test_has_sensor(self):
        self.assertHasVariable('SENSOR_CTD_12345')
        self.assertHasVariableAttribute('SENSOR_CTD_12345', 'sensor_serial_number', '12345')
        self.assertHasVariableAttribute('SENSOR_CTD_12345', 'sensor_maker', 'Company A')
        self.assertHasVariableAttribute('SENSOR_CTD_12345', 'sensor_model', 'Model X')
        self.assertHasVariableAttribute('SENSOR_CTD_12345', 'long_name', 'Company A Model X')

    def test_copied_var_attributes(self):
        self.assertHasVariable('TIME_QC')
        self.assertHasVariableAttribute('TIME_QC', 'flag_meanings', '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')

    def test_made_parameters(self):
        self.assertHasVariable('PRES')
        self.assertHasVariable('PRES_QC')
        self.assertHasVariableAttribute('PRES', 'coordinates', 'TIME,LONGITUDE,LATITUDE,DEPTH')
        self.assertHasVariableAttribute('PRES_QC', 'coordinates', 'TIME,LONGITUDE,LATITUDE,DEPTH')
        self.assertDoesNotHaveVariable('TEMP')
        self.assertDoesNotHaveVariable('TEMP_QC')
        self.assertDoesNotHaveVariable('CNDC')
        self.assertDoesNotHaveVariable('CNDC_QC')

    def test_depths(self):
        self.assertHasVariable('DEPTH')
        depths = unnumpy(self.new_handle.variables['DEPTH'][:])
        self.assertAlmostEqual(depths[0], 1220.3517408, 5)
        self.assertAlmostEqual(depths[1], 1221.2250968, 5)
        self.assertIsNone(depths[2])
        self.assertAlmostEqual(self.new_handle.getncattr('geospatial_vertical_min'), 1220.3517408, 5)
        self.assertAlmostEqual(self.new_handle.getncattr('geospatial_vertical_max'), 1221.2250968, 5)

    def test_times(self):
        self.assertHasVariable('TIME')
        times = unnumpy(self.new_handle.variables['TIME'][:])
        self.assertEqual(times[0], 1773368640.0)
        self.assertAlmostEqual(times[1], 1773377280.0)
        self.assertIsNone(times[2])
        self.assertHasAttribute('time_coverage_start', '20260313T022400+00:00')
        self.assertHasAttribute('time_coverage_end', '20260313T022400+00:00')
