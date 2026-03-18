import math
import pathlib

import netCDF4
import numpy

from cnodc.programs.glider.ego_convert import OpenGliderConverter
from cnodc.util.sanitize import netcdf_bytes_to_string
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

    def assertHasVariableAttribute(self, var_name: str, attr_name: str, value: t.Any):
        self.assertIn(var_name, self.new_handle.variables.keys(), msg=f'Missing variable {var_name}')
        self.assertTrue(hasattr(self.new_handle.variables[var_name], attr_name), msg=f'Variable {var_name} is missing attribute {attr_name}')
        self.assertEqual(self.new_handle.variables[var_name].getncattr(attr_name), value)

    def assertHasAttribute(self, name: str, value: t.Any):
        self.assertTrue(hasattr(self.new_handle, name), msg=f'Attribute {name} is missing')
        self.assertEqual(value, self.new_handle.getncattr(name), msg=f'Attribute {name} expected to be {value}, actually {self.new_handle.getncattr(name)}')

    def assertHasVariable(self, name: str):
        self.assertIn(name, self.new_handle.variables.keys(), msg=f'Variable {name} is missing')

    def assertDoesNotHaveVariable(self, name: str):
        self.assertNotIn(name, self.new_handle.variables.keys(), msg=f'Variable {name} is present, but shold not be')

    def assertDoesNotHaveAttribute(self, name: str):
        self.assertFalse(hasattr(self.new_handle, name), msg=f"Attribute {name} is present, but should not be")



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
        self.assertHasAttribute('rtqc_method', 'Real-time QC performed with Coriolis matlab toolbox')
        self.assertHasAttribute('rtqc_method_fr', 'Contrôle qualité en temps réel réalisé avec la boîte à outils Coriolis Matlab')
        self.assertHasAttribute('summary', 'Real-time data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données en temps réel de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Real Time)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (temps réel)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_R')

    def test_can_set_filename_metadata_preliminary(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'P')
        self.assertHasAttribute('rtqc_method', 'No QC applied')
        self.assertHasAttribute('rtqc_method_fr', 'Aucun contrôle qualité appliqué')
        self.assertHasAttribute('summary', 'Preliminary data from glider mission TEST001_20150102030405')
        self.assertHasAttribute('summary_fr', 'Données préliminaire de la mission du planeur TEST001_20150102030405')
        self.assertHasAttribute('title', 'Glider TEST001 - 20150102030405 (Preliminary)')
        self.assertHasAttribute('title_fr', 'Planeur TEST001 - 20150102030405 (préliminaire)')
        self.assertHasAttribute('id', 'TEST001_20150102030405_P')

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

    def test_build_contributors(self):
        self.converter._set_metadata_from_file_name(self.new_handle, 'TEST001', '20150102030405', 'R')
        self.converter._build_contributors(self.new_handle, self.old_handle)
        for attr in ('infoUrl', 'contributor_name', 'contributor_email', 'contributor_id', 'contributor_id_vocabulary',
                     'contributor_role', 'contributor_role_vocabulary', 'contributing_institutions',
                     'contributing_institutions_id', 'contributing_institutions_id_vocabulary',
                     'contributing_institutions_role' ,'contributing_institutions_role_vocabulary'):
            with self.subTest(missing_attr=attr):
                self.assertDoesNotHaveAttribute(attr)

    def test_build_deployment_info(self):
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._create_dimensions(self.new_handle)
            self.converter._build_variables(self.new_handle, self.old_handle)
            self.converter._build_deployment_info(self.new_handle, self.old_handle, 'TEST001', '20150102030405')
            self.assertDoesNotHaveAttribute('start_date')
            self.assertEqual(self.new_handle.variables['DEPLOYMENT_TIME'][:].item(), 0.0)
            self.assertEqual(netcdf_bytes_to_string(self.new_handle.variables['TRAJECTORY'][:]), 'TEST001_20150102030405')

    def test_cannot_set_geospatial_bounds(self):
        with self.assertRaisesCNODCError('GLIDER-2000'):
            self.converter._set_geospatial_bounds_metadata(self.new_handle, self.old_handle)

    def test_cannot_build_depths(self):
        with self.assertRaisesCNODCError('GLIDER-2002'):
            self.converter._build_depths(self.new_handle, self.old_handle)

    def test_cannot_build_times(self):
        with self.assertRaisesCNODCError('GLIDER-2005'):
            self.converter._build_times(self.new_handle, self.old_handle)
"""
class TestGoodGliderConversion(BaseTestCase):
    def test_convert(self):
        old_file = self.temp_dir / 'ego.nc'
        new_file = self.temp_dir / 'og.nc'
        with netCDF4.Dataset(old_file, 'w') as old:
            old.createDimension('N_COUNT', )
            old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
            old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
            old.createVariable('PRES', 'f8', ('N_COUNT',))
            old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
            old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
            juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        converter = OpenGliderConverter.build(None, self.halt_flag)
        converter.convert(old_file, new_file, 'TEST_201501030405_R.nc')



"""