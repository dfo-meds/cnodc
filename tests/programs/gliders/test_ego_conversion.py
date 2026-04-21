import datetime
import math

import netCDF4

from pipeman.programs.glider.ego_convert import validate_ego_glider_file, ContactInfo
from medsutil import json
from medsutil.exceptions import CodedError
from medsutil.sanitize import netcdf_string_to_vlen_bytes, unnumpy

from tests.programs.gliders.helpers import GliderBaseTest


class TestEmptyGliderConversion(GliderBaseTest):

    def test_get_info_url(self):
        tests = (
            ('', '', 'C-PROOF', 'https://cproof.uvic.ca/'),
            ('', '', {'und': 'C-PROOF'}, 'https://cproof.uvic.ca/'),
            ('', '', {'en': 'C-PROOF'}, 'https://cproof.uvic.ca/'),
            ('', '', {'fr': 'C-PROOF'}, 'https://cproof.uvic.ca/'),
            ('', '', 'CEOTR', 'https://ceotr.ocean.dal.ca/gliders/'),
            ('', '', 'MEMORIAL', 'https://www.mun.ca/creait/autonomous-ocean-systems-centre/gliders--small-auvs/'),
            ('c-proof > stuff', '', '', 'https://cproof.uvic.ca/'),
            ('other > CEOTR > stuff', '', '', 'https://ceotr.ocean.dal.ca/gliders/'),
            ('', 'hal_1002_19900102', '',  'https://cproof.uvic.ca/'),
            ('', 'sunfish_19900102', '',  'https://www.mun.ca/creait/autonomous-ocean-systems-centre/gliders--small-auvs/'),
        )
        for network, glider_name, contact, result in tests:
            with self.subTest(network=network, glider_name=glider_name, contact=contact):
                self.assertEqual(self.converter._get_info_url(network, glider_name, [ContactInfo(proper_name=contact)]), result)


    def test_can_make_dimensions(self):
        self.converter._create_dimensions(self.new_handle)
        self.assertIn('N_MEASUREMENTS', self.new_handle.dimensions)

    def test_can_set_static_dimensions(self):
        self.converter._map_static_metadata(self.new_handle)
        self.assertHasAttribute('Conventions', 'CF-1.0,ACDD-1.3,OG-1.0,CNODC-1.0')
        self.assertHasAttribute('featureType', 'trajectory')
        self.assertHasAttribute('naming_authority', 'ca.dfo-mpo.cnodc-cndoc')
        self.assertHasAttribute('platform_vocabulary', 'https://vocab.nerc.ac.uk/collection/L06/current/27/')
        self.assertHasAttribute('standard_name_vocabulary', 'CF 1.13')
        self.assertHasAttribute('platform', 'sub-surface gliders')
        self.assertHasAttribute('institution', 'DFO-MPO')
        self.assertHasAttribute('contributor_role_vocabulary', 'https://standards.iso.org/iso/19115/resources/Codelists/cat/codelists.xml#CI_RoleCode')
        self.assertHasAttribute('locale_default', 'en-CA')
        self.assertHasAttribute('locale_others', '_fr: fr-CA')
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
        self.assertEqual(ContactInfo(
            proper_name='Erin Turnbull',
            short_name='Erin Turnbull',
            key_name='erinturnbull',
            research_id='0009-0004-9696-0758',
            research_id_type='https://orcid.org/',
            guid='0009-0004-9696-0758',
            email='erin.turnbull@dfo-mpo.gc.ca',
            role='contributor',
            contact_type='individual',
        ), self.converter._build_contact_info('ERIN TURNBULL', 'contributor'))

    def test_build_no_contact_info(self):
        self.assertEqual(ContactInfo(
            proper_name='John William Turnbull',
            short_name='John William Turnbull',
            role='editor',
            guid='johnwilliamturnbull',
            key_name='johnwilliamturnbull',
            contact_type='individual',
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

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)

    def test_bad_data_mode(self):
        with self.assertRaises(ValueError):
            self.converter._validate_file_name('TEST001_0102030405_Z.nc')

    def test_bad_mission_time(self):
        with self.assertRaises(ValueError):
            self.converter._validate_file_name('TEST001_201501_R.nc')

    def test_good_mission_time_ymd(self):
        self.converter._validate_file_name('TEST001_20150102_R.nc')

    def test_good_mission_time_ymdh(self):
        self.converter._validate_file_name('TEST001_2015010203_R.nc')

    def test_good_mission_time_ymdhm(self):
        self.converter._validate_file_name('TEST001_201501020304_R.nc')

    def test_good_mission_time_ymdhms(self):
        self.converter._validate_file_name('TEST001_20150102030405_R.nc')

    def test_parse_bad_filename(self):
        with self.assertRaisesCNODCError('GLIDER-2021'):
            self.converter._parse_file_name('MISSING_UNDERSCORE.nc')



class TestPartialBadConversion(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
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

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)


class TestPartial2BadConversion(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'bad units epoch'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')
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

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)


class TestPartial3BadConversion(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')
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

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)


class TestMinimalEmptyConversion(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dslat = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')
        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')
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

    def test_is_valid(self):
        self.assertTrue(validate_ego_glider_file(self.old_file, {}, self.old_file.name))



class TestBadPosSystem(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createDimension('N_POS', 2)
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        dslat = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')
        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('GLIDER_SERIAL_NO', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')
        ps = old.createVariable('POSITIONING_SYSTEM', str, ('N_POS',))
        ps[:] = netcdf_string_to_vlen_bytes(['', 'foobar'])

    def test_bad_positioning_system(self):
        self.converter._create_dimensions(self.new_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_variables(self.new_handle, self.old_handle)
            with self.assertRaisesCNODCError('GLIDER-2018'):
                self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)


class TestBadTrackSystem(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createDimension('N_TRANS', 2)
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        dslat = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102030405')
        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('GLIDER_SERIAL_NO', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')
        ps = old.createVariable('TRANS_SYSTEM', str, ('N_TRANS',))
        ps[:] = netcdf_string_to_vlen_bytes(['', 'foobar'])

    def test_bad_telecom_system(self):
        self.converter._create_dimensions(self.new_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_variables(self.new_handle, self.old_handle)
            with self.assertRaisesCNODCError('GLIDER-2017'):
                self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_deployment_info(self):
        self.assertSameTime(
            self.converter._validate_deployment_info(self.old_handle),
            datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
        )

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)




class TestBadBatterySystem(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
        old.createDimension('N_COUNT', )
        old.createDimension('N_TRANS', 1)
        old.createVariable('LATITUDE', 'f8', ('N_COUNT',))
        old.createVariable('LONGITUDE', 'f8', ('N_COUNT',))
        old.createVariable('PRES', 'f8', ('N_COUNT',))
        old.createVariable('PRES_QC', 'i2', ('N_COUNT',))
        old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        dslat = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 2015-01-02T03:04:05+00:00'
        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('201501020304')
        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')
        old.createVariable('OPERATING_INSTITUTION', str, ())
        old.createVariable('GLIDER_SERIAL_NO', str, ())
        old.createVariable('PHASE', 'i2', ('N_COUNT',))
        old.setncattr('principal_investigator', 'Erin Turnbull')
        old.setncattr('comment', 'wtf')
        old.setncattr('wmo_platform_code', '12345')
        ps = old.createVariable('BATTERY_TYPE', str, ())
        ps[:] = netcdf_string_to_vlen_bytes('foobar')

    def test_bad_battery_type(self):
        self.converter._create_dimensions(self.new_handle)
        with self.assertLogs('cnodc.gliders.ego_convert', 'WARNING'):
            self.converter._build_variables(self.new_handle, self.old_handle)
            with self.assertRaisesCNODCError('GLIDER-2016'):
                self.converter._build_glider_info(self.new_handle, self.old_handle, 'TEST001')

    def test_deployment_info(self):
        self.assertSameTime(
            self.converter._validate_deployment_info(self.old_handle),
            datetime.datetime(2015, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)
        )

    def test_is_invalid(self):
        with self.assertRaises(CodedError):
            validate_ego_glider_file(self.old_file, {}, self.old_file.name)



class TestMinimalConversion(GliderBaseTest):

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
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
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')

        oi = old.createVariable('OPERATING_INSTITUTION', str, ())
        oi[:] = netcdf_string_to_vlen_bytes('C-PROOF')

        gsn = old.createVariable('GLIDER_SERIAL_NO', str, ())
        gsn[:] = netcdf_string_to_vlen_bytes('123456')

        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')

        phases = old.createVariable('PHASE', 'i2', ('N_COUNT',))
        phases.setncattr('missing_value', -1)
        phases[:] = [1, 2, -1]

        bt = old.createVariable('BATTERY_TYPE', str, ())
        bt[:] = netcdf_string_to_vlen_bytes('LithiumION')


        ts = old.createVariable('TRANS_SYSTEM', str, ('N_TRANS',))
        ts[:] = netcdf_string_to_vlen_bytes(['iridium'])

        ps = old.createVariable('POSITIONING_SYSTEM', str, ('N_POS',))
        ps[:] = netcdf_string_to_vlen_bytes(['gps', 'argos', 'iridium'])


        go = old.createVariable('GLIDER_OWNER', str, ())
        go[:] = netcdf_string_to_vlen_bytes('BIO;NAFC;DFO')

        cls._add_new_ego_sensor_info(old, [
                ('CTD_PRES', 'Company A', 'Model X', '12345' ,'', ''),
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
        return True

    def test_is_valid(self):
        self.assertTrue(validate_ego_glider_file(self.old_file, {}, self.old_file.name))

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

    def test_time_conversion(self):
        self.assertHasVariable('TIME')
        times = unnumpy(self.new_handle.variables['TIME'][:])
        self.assertEqual(times[0], 1773368640.0)
        self.assertAlmostEqual(times[1], 1773377280.0)
        self.assertIsNone(times[2])
        self.assertHasAttribute('time_coverage_start', '20260313T022400+00:00')
        self.assertHasAttribute('time_coverage_end', '20260313T022400+00:00')

    def test_contributor_info(self):
        self.assertHasAttribute('infoUrl', 'https://cproof.uvic.ca/')
        self.assertHasAttribute('contributor_name', 'Erin Turnbull')
        self.assertHasAttribute('contributor_email', 'erin.turnbull@dfo-mpo.gc.ca')
        self.assertHasAttribute('contributor_id', '0009-0004-9696-0758')
        self.assertHasAttribute('contributor_id_vocabulary', 'https://orcid.org/')
        self.assertHasAttribute('contributor_role', 'CONT0004')
        self.assertHasAttribute('contributor_role_vocabulary', 'https://vocab.nerc.ac.uk/collection/W08/current/')
        self.assertHasAttribute('contributing_institutions', 'C-PROOF,BIO,NAFC,DFO-MPO')
        self.assertHasAttribute('contributing_institutions_id', '03c62s410,03bz9t645,,02qa1x782')
        self.assertHasAttribute('contributing_institutions_id_vocabulary', 'https://ror.org/')
        self.assertHasAttribute('contributing_institutions_role', 'CONT0003,CONT0002,CONT0002,CONT0002')
        self.assertHasAttribute('contributing_institutions_role_vocabulary', 'https://vocab.nerc.ac.uk/collection/W08/current/')

    def test_deployment_info(self):
        self.assertHasAttribute('start_date', '2015-01-02T00:00:00+00:00')
        self.assertStringVariableEqual('TRAJECTORY', 'TEST001_20151001')
        self.assertEqual(self.new_handle.variables['DEPLOYMENT_TIME'][:].item(), 1420156800.0)

    def test_glider_info(self):
        self.assertStringVariableEqual('PLATFORM_NAME', 'TEST001')
        self.assertStringVariableEqual('WMO_IDENTIFIER', '12345')
        self.assertStringVariableEqual('PLATFORM_MODEL', 'Teledyne Webb Research Slocum G2 glider')
        self.assertStringVariableEqual('PLATFORM_SERIAL_NUMBER', 'unit_123456')
        self.assertStringVariableEqual('PLATFORM_MAKER', 'Teledyne Webb Research')
        self.assertStringVariableEqual('BATTERY_TYPE', 'lithium')
        self.assertStringVariableEqual('TELECOM_TYPE', 'iridium')
        self.assertStringVariableEqual('TRACKING_SYSTEM', 'argos,gps,iridium')

    def test_phase_codes(self):
        phase = unnumpy(self.new_handle.variables['PHASE'][:])
        phase_qc = unnumpy(self.new_handle.variables['PHASE_QC'][:])
        self.assertEqual(phase, [2, 4, None])
        self.assertEqual(phase_qc, [0, 0, None])


class TestFullConversionWithMetadata(GliderBaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.old_handle.close()
        cls.new_file = cls.class_temp_dir / 'og.nc'
        cls.converter.convert(cls.old_file, cls.new_file)
        cls.metadata = cls.converter.build_metadata(cls.new_file, cls.old_file.name)
        cls.new_handle = netCDF4.Dataset(cls.new_file, 'r')

    @classmethod
    def tearDownClass(cls):
        if cls.new_handle.isopen():
            cls.new_handle.close()
        super().tearDownClass()

    @classmethod
    def setup_ego_file(cls, old: netCDF4.Dataset):
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
        pres_qc.setncattr('flag_meanings',
                          '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')
        pres_qc[:] = [1, 1, 4]

        temp = old.createVariable('TEMP', 'f8', 'N_COUNT', )
        temp.setncattr('units', 'degrees_Celsius')
        temp[:] = [math.nan, math.nan, math.nan]

        temp_qc = old.createVariable('TEMP_QC', 'i2', ('N_COUNT',))
        temp_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5, 9])
        temp_qc.setncattr('flag_meanings',
                          '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')
        temp_qc[:] = [9, 9, 9]

        pos_qc = old.createVariable('POSITION_QC', 'i2', ('N_COUNT',))
        pos_qc[:] = [1, 1, 1]
        pos_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5])
        pos_qc.setncattr('flag_meanings',
                         '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')

        juld = old.createVariable('JULD', 'f8', ('N_COUNT',))
        juld.units = 'days since 1950-01-01T00:00:00+00:00'
        juld[:] = [27830.1, 27830.2, math.nan]

        time_qc = old.createVariable('TIME_QC', 'i2', ('N_COUNT',))
        time_qc[:] = [1, 1, 1]
        time_qc.setncattr('flag_values', [0, 1, 2, 3, 4, 5])
        time_qc.setncattr('flag_meanings',
                          '0: no 1: good, 2: probably_good, 3: probably_bad, 4: very_bad, 5: modified')

        dsl = old.createVariable('DEPLOYMENT_START_LATITUDE', 'f8', ())
        dsl[:] = [44.9]

        dslon = old.createVariable('DEPLOYMENT_START_LONGITUDE', 'f8', ())
        dslon[:] = [-125.9]

        pt = old.createVariable('PLATFORM_TYPE', str, ())
        pt[:] = netcdf_string_to_vlen_bytes('SLOCUM_G2')

        oi = old.createVariable('OPERATING_INSTITUTION', str, ())
        oi[:] = netcdf_string_to_vlen_bytes('C-PROOF')

        gsn = old.createVariable('GLIDER_SERIAL_NO', str, ())
        gsn[:] = netcdf_string_to_vlen_bytes('123456')

        dsd = old.createVariable('DEPLOYMENT_START_DATE', str, ())
        dsd[:] = netcdf_string_to_vlen_bytes('20150102')

        phases = old.createVariable('PHASE', 'i2', ('N_COUNT',))
        phases.setncattr('missing_value', -1)
        phases[:] = [1, 2, -1]

        bt = old.createVariable('BATTERY_TYPE', str, ())
        bt[:] = netcdf_string_to_vlen_bytes('LithiumION')

        ts = old.createVariable('TRANS_SYSTEM', str, ('N_TRANS',))
        ts[:] = netcdf_string_to_vlen_bytes(['iridium'])

        ps = old.createVariable('POSITIONING_SYSTEM', str, ('N_POS',))
        ps[:] = netcdf_string_to_vlen_bytes(['gps', 'argos', 'iridium'])

        go = old.createVariable('GLIDER_OWNER', str, ())
        go[:] = netcdf_string_to_vlen_bytes('BIO;NAFC;DFO')

        cls._add_new_ego_sensor_info(old, [
            ('CTD_PRES', 'Company A', 'Model X', '12345', '', '')
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

    def test_is_valid(self):
        self.assertTrue(validate_ego_glider_file(self.old_file, {}, self.old_file.name))

    def test_dimensions(self):
        self.assertIn('N_MEASUREMENTS', self.new_handle.dimensions)

    def test_to_json(self):
        d = json.dumps(self.metadata.export())
        self.assertIsInstance(d, str)

