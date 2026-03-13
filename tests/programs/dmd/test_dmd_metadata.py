import datetime
import math

import netCDF4

from cnodc.programs.dmd.metadata import Encoding, Axis, NetCDFDataType, CoverageContentType, CommonDataModelType, \
    Common, CoordinateReferenceSystem, ContactRole, EntityRef, Variable, Direction, TimePrecision, TimeZone, Calendar, \
    IOOSCategory, CFVariableRole, ERDDAPVariableRole, get_bilingual_attribute, NumericTimeUnits, QuickWebPage, \
    ResourcePurpose, MaintenanceRecord, MaintenanceScope, ResourceType, Resource, GCContentType, GCLanguage, \
    GCContentFormat, Individual, TelephoneType, Country, IDSystem, Organization, Position, Citation, \
    GeneralUseConstraint, LegalConstraint, RestrictionCode, SecurityConstraint, ClassificationCode, ERDDAPServer, \
    Thesaurus, KeywordType, Keyword, DistributionChannel, DatasetMetadata, SpatialRepresentation, DistanceUnit, \
    AngularUnit, EssentialOceanVariable, StandardName, ERDDAPDatasetType, GCCollectionType, GCAudience, GCPlace, \
    GCSubject, TopicCategory, MaintenanceFrequency, StatusCode, GCPublisher, Locale, _Contact
from helpers.base_test_case import BaseTestCase


class TestDMDMetadataBasics(BaseTestCase):

    def test_encoding(self):
        self.assertIs(Encoding.from_string('utf-8'), Encoding.UTF8)
        self.assertIs(Encoding.from_string('utf8'), Encoding.UTF8)
        self.assertIs(Encoding.from_string('utf16'), Encoding.UTF16)
        self.assertEqual(Encoding.from_string('iso-8859-1'), Encoding.ISO_8859_1)

    def test_ioos_category(self):
        self.assertIs(IOOSCategory.from_string('DissolvedNutrients'), IOOSCategory.DissolvedNutrients)
        self.assertIs(IOOSCategory.from_string('dissolved nutrients'), IOOSCategory.DissolvedNutrients)
        self.assertIsNone(IOOSCategory.from_string(''))
        with self.assertRaises(ValueError):
            IOOSCategory.from_string('definitely not and never will be an ioos category thanks')

    def test_axis(self):
        self.assertIs(Axis.from_string('T'), Axis.Time)
        self.assertIs(Axis.from_string('X'), Axis.Longitude)
        self.assertIs(Axis.from_string('Y'), Axis.Latitude)
        self.assertIs(Axis.from_string('Z'), Axis.Depth)

    def test_netcdf_dtypes(self):
        with netCDF4.Dataset("inmemory", "w", diskless=True) as ds:
            str_var = ds.createVariable('test', str)
            cases = {
                NetCDFDataType.String: ["String", "S2", "S3", "S256", str_var.dtype],
                NetCDFDataType.Character: ["char", "S1", "c"],
                NetCDFDataType.Double: ["double", "float64", "f8", "d", ds.createVLType('f8', 'test2')],
                NetCDFDataType.Float: ["float", "float32", "f4", "f", ds.createVLType('f4', 'test3')],
                NetCDFDataType.Long: ["long", "int64", "i8", ds.createVLType('i8', 'test4')],
                NetCDFDataType.LongUnsigned: ["ulong", "uint64", "u8", ds.createVLType('u8', 'test5')],
                NetCDFDataType.Integer: ["int", "int32", "i4", "i", ds.createVLType('i4', 'test6')],
                NetCDFDataType.IntegerUnsigned: ["uint", "uint32", "u4", ds.createVLType('u4', 'test7')],
                NetCDFDataType.Short: ["short", "int16", "i2", "s", "h", ds.createVLType('i2', 'test8')],
                NetCDFDataType.ShortUnsigned: ["ushort", "uint16", "u2", ds.createVLType('u2', 'test9')],
                NetCDFDataType.Byte: ["byte", "int8", "i1", "b", "B", ds.createVLType('i1', 'test10')],
                NetCDFDataType.ByteUnsigned: ["ubyte", "uint8", "u1", ds.createVLType('u1', 'test11')],
            }
        for result in cases:
            for test_val in cases[result]:
                with self.subTest(dtype=test_val):
                    self.assertIs(NetCDFDataType.from_string(test_val), result)
        self.assertIsNone(NetCDFDataType.from_string(None))
        self.assertIsNone(NetCDFDataType.from_string(""))

    def test_coverage_content_type(self):
        self.assertIsNone(CoverageContentType.from_string(None))
        self.assertIsNone(CoverageContentType.from_string(""))
        self.assertIs(CoverageContentType.from_string("coordinate"), CoverageContentType.Coordinate)

    def test_cdm_type(self):
        tests = {
            CommonDataModelType.Point: ["Point", "point"],
            CommonDataModelType.Profile: ["Profile", "profile"],
            CommonDataModelType.TimeSeries: ["TimeSeries", "timeseries"],
            CommonDataModelType.TimeSeriesProfile: ["TimeSeriesProfile", "timeseriesprofile"],
            CommonDataModelType.Trajectory: ["Trajectory", "trajectory"],
            CommonDataModelType.TrajectoryProfile: ["TrajectoryProfile", "trajectoryprofile"],
            CommonDataModelType.Grid: ["Grid", "grid"],
            CommonDataModelType.MovingGrid: ["MovingGrid", "movinggrid"],
            CommonDataModelType.RadialSweep: ["RadialSweep", "radialsweep"],
            CommonDataModelType.Swath: ["Swath", "swath"],
            CommonDataModelType.Other: ["Other", "other", "nonsense"],
        }
        for result in tests:
            for in_val in tests[result]:
                with self.subTest(input=in_val):
                    self.assertIs(result, CommonDataModelType.from_string(in_val))
        self.assertIsNone(CommonDataModelType.from_string(None))
        self.assertIsNone(CommonDataModelType.from_string(''))

    def test_crs(self):
        tests = {
            CoordinateReferenceSystem.NAD27: ['4267', 4267, 'EPSG:4267', 'EPSG: 4267'],
            CoordinateReferenceSystem.WGS84: ['4326', 4326, 'EPSG:4326', 'EPSG: 4326'],
            CoordinateReferenceSystem.MSL_Depth: ['5715', 5715, 'epsg:5715', 'epsg: 5715'],
            CoordinateReferenceSystem.MSL_Heights: ['5714', 5714, 'epsg:5714', 'epsg :5714'],
            CoordinateReferenceSystem.Instant_Depth: ['5831', 5831, 'epsg:5831', 'EPSG : 5831'],
            CoordinateReferenceSystem.Instant_Heights: ['5829', 5829, 'EPSG:5829'],
            CoordinateReferenceSystem.Gregorian: ['gregorian', 'standard']
        }
        for result in tests:
            for in_val in tests[result]:
                with self.subTest(input=in_val):
                    self.assertIs(result, CoordinateReferenceSystem.from_string(in_val))
        self.assertIsNone(CoordinateReferenceSystem.from_string(None))
        self.assertIsNone(CoordinateReferenceSystem.from_string(''))
        with self.assertRaises(ValueError):
            CoordinateReferenceSystem.from_string('NOT A CRS')

    def test_gc_place(self):
        tests = {
            GCPlace.Canada: ['canada', 'CANADA', 'Canada'],
            GCPlace.Burlington: ['ontario - halton', 'Halton, ON', 'Halton, Ontario', 'Ontario -  Halton', 'ontario_-_halton'],
            GCPlace.Ottawa: ['Ottawa, ON', 'Ottawa, Ontario', 'Ontario - Ottawa'],
            GCPlace.Dartmouth: ['Halifax, NS', 'Halifax, Nova Scotia', 'Nova  Scotia  - Halifax '],
            GCPlace.Moncton: ['Westmorland, NS', '  Westmorland,  Nova Scotia', '  westmorland,nova scotia'],

        }
        for result in tests:
            for in_val in tests[result]:
                with self.subTest(input=in_val):
                    self.assertIs(result, GCPlace.from_string(in_val))
        self.assertIsNone(GCPlace.from_string(''))
        self.assertIsNone(GCPlace.from_string(None))

    def test_roles(self):
        self.assertIs(ContactRole.Stakeholder, ContactRole.from_string('CONT0001'))
        self.assertIs(ContactRole.Owner, ContactRole.from_string('CONT0002'))
        self.assertIs(ContactRole.Originator, ContactRole.from_string('CONT0003'))
        self.assertIs(ContactRole.PrincipalInvestigator, ContactRole.from_string('CONT0004'))
        self.assertIs(ContactRole.Stakeholder, ContactRole.from_string('CONT0005'))
        self.assertIs(ContactRole.Processor, ContactRole.from_string('CONT0006'))
        self.assertIs(ContactRole.Stakeholder, ContactRole.from_string('CONT0007'))
        self.assertIs(ContactRole.User, ContactRole.from_string('user'))
        self.assertIsNone(ContactRole.from_string(''))
        self.assertIsNone(ContactRole.from_string(None))

    def test_get_bilingual_attribute(self):
        locale_map = {'_en': 'en', '_fr': 'fr', '': 'en'}
        self.assertEqual({'en': 'foo'}, get_bilingual_attribute({'bar': 'foo'}, 'bar', locale_map))
        self.assertEqual({'en': 'foo', 'fr': 'le foo'},
                         get_bilingual_attribute({'bar': 'foo', 'bar_fr': 'le foo'}, 'bar', locale_map))
        self.assertEqual({}, get_bilingual_attribute({'bar': 'foo', 'bar_fr': 'le foo'}, 'bar2', locale_map))

    def test_calendar(self):
        self.assertIs(Calendar.Standard, Calendar.from_string('standard'))
        self.assertIs(Calendar.Standard, Calendar.from_string('gregorian'))
        self.assertIs(Calendar.Julian, Calendar.from_string('julian'))
        self.assertIs(Calendar.Julian, Calendar.from_string('JULIAN'))
        self.assertIsNone(Calendar.from_string(''))
        self.assertIsNone(Calendar.from_string(None))


class TestCoreEntityRef(BaseTestCase):

    def test_build_request_body(self):
        obj = EntityRef()
        obj.guid = '12345'
        obj.display_name = 'hello'
        obj._metadata['foo'] = 'bar'
        sub_ref = EntityRef()
        sub_ref.guid = '23456'
        obj._children['foo2'] = sub_ref
        obj._children['foo3'] = [EntityRef('12'), EntityRef('34'), EntityRef('56')]
        self.assertDictSimilar(obj.build_request_body(), {
            '_guid': '12345',
            '_display_names': {'und': 'hello'},
            'foo': 'bar',
            'foo2': {'_guid': '23456'},
            'foo3': [{'_guid': '12'}, {'_guid': '34'}, {'_guid': '56'}]
        })

    def test_multilingual_text(self):
        self.assertIsNone(EntityRef.format_multilingual_text(None))
        self.assertEqual(EntityRef.format_multilingual_text(''), {'und': ''})
        self.assertEqual(EntityRef.format_multilingual_text('foobar'), {'und': 'foobar'})
        self.assertEqual(EntityRef.format_multilingual_text({'en': 'foo'}), {'en': 'foo'})

    def test_format_date(self):
        self.assertIsNone(EntityRef.format_date(None))
        self.assertEqual(EntityRef.format_date(datetime.date(2015, 1, 2)), '2015-01-02')
        self.assertEqual(EntityRef.format_date(datetime.datetime(2015, 1, 2, 3, 4, 5)), '2015-01-02T03:04:05')
        self.assertEqual(EntityRef.format_date(datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)), '2015-01-02T03:04:05+00:00')
        self.assertEqual(EntityRef.format_date('2015-01-02'), '2015-01-02')
        self.assertEqual(EntityRef.format_date('2015-01-02T01:02:03'), '2015-01-02T01:02:03')

    def test_coerce_from_enum(self):
        self.assertListSimilar([None], EntityRef._coerce_from_enum(None, Locale, coerce_list=True))

    def test_coerce_to_enum(self):
        self.assertIs(EntityRef._coerce_to_enum(Locale.CanadianFrench, Locale), Locale.CanadianFrench)

class TestVariable(BaseTestCase):

    def test_set_via_constructor(self):
        var = Variable(
            guid='TEMP',
            display_name='Temperature',
            source_name='TEMP',
            source_data_type='f8',
            cnodc_name='Temperature',
            axis='T',
            actual_min=5,
            actual_max=10,
            positive_direction=Direction.Up,
            encoding='utf-8',
            destination_name='temperature',
            destination_data_type='f8',
            dimensions=('a', 'b'),
            long_name={
                'en': 'Temp',
                'fr': 'Temp but Fr',
            },
            standard_name='sea_water_temperature',
            time_precision=TimePrecision.Second.value,
            calendar=Calendar.Standard,
            time_zone=TimeZone.UTC,
            missing_value=-1,
            scale_factor=2,
            add_offset=10,
            ioos_category=IOOSCategory.Time,
            valid_min=0,
            valid_max=99,
            allow_subsets=False,
            cf_role=CFVariableRole.ProfileID,
            erddap_role=ERDDAPVariableRole.ProfileExtra,
            comment='hello world',
            references='oh no',
            source='sauce',
            coverage_content_type=CoverageContentType.Coordinate.value,
            variable_order=99,
            is_axis=True,
            is_altitude_proxy=False
        )
        self.assertEqual(var.guid, 'TEMP')
        self.assertEqual(var.display_name, {'und': 'Temperature'})
        self.assertEqual(var.source_name, 'TEMP')
        self.assertEqual(var.cnodc_name, 'Temperature')
        self.assertEqual(var.actual_min, 5)
        self.assertEqual(var.actual_max, 10)
        self.assertEqual(var.destination_name, 'temperature')
        self.assertEqual(var.dimensions, 'a,b')
        self.assertEqual(var.long_name, {'en': 'Temp', 'fr': 'Temp but Fr'})
        self.assertEqual(var.standard_name, 'sea_water_temperature')
        self.assertEqual(var.missing_value, -1)
        self.assertEqual(var.scale_factor, 2)
        self.assertEqual(var.add_offset, 10)
        self.assertEqual(var.valid_min, 0)
        self.assertEqual(var.valid_max, 99)
        self.assertEqual(var.comment, 'hello world')
        self.assertEqual(var.references, 'oh no')
        self.assertEqual(var.source, 'sauce')
        self.assertEqual(var.variable_order, 99)
        self.assertIs(var.is_axis, True)
        self.assertIs(var.is_altitude_proxy, False)
        self.assertIs(var.allow_subsets, False)
        self.assertIs(var.coverage_content_type, CoverageContentType.Coordinate)
        self.assertIs(var.erddap_role, ERDDAPVariableRole.ProfileExtra)
        self.assertIs(var.cf_role, CFVariableRole.ProfileID)
        self.assertIs(var.ioos_category, IOOSCategory.Time)
        self.assertIs(var.time_zone, TimeZone.UTC)
        self.assertIs(var.calendar, Calendar.Standard)
        self.assertIs(var.time_precision, TimePrecision.Second)
        self.assertIs(var.destination_data_type, NetCDFDataType.Double)
        self.assertIs(var.positive_direction, Direction.Up)
        self.assertIs(var.encoding, Encoding.UTF8)
        self.assertIs(var.source_data_type, NetCDFDataType.Double)
        self.assertIs(var.axis, Axis.Time)

    def test_set_to_none(self):
        var = Variable(
            calendar=None
        )
        self.assertIsNone(var.calendar)

    def test_additional_properties(self):
        var = Variable()
        var.additional_properties = {
            'five': 5,
            'none': None
        }
        self.assertIn('five', var.additional_properties)
        self.assertNotIn('none', var.additional_properties)
        var.additional_properties = {
            'ten': 10
        }
        self.assertIn('ten', var.additional_properties)
        self.assertNotIn('five', var.additional_properties)

    def test_set_time_from_epoch(self):
        var = Variable()
        var.set_time_units(NumericTimeUnits.Days, datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc))
        self.assertEqual(var.units, 'days since 2015-01-02T03:04:05+00:00')

    def test_build_numeric_from_netcdf(self):
        with netCDF4.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('N_MEASUREMENTS')
            test_var = ds.createVariable('test_var', 'i4', ('N_MEASUREMENTS',))
            test_var[:] = [-7, 2, 3, 4, 5, 6, 7, 8, 199, 201, 202]
            attrs = {
                'long_name': 'Long Name',
                'long_name_fr': 'Nom long',
                'comment': 'hello',
                'references': 'oh no',
                'source': 'sauce',
                'coverage_content_type': '',
                'units': 'km',
                'valid_min': 0,
                'valid_max': 200,
                'standard_name': 'something',
                'calendar': 'gregorian',
                'positive': 'down',
                'scale_factor': 1,
                'add_offset': 0,
                'time_precision': 'second',
                'time_zone': 'Etc/UTC',
                'cf_role': 'profile_id',
                'axis': 'T',
                'cnodc_standard_name': 'Temperature',
                'actual_min': 3,
                'actual_max': 10,
                '_Encoding': 'utf-8',
                'missing_value': 202,
            }
            for key in attrs:
                test_var.setncattr(key, attrs[key])
            var = Variable.build_from_netcdf(test_var, {'': 'en', '_fr': 'fr'})
            self.assertEqual(var.comment, 'hello')
            self.assertEqual(var.references, 'oh no')
            self.assertEqual(var.source, 'sauce')
            self.assertEqual(var.units, 'km')
            self.assertEqual(var.valid_min, 0)
            self.assertEqual(var.valid_max, 200)
            self.assertEqual(var.actual_min, 2)
            self.assertEqual(var.actual_max, 199)
            self.assertEqual(var.standard_name, 'something')
            self.assertEqual(var.scale_factor, 1)
            self.assertEqual(var.add_offset, 0)
            self.assertEqual(var.cnodc_name, 'Temperature')
            self.assertEqual(var.actual_min, 2)
            self.assertEqual(var.actual_max, 199)
            self.assertEqual(var.missing_value, 202)
            self.assertIs(var.encoding, Encoding.UTF8)
            self.assertIs(var.calendar, Calendar.Standard)
            self.assertIs(var.positive_direction, Direction.Down)
            self.assertIs(var.time_zone, TimeZone.UTC)
            self.assertIs(var.time_precision, TimePrecision.Second)
            self.assertIs(var.cf_role, CFVariableRole.ProfileID)
            self.assertIs(var.axis, Axis.Time)

    def test_netcdf_fill_value(self):
        with netCDF4.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('N_MEASUREMENTS')
            test_var = ds.createVariable('test_var', str, ('N_MEASUREMENTS',))
            var = Variable.build_from_netcdf(test_var, {'': 'en', '_fr': 'fr'})
            self.assertIs(var.encoding, Encoding.UTF8)
            self.assertIs(var.source_data_type, NetCDFDataType.String)


class TestMaintenanceRecord(BaseTestCase):

    def test_basics(self):
        mr = MaintenanceRecord(
            date=datetime.date(2015, 1, 2),
            notes='did stuff',
            scope=MaintenanceScope.Dataset
        )
        self.assertIs(mr.scope, MaintenanceScope.Dataset)
        self.assertEqual(mr.date, '2015-01-02')
        self.assertEqual(mr.notes, {'und': 'did stuff'})


class TestQuickWebPage(BaseTestCase):

    def test_basics(self):
        wp = QuickWebPage(
            name='hello',
            url='http://google.com/test',
            description={
                'en': 'foo',
                'fr': 'bar'
            },
            link_purpose=ResourcePurpose.Information
        )
        self.assertEqual(wp.name, {'und': 'hello'})
        self.assertEqual(wp.url, {'und': 'http://google.com/test'})
        self.assertEqual(wp.description, {'en': 'foo', 'fr': 'bar'})
        self.assertEqual(wp.resource_type, 'http')
        self.assertEqual(wp.link_purpose, ResourcePurpose.Information)

    def test_url(self):
        wp = QuickWebPage()
        wp.url = 'https://google.com/test'
        self.assertEqual(wp.url, {'und': 'https://google.com/test'})
        self.assertIs(wp.resource_type, 'https')

    def test_insecure_url(self):
        wp = QuickWebPage()
        wp.url = 'http://google.com/test'
        self.assertIs(wp.resource_type, 'http')

    def test_ftp_url(self):
        wp = QuickWebPage()
        wp.url = 'ftp://google.com/test'
        self.assertIs(wp.resource_type, 'ftp')

    def test_ftps_url(self):
        wp = QuickWebPage()
        wp.url = 'ftps://google.com/test'
        self.assertIs(wp.resource_type, 'ftp')

    def test_ftpse_url(self):
        wp = QuickWebPage()
        wp.url = 'ftpse://google.com/test'
        self.assertIs(wp.resource_type, 'ftp')

    def test_git_url(self):
        wp = QuickWebPage()
        wp.url = 'git://google.com/test'
        self.assertIs(wp.resource_type, 'git')

    def test_file_url(self):
        wp = QuickWebPage()
        wp.url = 'file:///google.com/test'
        self.assertIs(wp.resource_type, 'file')

    def test_other_url(self):
        wp = QuickWebPage()
        wp.url = 'azure://google.com/test'
        self.assertIsNone(wp.resource_type)

    def test_direct_set(self):
        wp = QuickWebPage()
        wp.resource_type = ResourceType.ERDDAPGrid
        wp.url = 'https://google.com/test'
        self.assertEqual(wp.resource_type, ResourceType.ERDDAPGrid.value)

    def test_autodetect_resource_type(self):
        self.assertIsNone(QuickWebPage.autodetect_resource_type(None))
        self.assertEqual('http', QuickWebPage.autodetect_resource_type({'und': 'http://www.google.com'}))
        self.assertEqual('https', QuickWebPage.autodetect_resource_type({'en': 'https://www.google.com'}))
        self.assertEqual('ftp', QuickWebPage.autodetect_resource_type({'fr': 'ftp://www.google.com'}))


class TestResource(BaseTestCase):

    def test_basics(self):
        res = Resource(
            name='hello',
            url='http://google.com/test',
            description={
                'en': 'foo',
                'fr': 'bar'
            },
            link_purpose=ResourcePurpose.Information,
            additional_request_info='dont',
            additional_app_info='browser',
            gc_content_type=GCContentType.Dataset,
            gc_language=GCLanguage.French
        )
        self.assertEqual(res.name, {'und': 'hello'})
        self.assertEqual(res.url, {'und': 'http://google.com/test'})
        self.assertEqual(res.description, {'en': 'foo', 'fr': 'bar'})
        self.assertEqual(res.resource_type, 'http')
        self.assertEqual(res.additional_app_info, {'und': 'browser'})
        self.assertEqual(res.additional_request_info, {'und': 'dont'})
        self.assertIs(res.gc_content_format, GCContentFormat.Hypertext)
        self.assertIs(res.gc_content_type, GCContentType.Dataset)
        self.assertIs(res.gc_language, GCLanguage.French)
        self.assertIs(res.link_purpose, ResourcePurpose.Information)

    def test_set_non_auto_gc_content_format(self):
        res = Resource()
        res.gc_content_format = GCContentFormat.DocumentDOC
        self.assertIs(res.gc_content_format, GCContentFormat.DocumentDOC)

    def test_autodetect_gc_content_format(self):
        self.assertIsNone(Resource.autodetect_gc_content_format(None))
        self.assertEqual(GCContentFormat.ArchiveTARGZIP.value, Resource.autodetect_gc_content_format({'und': 'https://domain.com/file.tar.gz'}))
        self.assertEqual(GCContentFormat.ImageBMP.value, Resource.autodetect_gc_content_format({'en': 'https://domain.com/file.bmp'}))
        self.assertEqual(GCContentFormat.DocumentPDF.value, Resource.autodetect_gc_content_format({'fr': 'https://domain.com/file.PDF'}))
        self.assertEqual(GCContentFormat.DocumentDOCX.value, Resource.autodetect_gc_content_format({'fr': 'https://domain.com\\file.DOCX'}))
        self.assertIsNone(Resource.autodetect_gc_content_format({'fr': 'file://domain.com/file/whatever'}))


class TestIndividual(BaseTestCase):

    def test_basics(self):
        c = Individual(
            name='Joanne Smith',
            email='hello@world.com',
            service_hours='never',
            instructions='just dont',
        )
        self.assertEqual(c.email, {'und': 'hello@world.com'})
        self.assertEqual(c.service_hours, {'und': 'never'})
        self.assertEqual(c.instructions, {'und': 'just dont'})
        self.assertEqual(c.name, 'Joanne Smith')
        self.assertEqual(c._metadata['individual_name'], 'Joanne Smith')

    def test_orcid(self):
        c = Individual()
        c.orcid = 'http://orcid.org/12345'
        self.assertEqual(c.orcid, '12345')
        self.assertEqual(c.orcid, c.id_code)
        self.assertIs(c.id_system, IDSystem.ORCID)
        c.id_description = 'foobar'
        self.assertEqual(c.id_description, {'und': 'foobar'})

    def test_resource(self):
        c = Individual()
        res = Resource(url='http://www.google.com')
        c.resources.append(res)
        self.assertEqual(1, len(c.resources))
        self.assertEqual(1, len(c._children['web_resources']))
        self.assertIs(res, c._children['web_resources'][0])

    def test_add_telephone(self):
        c = Individual()
        c.add_telephone_number(TelephoneType.Cell, '613-325-6210')
        self.assertEqual(c._metadata['phone'], [
            {'phone_number_type': TelephoneType.Cell.value, 'phone_number': '613-325-6210'}
        ])

    def test_set_address(self):
        c = Individual()
        c.set_address(
            {'en': '555 Fake Road', 'fr': '555 rue Fake'},
            'Ottawa',
            'Ontario',
            Country.Canada,
            'H0H0H0'
        )
        self.assertEqual(c._metadata['delivery_point'], {'en': '555 Fake Road', 'fr': '555 rue Fake'})
        self.assertEqual(c._metadata['city'], 'Ottawa')
        self.assertEqual(c._metadata['admin_area'], {'und': 'Ontario'})
        self.assertEqual(c._metadata['country'], Country.Canada.value)
        self.assertEqual(c._metadata['postal_code'], 'H0H0H0')

    def test_set_web_page(self):
        c = Individual()
        c.set_web_page(
            'https://www.google.com',
            'hello world',
            'this is my hello world web page'
        )
        self.assertEqual(c._metadata['web_page']['url'], {'und': 'https://www.google.com'})
        self.assertEqual(c._metadata['web_page']['name'], {'und': 'hello world'})
        self.assertEqual(c._metadata['web_page']['description'], {'und': 'this is my hello world web page'})
        self.assertEqual(c._metadata['web_page']['function'], ResourcePurpose.Information.value)
        self.assertEqual(c._metadata['web_page']['protocol'], 'https')


class TestOrganization(BaseTestCase):

    def test_name(self):
        o = Organization()
        o.name = 'foobar'
        self.assertEqual(o.name, {'und': 'foobar'})
        self.assertEqual(o._metadata['organization_name'], {'und': 'foobar'})

    def test_ror(self):
        o = Organization()
        o.ror = 'https://ror.org/234567'
        self.assertEqual(o.ror, '234567')
        self.assertEqual(o.ror, o.id_code)
        self.assertIs(o.id_system, IDSystem.ROR)

    def test_individuals(self):
        x = Individual(name='Barbara Anne')
        o = Organization()
        o.individuals.append(x)
        self.assertEqual(1, len(o._children['individuals']))
        self.assertIs(x, o._children['individuals'][0])


class TestPosition(BaseTestCase):

    def test_name(self):
        p = Position(name='foobar')
        self.assertEqual(p.name, {'und': 'foobar'})
        self.assertEqual(p._metadata['position_name'], {'und': 'foobar'})


class TestCitation(BaseTestCase):

    def test_basics(self):
        res = Resource(url='https://www.google.com/')
        cit = Citation(
            title='title',
            alt_title='alt_title',
            details='details',
            edition='edition',
            publication_date=datetime.date(2015, 1, 2),
            revision_date=datetime.date(2015, 1, 3),
            creation_date='2015-01-01',
            isbn='12345',
            issn='12345X',
            resource=res,
            id_code='12345',
            id_system=IDSystem.DOI,
            id_description='code for my luggage'
        )
        self.assertEqual(cit.title, {'und': 'title'})
        self.assertEqual(cit.alt_title, {'und': 'alt_title'})
        self.assertEqual(cit.details, {'und': 'details'})
        self.assertEqual(cit.edition, {'und': 'edition'})
        self.assertEqual(cit.publication_date, '2015-01-02')
        self.assertEqual(cit.revision_date, '2015-01-03')
        self.assertEqual(cit.creation_date, '2015-01-01')
        self.assertEqual(cit.isbn, '12345')
        self.assertEqual(cit.issn, '12345X')
        self.assertIs(cit.resource, res)
        self.assertEqual(cit.id_code, '12345')
        self.assertEqual(cit.id_description, {'und': 'code for my luggage'})
        self.assertIs(cit.id_system, IDSystem.DOI)

    def test_resource_none_to_dict(self):
        cit = Citation()
        cit.title = 'foobar'
        cit.resource = None
        self.assertEqual({
            'title': {'und': 'foobar'}
        }, cit.build_request_body())

    def test_resource_something_to_dict(self):
        cit = Citation()
        cit.title = 'foobar'
        cit.resource = Resource(url='http://foobar.com')
        self.assertDictSimilar({
            'title': {'und': 'foobar'},
            'resource': {
                'url': {'und': 'http://foobar.com'},
                'goc_formats': ['HTML'],
                'protocol': 'http',
            }
        }, cit.build_request_body())

    def test_responsibles(self):
        cit = Citation()
        contact = Individual(name='Oscar Wilde')
        cit.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(cit._children['responsibles']))
        self.assertIs(contact, cit._children['responsibles'][0]._children['contact'])
        self.assertEqual(ContactRole.Owner.value, cit._children['responsibles'][0]._metadata['role'])


class TestGeneralUseConstraint(BaseTestCase):

    def test_basics(self):
        con = GeneralUseConstraint(
            description='hello',
            plain_text_version='what'
        )
        self.assertEqual(con.description, {'und': 'hello'})
        self.assertEqual(con.plain_text_version, {'und': 'what'})

    def test_references(self):
        ref = Citation()
        con = GeneralUseConstraint()
        con.citations.append(ref)
        self.assertIs(ref, con._children['reference'][0])

    def test_responsibles(self):
        con = GeneralUseConstraint()
        contact = Individual(name='Oscar Wilde')
        con.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(con._children['responsibles']))
        self.assertIs(contact, con._children['responsibles'][0]._children['contact'])
        self.assertEqual(ContactRole.Owner.value, con._children['responsibles'][0]._metadata['role'])


class TestLegalConstaint(BaseTestCase):

    def test_basics(self):
        con = LegalConstraint(
            access_constraints=RestrictionCode.Restricted,
            use_constraints=[RestrictionCode.License, RestrictionCode.Confidential],
            other_constraints='oh no'
        )
        self.assertEqual(con.other_constraints, {'und': 'oh no'})
        self.assertEqual(con.access_constraints, [RestrictionCode.Restricted])
        self.assertEqual(con.use_constraints, [RestrictionCode.License, RestrictionCode.Confidential])


class TestSecurityConstraint(BaseTestCase):

    def test_basics(self):
        con = SecurityConstraint(
            classification=ClassificationCode.Secret,
            user_notes='oh no',
            classification_system='custom'
        )
        self.assertEqual(con.classification, ClassificationCode.Secret)
        self.assertEqual(con.user_notes, {'und': 'oh no'})
        self.assertEqual(con.classification_system, {'und': 'custom'})


class TestErddapServer(BaseTestCase):

    def test_basics(self):
        server = ERDDAPServer(
            base_url='https://www.google.com/'
        )
        self.assertEqual(server.base_url, 'https://www.google.com/')

    def test_responsibles(self):
        server = ERDDAPServer()
        contact = Individual(name='Oscar Wilde')
        server.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(server._children['responsibles']))
        self.assertIs(contact, server._children['responsibles'][0]._children['contact'])
        self.assertEqual(ContactRole.Owner.value, server._children['responsibles'][0]._metadata['role'])


class TestThesaurus(BaseTestCase):

    def test_basics(self):
        cit = Citation()
        t = Thesaurus(
            keyword_type=KeywordType.Place,
            prefix='foo',
            citation=cit
        )
        self.assertEqual(t.prefix, 'foo')
        self.assertIs(t.keyword_type, KeywordType.Place)
        self.assertIs(t.citation, cit)


class TestKeyword(BaseTestCase):

    def test_basics(self):
        t = Thesaurus(prefix='foo')
        k = Keyword(
            text='keyword',
            description='stuff',
            thesaurus=t
        )
        self.assertEqual(k.text, {'und': 'keyword'})
        self.assertEqual(k.description, {'und': 'stuff'})
        self.assertIs(k.thesaurus, t)


class TestDistributionChannel(BaseTestCase):

    def test_basics(self):
        res1 = Resource()
        dc = DistributionChannel(
            description='foo',
            primary_link=res1
        )
        self.assertEqual(dc.description, {'und': 'foo'})
        self.assertIs(dc.primary_link, res1)

    def test_links(self):
        dc = DistributionChannel()
        res = Resource(url='https://www.google.com/')
        dc.links.append(res)
        self.assertIs(res, dc._children['links'][0])

    def test_responsibles(self):
        dc = DistributionChannel()
        contact = Individual(name='Oscar Wilde')
        dc.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(dc._children['responsibles']))
        self.assertIs(contact, dc._children['responsibles'][0]._children['contact'])
        self.assertEqual(ContactRole.Owner.value, dc._children['responsibles'][0]._metadata['role'])


class TestMetadata(BaseTestCase):

    def test_feature_type(self):
        md = DatasetMetadata()
        md.feature_type = CommonDataModelType.Swath
        self.assertIs(md.feature_type, CommonDataModelType.Swath)
        self.assertIsNone(md.spatial_representation)

    def test_feature_type_with_sr(self):
        md = DatasetMetadata()
        md.feature_type = CommonDataModelType.Profile
        self.assertIs(md.feature_type, CommonDataModelType.Profile)
        self.assertIs(md.spatial_representation, SpatialRepresentation.TextTable)

    def test_feature_type_with_sr_no_override(self):
        md = DatasetMetadata()
        md.spatial_representation = SpatialRepresentation.Grid
        md.feature_type = CommonDataModelType.Profile
        self.assertIs(md.feature_type, CommonDataModelType.Profile)
        self.assertIs(md.spatial_representation, SpatialRepresentation.Grid)

    def test_standard_names(self):
        md = DatasetMetadata()
        md.add_cf_standard_name(StandardName.AirPressure)
        self.assertEqual(1, len(md.cf_standard_names))
        # no duplicates
        md.add_cf_standard_name('sea_water_temperature')
        self.assertEqual(2, len(md.cf_standard_names))
        md.add_cf_standard_name('sea_water_temperature')
        self.assertEqual(2, len(md.cf_standard_names))
        md.add_cf_standard_name('sea_water_practical_salinity')
        self.assertEqual(3, len(md.cf_standard_names))

    def test_set_spatial_res(self):
        md = DatasetMetadata()
        md.set_spatial_resolution(
            scale=30000,
            level_of_detail='stuff is visibile',
            horizontal=5,
            horizontal_units=DistanceUnit.Meters,
            vertical=10,
            vertical_units=DistanceUnit.Kilometers,
            angular=30,
            angular_units=AngularUnit.ArcMinutes
        )
        self.assertEqual(md._metadata['spatial_resolution'], {
            'scale': 30000,
            'level_of_detail': {'und': 'stuff is visibile'},
            'distance': "5",
            'distance_units': 'm',
            'vertical': "10",
            'vertical_units': 'km',
            'angular': "30",
            'angular_units': 'arc_minute'
        })

    def test_set_time_res(self):
        md = DatasetMetadata()
        md.set_time_resolution(seconds=3.5)
        self.assertEqual(md._metadata['temporal_resolution'], {
            'years': None,
            'months': None,
            'days': None,
            'hours': None,
            'minutes': None,
            'seconds': 3
        })

    def test_set_time_resolution_from_iso(self):
        md = DatasetMetadata()
        md.set_time_resolution_from_iso('P5W')
        self.assertEqual(md._metadata['temporal_resolution']['days'], 35)
        md.set_time_resolution_from_iso('P3Y')
        self.assertEqual(md._metadata['temporal_resolution']['years'], 3)
        self.assertIsNone(md._metadata['temporal_resolution']['days'])
        md.set_time_resolution_from_iso('P4M')
        self.assertEqual(md._metadata['temporal_resolution']['months'], 4)
        self.assertIsNone(md._metadata['temporal_resolution']['years'])
        md.set_time_resolution_from_iso('P5D')
        self.assertEqual(md._metadata['temporal_resolution']['days'], 5)
        md.set_time_resolution_from_iso('PT3H')
        self.assertEqual(md._metadata['temporal_resolution']['hours'], 3)
        md.set_time_resolution_from_iso('PT9M')
        self.assertEqual(md._metadata['temporal_resolution']['minutes'], 9)
        self.assertIsNone(md._metadata['temporal_resolution']['months'])
        md.set_time_resolution_from_iso('PT1M30S')
        self.assertEqual(md._metadata['temporal_resolution']['minutes'], 1)
        self.assertEqual(md._metadata['temporal_resolution']['seconds'], 30)
        md.set_time_resolution_from_iso('P0000-00-00T01:30:00')
        self.assertEqual(md._metadata['temporal_resolution']['hours'], 1)
        self.assertEqual(md._metadata['temporal_resolution']['minutes'], 30)
        self.assertIsNone(md._metadata['temporal_resolution']['seconds'])
        with self.assertRaises(ValueError):
            md.set_time_resolution_from_iso('P00-00-00T00:00:00')
        with self.assertRaises(ValueError):
            md.set_time_resolution_from_iso('P00Z')
        with self.assertRaises(ValueError):
            md.set_time_resolution_from_iso('P0000-00-00T00:00:0')
        with self.assertRaises(ValueError):
            md.set_time_resolution_from_iso('P5WT2H')
        with self.assertRaises(ValueError):
            md.set_time_resolution_from_iso('5W')

    def test_add_eov(self):
        md = DatasetMetadata()
        md.add_essential_ocean_variable(EssentialOceanVariable.OceanSound)
        self.assertIn(EssentialOceanVariable.OceanSound.value, md._metadata['cioos_eovs'])

    def test_set_erddap_info(self):
        md = DatasetMetadata()
        md.set_erddap_info(
            ERDDAPServer(base_url='https://www.google.com/erddap/'),
            'foobar',
            ERDDAPDatasetType.NetCDFGrid,
            '/cloud_data/stuff',
            '*\\.nc'
        )
        self.assertIn('erddap', md._profiles)
        self.assertEqual(1, len(md._children['erddap_servers']))
        self.assertEqual('*\\.nc', md._metadata['erddap_data_file_pattern'])
        self.assertEqual('/cloud_data/stuff', md._metadata['erddap_data_file_path'])
        self.assertEqual('foobar', md._metadata['erddap_dataset_id'])
        self.assertEqual(ERDDAPDatasetType.NetCDFGrid.value, md._metadata['erddap_dataset_type'])

    def test_set_erddap_info_list(self):
        md = DatasetMetadata()
        md.set_erddap_info(
            [ERDDAPServer(base_url='https://www.google.com/erddap/'), ERDDAPServer(base_url='https://www.google.com/erddap2/')],
            'foobar',
            ERDDAPDatasetType.NetCDFGrid,
            '/cloud_data/stuff',
            '*\\.nc'
        )
        self.assertIn('erddap', md._profiles)
        self.assertEqual(2, len(md._children['erddap_servers']))
        self.assertEqual('*\\.nc', md._metadata['erddap_data_file_pattern'])
        self.assertEqual('/cloud_data/stuff', md._metadata['erddap_data_file_path'])
        self.assertEqual('foobar', md._metadata['erddap_dataset_id'])
        self.assertEqual(ERDDAPDatasetType.NetCDFGrid.value, md._metadata['erddap_dataset_type'])

    def test_set_meds_defaults(self):
        md = DatasetMetadata()
        md.set_meds_defaults()
        self.assertEqual(md.goc_collection, GCCollectionType.Geospatial)
        self.assertEqual(md.goc_audiences, [GCAudience.Scientists])
        self.assertEqual(md.goc_publication_places, [GCPlace.Ottawa])
        self.assertEqual(md.goc_subject, GCSubject.Oceanography)
        self.assertEqual(md._security_level, 'unclassified')
        self.assertEqual(md.cf_standard_name_vocab, 'CF 1.13')
        self.assertEqual(md._pub_workflow, 'cnodc_publish')
        self.assertEqual(md._act_workflow, 'cnodc_activation')
        self.assertEqual(md.topic_category, TopicCategory.Oceans)
        self.assertEqual(md.status, StatusCode.Final)
        self.assertEqual(md.data_maintenance_frequency, MaintenanceFrequency.NotPlanned)
        self.assertEqual(md.metadata_maintenance_frequency, MaintenanceFrequency.NotPlanned)
        self.assertEqual(md.publisher, Common.Contact_CNODC)
        self.assertEqual(md.goc_publisher, GCPublisher.MEDS)
        self.assertEqual(md.metadata_owner, Common.Contact_CNODC)
        self.assertIs(md.metadata_standards[0], Common.MetadataStandard_ISO19115)
        self.assertIs(md.metadata_standards[1], Common.MetadataStandard_ISO191151)
        self.assertIs(md.metadata_profiles[0], Common.MetadataProfile_CIOOS)
        self.assertIs(md.data_constraints[0], Common.Constraint_Unclassified)
        self.assertIs(md.data_constraints[1], Common.Constraint_OpenGovernmentLicense)
        self.assertIs(md.metadata_constraints[0], Common.Constraint_Unclassified)
        self.assertIs(md.metadata_constraints[1], Common.Constraint_OpenGovernmentLicense)

    def test_set_info_link(self):
        md = DatasetMetadata()
        md.set_info_link('http://www.google.com', 'hello', 'world')
        self.assertEqual(md._children['info_link'].url, {'und': 'http://www.google.com'})
        self.assertEqual(md._children['info_link'].name, {'und': 'hello'})
        self.assertEqual(md._children['info_link'].description, {'und': 'world'})
        self.assertEqual(md._children['info_link'].purpose, ResourcePurpose.Information)
        self.assertEqual(md._children['info_link'].resource_type, 'http')

    def test_update_additional_properties(self):
        md = DatasetMetadata()
        md.update_additional_properties({'foo': 'bar', 'bar': None, 'hello': 'world'})
        self.assertEqual(md._metadata['custom_metadata'], {'foo': 'bar', 'hello': 'world'})
        md.update_additional_properties({'foo': 'bar2', 'bar': 'yes', 'hello': None})
        self.assertEqual(md._metadata['custom_metadata'], {'foo': 'bar2', 'hello': 'world', 'bar': 'yes'})

    def test_responsibles(self):
        md = DatasetMetadata()
        contact = Individual(name='Oscar Wilde')
        md.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(md._children['responsibles']))
        self.assertIs(contact, md._children['responsibles'][0]._children['contact'])
        self.assertEqual(ContactRole.Owner.value, md._children['responsibles'][0]._metadata['role'])

    def test_add_dmd_user(self):
        md = DatasetMetadata()
        md.add_user('turnbuller')
        self.assertIn('turnbuller', md._users)

    def test_set_parent_org(self):
        md = DatasetMetadata()
        self.assertIsNone(md._org_name)
        md.set_parent_organization('meds')
        self.assertEqual(md._org_name, 'meds')

    def test_add_variable_eov_surface(self):
        v = Variable()
        v.cnodc_name = 'parameters/Temperature'
        v.standard_name = 'seawater_temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['seaSurface'])
        self.assertIs(v.ioos_category, IOOSCategory.Temperature)
        self.assertIn('seawater_temperature', md.cf_standard_names)
        self.assertIn(EssentialOceanVariable.SurfaceTemperature.value, md._metadata['cioos_eovs'])
        self.assertNotIn(EssentialOceanVariable.SubSurfaceTemperature.value, md._metadata['cioos_eovs'])

    def test_add_variable_eov_subsurface(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['subSurface'])
        self.assertIs(v.ioos_category, IOOSCategory.Temperature)
        self.assertNotIn(EssentialOceanVariable.SurfaceTemperature.value, md._metadata['cioos_eovs'])
        self.assertIn(EssentialOceanVariable.SubSurfaceTemperature.value, md._metadata['cioos_eovs'])

    def test_add_variable_eov_both(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['subSurface', 'seaSurface'])
        self.assertIn(EssentialOceanVariable.SurfaceTemperature.value, md._metadata['cioos_eovs'])
        self.assertIn(EssentialOceanVariable.SubSurfaceTemperature.value, md._metadata['cioos_eovs'])

    def test_add_variable_eov_none(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertNotIn('cioos_eovs', md._metadata)

    def test_add_variable_ioos_unknown(self):
        v = Variable()
        v.cnodc_name = 'Temp'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)
        self.assertNotIn('cioos_eovs', md._metadata)

    def test_add_variable_known_missing(self):
        v = Variable()
        v.cnodc_name = 'CNODCDuplicateId'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)
        self.assertNotIn('cioos_eovs', md._metadata)

    def test_set_longitude_from_var(self):
        v = Variable(axis='X')
        v.actual_min = 5
        v.actual_max = 15
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.geospatial_lon_max, 15)
        self.assertEqual(md.geospatial_lon_min, 5)

    def test_set_latitude_from_var(self):
        v = Variable(axis='Y')
        v.actual_min = 50
        v.actual_max = 60
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.geospatial_lat_max, 60)
        self.assertEqual(md.geospatial_lat_min, 50)

    def test_set_depth_from_var(self):
        v = Variable(axis='Z')
        v.actual_min = 75
        v.actual_max = 125
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.geospatial_vertical_max, 125)
        self.assertEqual(md.geospatial_vertical_min, 75)

    def test_set_time_from_var_days(self):
        v = Variable(axis='T')
        v.actual_min = 10
        v.actual_max = 20
        v.units = 'days since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.time_coverage_start, '1950-01-11T00:00:00+00:00')
        self.assertEqual(md.time_coverage_end, '1950-01-21T00:00:00+00:00')

    def test_set_time_from_var_seconds(self):
        v = Variable(axis='T')
        v.actual_min = 90
        v.actual_max = 600
        v.units = 'seconds since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.time_coverage_start, '1950-01-01T00:01:30+00:00')
        self.assertEqual(md.time_coverage_end, '1950-01-01T00:10:00+00:00')

    def test_set_time_from_var_minutes(self):
        v = Variable(axis='T')
        v.actual_min = 90
        v.actual_max = 630.5
        v.units = 'minutes since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.time_coverage_start, '1950-01-01T01:30:00+00:00')
        self.assertEqual(md.time_coverage_end, '1950-01-01T10:30:30+00:00')

    def test_set_time_from_var_hours(self):
        v = Variable(axis='T')
        v.actual_min = 48
        v.actual_max = 48 + 5 + (30 / 60) + (45/3600)
        v.units = 'hours since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(md.time_coverage_start, '1950-01-03T00:00:00+00:00')
        self.assertEqual(md.time_coverage_end, '1950-01-03T05:30:45+00:00')

    def test_set_time_from_var_unsupported(self):
        v = Variable(axis='T')
        v.actual_min = 1
        v.actual_max = 2
        v.units = 'months since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', level='WARNING'):
            md.add_variable(v)
        self.assertIsNone(md.time_coverage_end)
        self.assertIsNone(md.time_coverage_start)

    def test_set_time_from_var_weird_units(self):
        v = Variable(axis='T')
        v.actual_min = 1
        v.actual_max = 2
        v.units = 'days'
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', level='WARNING'):
            md.add_variable(v)
        self.assertIsNone(md.time_coverage_end)
        self.assertIsNone(md.time_coverage_start)

    def test_set_time_from_var_bad_epoch(self):
        v = Variable(axis='T')
        v.actual_min = 1
        v.actual_max = 2
        v.units = 'seconds since 1950-13-41'
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', level='ERROR'):
            md.add_variable(v)
        self.assertIsNone(md.time_coverage_end)
        self.assertIsNone(md.time_coverage_start)

    def test_set_from_english_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths[:] = [5,6,7,8]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            attrs = {
                'default_locale': 'en-CA',
                'locales': '_fr: fr-CA',
                'title': 'Hello',
                'title_fr': 'Bonjour',
                'program': 'Program',
                'project': 'Project',
                'institution': 'dfo',
                'id': '12345',
                'featureType': 'profile',
                'processing_level': 'raw',
                'geospatial_bounds': 'POINT(1 2)',
                'Conventions': 'hello,world,shenanigans',
                'processing_description': 'i did stuff',
                'processing_environment': 'my computer',
                'acknowledgement': 'my computer did stuff',
                'comment': 'oh no',
                'references': 'yes i have them',
                'source': 'i made it up',
                'summary': 'what i did but briefly',
                'purpose': 'why i made this',
                'standard_name_vocabulary': 'CF-1.12',
                'date_issued': '2015-01-02',
                'date_created': '2016-01-02',
                'date_modified': '2017-01-02',
                'data_maintenance_frequency': 'daily',
                'metadata_maintenance_frequency': 'asNeeded',
                'status': 'onGoing',
                'topic_category': 'oceans',
                'gc_audiences': 'scientists;parents',
                'gc_subject': 'oceanography',
                'gc_publication_places': 'Ottawa, Ontario;Nanaimo, BC;Newfoundland and Labrador - Division No. 1;ontario_-_halton',
                'infoUrl': 'https://dfo-mpo.gc.ca',
                'doi': 'doi:10.1.2.3/456',
                'metadata_link': 'https://cnodc/full_metadata.xml',
                'time_coverage_resolution': 'P60S',
                'geospatial_bounds_crs': 'wgs84',
                'geospatial_bounds_vertical_crs': 'msld',
                'creator_name': 'Erin Turnbull',
                'creator_email': 'erin@fake.com',
                'creator_id': '12345',
                'publisher_name': 'Marine Environmental Data Section',
                'publisher_name_fr': 'SDMM',
                'publisher_email': 'meds@fake.com',
                'publisher_email_fr': 'sdmm@fake.com',
                'publisher_id': '234567',
                'publisher_url': 'https://meds.com',
                'publisher_type': 'institution',
                'contributor_name': 'Anh Tran,Jenny Chiu,BIO,MEDS Coordinator',
                'contributor_name_fr': ',,,Coordinateur de SDMM',
                'contributor_email': 'anh@fake.com,jenny@fake.com,bio@fake.com,coordinator@fake.com',
                'contributor_id': '123,456,7890,',
                'contributor_type': 'individual,individual,institution,position',
                'contributor_role': 'contributor,editor,funder,mediator',
                'contributor_id_vocabulary': 'https://orcid.org,https://orcid.org,https://ror.org,',
                'contributing_institutions_name': 'C-PROOF',
            }
            for attr in attrs:
                ds.setncattr(attr, attrs[attr])
            md = DatasetMetadata()
            md.set_meds_defaults()
            md.set_from_netcdf_file(ds, 'en')
            self.assertIs(md.primary_metadata_locale, Locale.CanadianEnglish)
            self.assertIs(md.secondary_metadata_locales[0], Locale.CanadianFrench)
            self.assertIs(md.primary_data_locale, Locale.CanadianEnglish)
            self.assertIs(md.secondary_data_locales[0], Locale.CanadianFrench)
            self.assertEqual(md.title, {'en': 'Hello', 'fr': 'Bonjour'})
            self.assertEqual(md.display_name, {'en': 'Hello', 'fr': 'Bonjour'})
            self.assertEqual(md.program, 'Program')
            self.assertEqual(md.project, 'Project')
            self.assertEqual(md.institution, 'dfo')
            self.assertEqual(md.guid, '12345')
            self.assertEqual(md.processing_level, 'raw')
            self.assertEqual(md.geospatial_bounds, 'POINT(1 2)')
            self.assertEqual(md.conventions, 'hello,world,shenanigans')
            self.assertEqual(md.processing_description, {'en': 'i did stuff'})
            self.assertEqual(md.processing_environment, {'en': 'my computer'})
            self.assertEqual(md.credit, {'en': 'my computer did stuff'})
            self.assertEqual(md.comment, {'en': 'oh no'})
            self.assertEqual(md.references, {'en': 'yes i have them'})
            self.assertEqual(md.source, {'en': 'i made it up'})
            self.assertEqual(md.abstract, {'en': 'what i did but briefly'})
            self.assertEqual(md.purpose, {'en': 'why i made this'})
            self.assertEqual(md.cf_standard_name_vocab, 'CF-1.12')
            self.assertEqual(md.date_issued, '2015-01-02T00:00:00')
            self.assertEqual(md.date_created, '2016-01-02T00:00:00')
            self.assertEqual(md.date_modified, '2017-01-02T00:00:00')
            self.assertIs(md.feature_type, CommonDataModelType.Profile)
            self.assertIs(md.data_maintenance_frequency, MaintenanceFrequency.Daily)
            self.assertIs(md.metadata_maintenance_frequency, MaintenanceFrequency.AsNeeded)
            self.assertIs(md.status, StatusCode.OnGoing)
            self.assertIs(md.topic_category, TopicCategory.Oceans)
            self.assertIs(md.goc_subject, GCSubject.Oceanography)
            self.assertListEqual(md.goc_audiences, [GCAudience.Scientists, GCAudience.Parents])
            self.assertListEqual(md.goc_publication_places, [GCPlace.Ottawa, GCPlace.Nanaimo, GCPlace.StJohns, GCPlace.Burlington])
            self.assertEqual({'en': 'https://dfo-mpo.gc.ca'}, md._children['info_link'].url)
            self.assertEqual('10.1.2.3/456', md.doi)
            self.assertEqual({'en': 'https://cnodc/full_metadata.xml'}, md.alt_metadata_citations[0].resource.url)
            self.assertEqual(md._metadata['temporal_resolution'], {
                'years': None,
                'months': None,
                'days': None,
                'hours': None,
                'minutes': None,
                'seconds': 60
            })
            self.assertIs(md.geospatial_crs, CoordinateReferenceSystem.WGS84)
            self.assertIs(md.geospatial_vertical_crs, CoordinateReferenceSystem.MSL_Depth)
            self.assertEqual(7, len(md.responsibles))
            resps: dict[str, tuple[str, _Contact]] = {
                (resp._children['contact'].name if isinstance(resp._children['contact'].name, str) else resp._children['contact'].name['en']): (resp._metadata['role'], resp._children['contact'])
                for resp in md.responsibles
            }
            with self.subTest(msg='creator'):
                self.assertIn('Erin Turnbull', resps)
                creator_role, creator = resps['Erin Turnbull']
                self.assertIsInstance(creator, Individual)
                self.assertEqual(creator_role, 'originator')
                self.assertEqual(creator.name, 'Erin Turnbull')
                self.assertEqual(creator.email, {'en': 'erin@fake.com'})
                self.assertEqual(creator.orcid, '12345')
            with self.subTest(msg='publisher'):
                self.assertIn('Marine Environmental Data Section', resps)
                pub_role, publisher = resps['Marine Environmental Data Section']
                self.assertEqual(pub_role, 'publisher')
                self.assertIsInstance(publisher, Organization)
                self.assertEqual(publisher.name, {'en': 'Marine Environmental Data Section', 'fr': 'SDMM'})
                self.assertEqual(publisher.email, {'en': 'meds@fake.com', 'fr': 'sdmm@fake.com'})
                self.assertEqual(publisher.ror, '234567')
                self.assertEqual(publisher._metadata['web_page']['url'], {'en': 'https://meds.com'})
            with self.subTest(msg='contributor1'):
                self.assertIn('Anh Tran', resps)
                role, contact = resps['Anh Tran']
                self.assertIsInstance(contact, Individual)
                self.assertEqual(contact.name, 'Anh Tran')
                self.assertEqual(contact.email, {'en': 'anh@fake.com'})
                self.assertEqual(contact.orcid, '123')
                self.assertEqual(role, 'contributor')
            with self.subTest(msg='contributor2'):
                self.assertIn('Jenny Chiu', resps)
                role, contact = resps['Jenny Chiu']
                self.assertIsInstance(contact, Individual)
                self.assertEqual(contact.name, 'Jenny Chiu')
                self.assertEqual(contact.email, {'en': 'jenny@fake.com'})
                self.assertEqual(contact.orcid, '456')
                self.assertEqual(role, 'editor')
            with self.subTest(msg='contributor3'):
                self.assertIn('BIO', resps)
                role, contact = resps['BIO']
                self.assertIsInstance(contact, Organization)
                self.assertEqual(role, 'funder')
                self.assertEqual(contact.name, {'en': 'BIO'})
                self.assertEqual(contact.email, {'en': 'bio@fake.com'})
                self.assertEqual(contact.ror, '7890')
            with self.subTest(msg='contributor4'):
                self.assertIn('MEDS Coordinator', resps)
                role, contact = resps['MEDS Coordinator']
                self.assertIsInstance(contact, Position)
                self.assertEqual(role, 'mediator')
                self.assertEqual(contact.name, {'en': 'MEDS Coordinator', 'fr': 'Coordinateur de SDMM'})
                self.assertEqual(contact.email, {'en': 'coordinator@fake.com'})
                self.assertIsNone(contact.id_code)
            with self.subTest(msg='contributing institution'):
                 self.assertIn('C-PROOF', resps)
                 role, contact = resps['C-PROOF']
                 self.assertIsInstance(contact, Organization)
                 self.assertEqual(contact.name, {'en': 'C-PROOF'})
                 self.assertEqual(role, 'contributor')
                 self.assertIsNone(contact.email)
                 self.assertIsNone(contact.id_code)
        self.assertEqual(md.geospatial_vertical_min, 5)
        self.assertEqual(md.geospatial_vertical_max, 8)
        body = md.build_request_body()
        self.assertIsInstance(body, dict)
        self.assertDictSimilar({
            'guid': '12345',
            'authority': None,
            'profiles': ['cnodc'],
            'org_name': None,
            'display_names': {'en': 'Hello', 'fr': 'Bonjour'},
            'users': [],
            'metadata': {
                'variables': [
                    {
                        '_guid': 'temp',
                        'source_name': 'temp',
                        '_display_names': {'und': 'temp'},
                        'source_data_type': 'double',
                        'dimensions': 'N_COUNT',
                        'ioos_category': 'Temperature',
                        'actual_min': 1.0,
                        'actual_max': 4.0,
                        'units': 'degrees_C',
                        'cnodc_name': 'Temperature',
                    },
                    {
                        '_guid': 'depth',
                        'source_name': 'depth',
                        '_display_names': {'und': 'depth'},
                        'source_data_type': 'double',
                        'dimensions': 'N_COUNT',
                        'axis': 'Z',
                        'actual_min': 5.0,
                        'actual_max': 8.0,
                        'units': 'm',
                        'positive': 'down',
                    }
                ],
                'cioos_eovs': ['subSurfaceTemperature'],
                'geospatial_vertical_min': 5,
                'geospatial_vertical_max': 8,
                'project': 'Project',
                'program': 'Program',
                'title': {'en': 'Hello', 'fr': 'Bonjour'},
                'conventions': 'hello,world,shenanigans',
                'data_locale': {'_guid': 'canadian_english_utf8'},
                'metadata_locale': {'_guid': 'canadian_english_utf8'},
                'metadata_extra_locales': [{'_guid': 'canadian_french_utf8'}],
                'data_extra_locales': [{'_guid': 'canadian_french_utf8'}],
                'institution': 'dfo',
                'feature_type': 'Profile',
                'spatial_representation_type': 'textTable',
                'processing_level': 'raw',
                'geospatial_bounds': 'POINT(1 2)',
                'processing_description': {'en': 'i did stuff'},
                'processing_environment': {'en': 'my computer'},
                'acknowledgement': {'en': 'my computer did stuff'},
                'comment': {'en': 'oh no'},
                'references': {'en': 'yes i have them'},
                'source': {'en': 'i made it up'},
                'summary': {'en': 'what i did but briefly'},
                'purpose': {'en': 'why i made this'},
                'standard_name_vocab': 'CF-1.12',
                'date_issued': '2015-01-02T00:00:00',
                'date_created': '2016-01-02T00:00:00',
                'date_modified': '2017-01-02T00:00:00',
                'resource_maintenance_frequency': 'daily',
                'metadata_maintenance_frequency': 'asNeeded',
                'status': 'onGoing',
                'topic_category': 'oceans',
                'goc_audience': ['scientists', 'parents'],
                'goc_subject': 'oceanography',
                'goc_publication_place': ['ontario_-_ottawa', 'british_columbia_-_nanaimo', 'newfoundland_and_labrador_-_division_no._1', 'ontario_-_halton'],
                'dataset_id_code': '10.1.2.3/456',
                'dataset_id_system': {'_guid': 'DOI'},
                'temporal_resolution': {'seconds': 60},
                'geospatial_bounds_crs': {'_guid': 'wgs84'},
                'geospatial_bounds_vertical_crs': {'_guid': 'msl_depth'},
                'info_link': {
                    'url': {'en': 'https://dfo-mpo.gc.ca'},
                    'protocol': 'https',
                },
                'alt_metadata': [
                    {
                        'resource': {
                            'url': {'en': 'https://cnodc/full_metadata.xml'},
                            'protocol': 'https',
                            'goc_formats': ['XML'],
                            'function': 'completeMetadata',
                            'goc_content_type': 'support_doc',
                        }
                    }
                ],
                'responsibles': [
                    {
                        'role': 'originator',
                        'contact': {
                            '_guid': 'erin@fake.com',
                            'individual_name': 'Erin Turnbull',
                            'id_code': '12345',
                            'id_system': {'_guid': 'ORCID'},
                            'email': {'en': 'erin@fake.com'},
                        }
                    },
                    {
                        'role': 'publisher',
                        'contact': {
                            '_guid': 'meds@fake.com',
                            'organization_name': {'en': 'Marine Environmental Data Section', 'fr': 'SDMM'},
                            'id_code': '234567',
                            'id_system': {'_guid': 'ROR'},
                            'web_page': {
                                'url': {'en': 'https://meds.com'},
                                'function': 'information',
                                'protocol': 'https'
                            },
                            'email': {'en': 'meds@fake.com', 'fr': 'sdmm@fake.com'}
                        }
                    },{
                        'role': 'contributor',
                        'contact': {
                            '_guid': 'anh@fake.com',
                            'individual_name': 'Anh Tran',
                            'id_code': '123',
                            'id_system': {'_guid': 'ORCID'},
                            'email': {'en': 'anh@fake.com'},
                        }
                    },{
                        'role': 'editor',
                        'contact': {
                            '_guid': 'jenny@fake.com',
                            'individual_name': 'Jenny Chiu',
                            'id_code': '456',
                            'id_system': {'_guid': 'ORCID'},
                            'email': {'en': 'jenny@fake.com'},
                        }
                    },{
                        'role': 'funder',
                        'contact': {
                            '_guid': 'bio@fake.com',
                            'organization_name': {'en': 'BIO'},
                            'id_code': '7890',
                            'id_system': {'_guid': 'ROR'},
                            'email': {'en': 'bio@fake.com'},
                        }
                    },{
                        'role': 'mediator',
                        'contact': {
                            '_guid': 'coordinator@fake.com',
                            'position_name': {'en': 'MEDS Coordinator', 'fr': 'Coordinateur de SDMM'},
                            'email': {'en': 'coordinator@fake.com'},
                        }
                    },{
                        'role': 'contributor',
                        'contact': {
                            'organization_name': {'en': 'C-PROOF'}
                        }
                    }
                ],
                'publisher': {
                    '_guid': 'cnodc'
                },
                'metadata_owner': {
                    '_guid': 'cnodc'
                },
                'metadata_standards': [
                    {'_guid': 'metadata_standard_iso19115'},
                    {'_guid': 'metadata_standard_iso19115-1'}
                ],
                'metadata_profiles': [
                    {'_guid': 'metadata_profile_cioos'}
                ],
                'licenses': [
                    {'_guid': 'open_government_license'},
                    {'_guid': 'unclassified_data'},
                ],
                'metadata_licenses': [
                    {'_guid': 'open_government_license'},
                    {'_guid': 'unclassified_data'},
                ],
                'goc_collection_type': 'geogratis',
                'goc_publisher': {
                    '_guid': 'meds'
                },
            },
            'activation_workflow': 'cnodc_activation',
            'publication_workflow': 'cnodc_publish',
            'security_level': 'unclassified',
        }, body)

    def test_fresh_cf_names(self):
        md = DatasetMetadata()
        self.assertIsInstance(md.cf_standard_names, list)

    def test_set_bad_time_res_from_iso(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.setncattr('time_coverage_resolution', 'P1W2D')
            md = DatasetMetadata()
            with self.assertLogs('cnodc.dmd.metadata', 'ERROR'):
                md.set_from_netcdf_file(ds, 'en')
            self.assertNotIn('temporal_resolution', md._metadata)

    def test_set_from_french_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            attrs = {
                'default_locale': 'fr-CA',
                'locales': '_en: en-CA',
                'title': 'Bonjour',
                'title_en': 'Hello',
            }
            for attr in attrs:
                ds.setncattr(attr, attrs[attr])
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertEqual(md.title, {'en': 'Hello', 'fr': 'Bonjour'})
            self.assertIs(md.primary_metadata_locale, Locale.CanadianFrench)
            self.assertIs(md.primary_data_locale, Locale.CanadianFrench)

    def test_set_from_default_french_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            attrs = {
                'locales': '_en: en-CA',
                'title': 'Bonjour',
                'title_en': 'Hello',
            }
            for attr in attrs:
                ds.setncattr(attr, attrs[attr])
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds, 'fr')
            self.assertEqual(md.title, {'en': 'Hello', 'fr': 'Bonjour'})
            self.assertIs(md.primary_metadata_locale, Locale.CanadianFrench)
            self.assertIs(md.primary_data_locale, Locale.CanadianFrench)

    def test_set_from_no_depths_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths.valid_min = 0
            depths.valid_max = 20
            depths[:] = [math.nan, math.nan, math.nan, math.nan]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertNotIn('cioos_eovs', md._metadata)

    def test_set_from_no_depths_netcdf_file_empty(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths.valid_min = 0
            depths.valid_max = 20
            depths[:] = []
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = []
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertNotIn('cioos_eovs', md._metadata)

    def test_set_from_zero_depths_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths.valid_min = 0
            depths.valid_max = 20
            depths[:] = [0, 0, 0, 0]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertEqual(md._metadata['cioos_eovs'], ['seaSurfaceTemperature'])

    def test_set_from_surface_to_depth_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths.valid_min = 0
            depths.valid_max = 20
            depths[:] = [0, 5, 10, 15]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertListSimilar(md._metadata['cioos_eovs'], ['seaSurfaceTemperature', 'subSurfaceTemperature'])

    def test_set_from_depths_netcdf_file_up(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'up'
            depths.valid_min = -20
            depths.valid_max = 0
            depths[:] = [-5, -10, -15, -17]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertEqual(md._metadata['cioos_eovs'], ['subSurfaceTemperature'])

    def test_set_from_surface_and_depths_netcdf_file_up(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'up'
            depths.valid_min = -20
            depths.valid_max = 0
            depths[:] = [-5, -10, -15, 0]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertListSimilar(md._metadata['cioos_eovs'], ['seaSurfaceTemperature', 'subSurfaceTemperature'])

    def test_set_from_surface_only_netcdf_file_up(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'up'
            depths.valid_min = -20
            depths.valid_max = 0
            depths[:] = [0, 0, 0, 0]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            md = DatasetMetadata()
            md.set_from_netcdf_file(ds)
            self.assertListSimilar(md._metadata['cioos_eovs'], ['seaSurfaceTemperature'])

    def test_bad_org_id_vocab(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'institution', 'DOI')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._children['contact'].ror)

    def test_bad_individual_id_vocab(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'individual', 'ROR')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._children['contact'].orcid)
            self.assertEqual(md.responsibles[0]._children['contact'].guid, 'hello@hello')

    def test_bad_any_id_for_position(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'position', None)
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._children['contact'].id_code)

    def test_name_from_und(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({'und': 'me'}, '', '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._children['contact'].name, 'me')

    def test_name_from_fr(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({'fr': 'me'}, '', '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._children['contact'].name, 'me')

    def test_name_from_blank(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({}, '', '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertIsNone(md.responsibles[0]._children['contact'].name)

    def test_email_from_und(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {'und': 'hello@hello.com'}, '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._children['contact'].guid, 'hello@hello.com')

    def test_email_from_fr(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {'fr': 'hello@hello.com'}, '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._children['contact'].guid, 'hello@hello.com')

    def test_email_from_blank(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {}, '', None, None, 'editor', 'individual', 'https://orcid.org')
        self.assertEqual(len(md.responsibles), 1)
        self.assertIsNone(md.responsibles[0]._children['contact'].guid)

    def test_no_role(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, None, 'institution', 'DOI')
            self.assertEqual(len(md.responsibles), 0)

    def test_single_eov(self):
        md = DatasetMetadata()
        v = Variable()
        v.cnodc_name = 'AmmoniaMolar'
        v.source_name = 'foo'
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.DissolvedNutrients)
        self.assertEqual(md._metadata['cioos_eovs'], ['nutrients'])

    def test_no_ioos_category(self):
        md = DatasetMetadata()
        v = Variable()
        v.cnodc_name = 'CreationTime'
        v.source_name = 'foo'
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)