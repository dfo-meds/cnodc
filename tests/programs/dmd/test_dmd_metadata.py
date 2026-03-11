import datetime

import netCDF4

from cnodc.programs.dmd.metadata import Encoding, Axis, NetCDFDataType, CoverageContentType, CommonDataModelType, \
    Common, CoordinateReferenceSystem, ContactRole, EntityRef, Variable, Direction, TimePrecision, TimeZone, Calendar, \
    IOOSCategory, CFVariableRole, ERDDAPVariableRole, get_bilingual_attribute, NumericTimeUnits, QuickWebPage, \
    ResourcePurpose, MaintenanceRecord, MaintenanceScope, ResourceType, Resource, GCContentType, GCLanguage, \
    GCContentFormat, Individual, TelephoneType, Country, IDSystem, Organization, Position, Citation, \
    GeneralUseConstraint, LegalConstraint, RestrictionCode, SecurityConstraint, ClassificationCode, ERDDAPServer, \
    Thesaurus, KeywordType, Keyword, DistributionChannel, DatasetMetadata, SpatialRepresentation, DistanceUnit, \
    AngularUnit, EssentialOceanVariable, StandardName, ERDDAPDatasetType, GCCollectionType, GCAudience, GCPlace, \
    GCSubject, TopicCategory, MaintenanceFrequency, StatusCode, GCPublisher
from core import BaseTestCase


class TestDMDMetadataBasics(BaseTestCase):

    def test_encoding(self):
        self.assertIs(Encoding.from_string('utf-8'), Encoding.UTF8)
        self.assertIs(Encoding.from_string('utf8'), Encoding.UTF8)
        self.assertIs(Encoding.from_string('utf16'), Encoding.UTF16)
        self.assertEqual(Encoding.from_string('iso-8859-1'), Encoding.ISO_8859_1)

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
        self.assertEqual(obj.build_request_body(), {
            '_guid': '12345',
            '_display_name': {'und': 'hello'},
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
