import datetime
import math

import netCDF4

from pipeman.programs.dmd.metadata import Encoding, Axis, NetCDFDataType, CoverageContentType, CommonDataModelType, \
    Common, CoordinateReferenceSystem, ContactRole, EntityRef, Variable, Direction, TimePrecision, TimeZone, Calendar, \
    IOOSCategory, CFVariableRole, ERDDAPVariableRole, get_bilingual_attribute, NumericTimeUnits, QuickWebPage, \
    ResourcePurpose, MaintenanceRecord, MaintenanceScope, ResourceType, Resource, GCContentType, GCLanguage, \
    GCContentFormat, Individual, TelephoneType, Country, IDSystem, Organization, Position, Citation, \
    GeneralUseConstraint, LegalConstraint, RestrictionCode, SecurityConstraint, ClassificationCode, ERDDAPServer, \
    Thesaurus, KeywordType, Keyword, DistributionChannel, DatasetMetadata, SpatialRepresentation, DistanceUnit, \
    AngularUnit, EssentialOceanVariable, ERDDAPDatasetType, GCCollectionType, GCAudience, GCPlace, \
    GCSubject, TopicCategory, MaintenanceFrequency, StatusCode, GCPublisher, Locale, _Contact, TelephoneNumber, \
    SpatialResolution, TemporalResolution
from medsutil import json
from tests.helpers.base_test_case import BaseTestCase


class TestDMDMetadataBasics(BaseTestCase):

    def test_good_enum_values(self):
        with netCDF4.Dataset("inmemory", "w", diskless=True) as ds:
            str_var = ds.createVariable('test', str)
            enum_values = {
                Encoding: {
                    Encoding.UTF8: ('utf-8', 'utf8'),
                    Encoding.UTF16: ('utf-16', 'utf16'),
                    Encoding.ISO_8859_1: ('iso-8859-1',)
                },
                IOOSCategory: {
                    IOOSCategory.DissolvedNutrients: ('DissolvedNutrients', 'dissolved nutrients'),
                },
                Axis: {
                    Axis.Time: ('T', 't'),
                    Axis.Latitude: ('Y', 'y'),
                    Axis.Longitude: ('X', 'x'),
                    Axis.Depth: ('Z', 'z'),
                },
                NetCDFDataType: {
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
                },
                CoverageContentType: {
                    CoverageContentType.Coordinate: ('coordinate', ),
                },
                CommonDataModelType: {
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
                    CommonDataModelType.Other: ["Other", "other"],
                },
                CoordinateReferenceSystem: {
                    CoordinateReferenceSystem.NAD27: ['4267', 4267, 'EPSG:4267', 'EPSG: 4267'],
                    CoordinateReferenceSystem.WGS84: ['4326', 4326, 'EPSG:4326', 'EPSG: 4326'],
                    CoordinateReferenceSystem.MSL_Depth: ['5715', 5715, 'epsg:5715', 'epsg: 5715'],
                    CoordinateReferenceSystem.MSL_Heights: ['5714', 5714, 'epsg:5714', 'epsg :5714'],
                    CoordinateReferenceSystem.Instant_Depth: ['5831', 5831, 'epsg:5831', 'EPSG : 5831'],
                    CoordinateReferenceSystem.Instant_Heights: ['5829', 5829, 'EPSG:5829'],
                    CoordinateReferenceSystem.Gregorian: ['gregorian', 'standard']
                },
                GCPlace: {
                    GCPlace.Canada: ['canada', 'CANADA', 'Canada'],
                    GCPlace.Burlington: ['ontario - halton', 'Halton, ON', 'Halton, Ontario', 'Ontario -  Halton', 'ontario_-_halton'],
                    GCPlace.Ottawa: ['Ottawa, ON', 'Ottawa, Ontario', 'Ontario - Ottawa'],
                    GCPlace.Dartmouth: ['Halifax, NS', 'Halifax, Nova Scotia', 'Nova  Scotia  - Halifax '],
                    GCPlace.Moncton: ['Westmorland, NS', '  Westmorland,  Nova Scotia', '  westmorland,nova scotia'],
                },
                ContactRole: {
                    ContactRole.Stakeholder: ('CONT0001', 'CONT0005', 'CONT0007'),
                    ContactRole.PrincipalInvestigator: ('CONT0004',),
                    ContactRole.Owner: ('CONT0002',),
                    ContactRole.Originator: ('CONT0003',),
                    ContactRole.Processor: ('CONT0006',),
                    ContactRole.User: ('user',),
                },
                Calendar: {
                    Calendar.Standard: ('standard', 'gregorian', 'GREGORIAN'),
                    Calendar.Julian: ('julian', 'JULIAN')
                },
            }
            for enc_cls in enum_values:
                for result in enum_values[enc_cls]:
                    for value in enum_values[enc_cls][result]:
                        with self.subTest(cls=enc_cls.__name__, value=value):
                            self.assertIs(enc_cls(value), result)

    def test_bad_enum_values(self):
        enum_values = {
            Encoding: ('wtf',),
            IOOSCategory: ('definitely not and never will be an ioos category',),
            Axis: ('lat', 'lon', 'time', 'depth', 'altitude', 'A', 'D', 'd', 'a', 'TT'),
            CoordinateReferenceSystem: ('not a crs', 'EPSG:993132'),
        }
        for enc_cls in enum_values:
            for value in enum_values[enc_cls]:
                with self.subTest(cls=enc_cls.__name__, value=value):
                    with self.assertRaises(ValueError):
                        _ = enc_cls(value)

    def test_get_bilingual_attribute(self):
        locale_map = {'_en': 'en', '_fr': 'fr', '': 'en'}
        self.assertEqual({'en': 'foo'}, get_bilingual_attribute({'bar': 'foo'}, 'bar', locale_map))
        self.assertEqual({'en': 'foo', 'fr': 'le foo'},
                         get_bilingual_attribute({'bar': 'foo', 'bar_fr': 'le foo'}, 'bar', locale_map))
        self.assertEqual({}, get_bilingual_attribute({'bar': 'foo', 'bar_fr': 'le foo'}, 'bar2', locale_map))


class TestCoreEntityRef(BaseTestCase):

    def test_build_request_body(self):
        obj = EntityRef()
        obj.guid = '12345'
        obj.display_name = 'hello'
        sub_ref = EntityRef()
        sub_ref.guid = '23456'
        map_ = obj.export()
        DatasetMetadata.clean_for_request_body(map_)
        self.assertDictSimilar(map_, {
            '_guid': '12345',
            '_display_names': {'und': 'hello'},
        })


class TestVariable(BaseTestCase):

    def test_set_via_constructor(self):
        # TODO: move this into its own test case?
        var = Variable(
            guid='TEMP',
            display_name='Temperature',
            source_name='TEMP',
            source_data_type='f8',
            cnodc_name='Temperature',
            axis='T',
            actual_min=5,
            actual_max=10,
            positive=Direction.Up,
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
            altitude_proxy=False
        )
        self.assertEqual(var.guid, 'TEMP')
        self.assertEqual(var.display_name, {'und': 'Temperature'})
        self.assertEqual(var.source_name, 'TEMP')
        self.assertEqual(var.cnodc_name, 'Temperature')
        self.assertEqual(var.actual_min, 5)
        self.assertEqual(var.actual_max, 10)
        self.assertEqual(var.destination_name, 'temperature')
        self.assertEqual(var.dimensions, {'a', 'b'})
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
        self.assertIs(var.positive, Direction.Up)
        self.assertIs(var.encoding, Encoding.UTF8)
        self.assertIs(var.source_data_type, NetCDFDataType.Double)
        self.assertIs(var.axis, Axis.Time)

    def test_set_to_none(self):
        var = Variable(
            calendar=None
        )
        self.assertIsNone(var.calendar)

    def test_additional_properties_overwrite(self):
        var = Variable()
        var.additional_properties = {
            'five': 5,
        }
        self.assertIn('five', var.additional_properties)
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
        # TODO: move this into its own test case
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
            self.assertIs(var.positive, Direction.Down)
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
        # TODO: move this into its own test case?
        mr = MaintenanceRecord(
            date=datetime.date(2015, 1, 2),
            notes='did stuff',
            scope=MaintenanceScope.Dataset
        )
        self.assertIs(mr.scope, MaintenanceScope.Dataset)
        self.assertEqual(mr.date, datetime.date(2015, 1, 2))
        self.assertEqual(mr.notes, {'und': 'did stuff'})


class TestQuickWebPage(BaseTestCase):

    def test_basics(self):
        # TODO: move this into its own test case?
        wp = QuickWebPage(
            name='hello',
            url='http://google.com/test',
            description={
                'en': 'foo',
                'fr': 'bar'
            },
            purpose=ResourcePurpose.Information
        )
        self.assertEqual(wp.name, {'und': 'hello'})
        self.assertEqual(wp.url, {'und': 'http://google.com/test'})
        self.assertEqual(wp.description, {'en': 'foo', 'fr': 'bar'})
        self.assertEqual(wp.resource_type, ResourceType.WebPage)
        self.assertEqual(wp.purpose, ResourcePurpose.Information)

    def test_url(self):
        wp = QuickWebPage()
        wp.url = 'https://google.com/test'
        self.assertEqual(wp.url, {'und': 'https://google.com/test'})
        self.assertIs(wp.resource_type, ResourceType.SecureWebPage)

    def test_insecure_url(self):
        wp = QuickWebPage()
        wp.url = 'http://google.com/test'
        self.assertIs(wp.resource_type, ResourceType.WebPage)

    def test_ftp_url(self):
        wp = QuickWebPage()
        wp.url = 'ftp://google.com/test'
        self.assertIs(wp.resource_type, ResourceType.FTP)

    def test_ftps_url(self):
        wp = QuickWebPage()
        wp.url = 'ftps://google.com/test'
        self.assertIs(wp.resource_type, ResourceType.FTP)

    def test_ftpse_url(self):
        wp = QuickWebPage()
        wp.url = 'ftpse://google.com/test'
        self.assertIs(wp.resource_type, ResourceType.FTP)

    def test_git_url(self):
        wp = QuickWebPage()
        wp.url = 'git://google.com/test'
        self.assertIs(wp.resource_type, ResourceType.Git)

    def test_file_url(self):
        wp = QuickWebPage()
        wp.url = 'file:///google.com/test'
        self.assertIs(wp.resource_type, ResourceType.File)

    def test_other_url(self):
        wp = QuickWebPage()
        wp.url = 'azure://google.com/test'
        self.assertIsNone(wp.resource_type)

    def test_direct_set(self):
        wp = QuickWebPage()
        wp.resource_type = ResourceType.ERDDAPGrid
        wp.url = 'https://google.com/test'
        self.assertEqual(wp.resource_type, ResourceType.ERDDAPGrid)

    def test_autodetect_resource_type(self):
        self.assertIsNone(QuickWebPage.autodetect_resource_type(None))
        self.assertEqual(ResourceType.WebPage, QuickWebPage.autodetect_resource_type({'und': 'http://www.google.com'}))
        self.assertEqual(ResourceType.SecureWebPage, QuickWebPage.autodetect_resource_type({'en': 'https://www.google.com'}))
        self.assertEqual(ResourceType.FTP, QuickWebPage.autodetect_resource_type({'fr': 'ftp://www.google.com'}))


class TestResource(BaseTestCase):

    def test_basics(self):
        # TODO: move this into its own test case?
        res = Resource(
            name='hello',
            url='http://google.com/test',
            description={
                'en': 'foo',
                'fr': 'bar'
            },
            purpose=ResourcePurpose.Information,
            additional_request_info='dont',
            additional_app_info='browser',
            goc_content_type=GCContentType.Dataset,
            goc_languages=GCLanguage.French
        )
        self.assertEqual(res.name, {'und': 'hello'})
        self.assertEqual(res.url, {'und': 'http://google.com/test'})
        self.assertEqual(res.description, {'en': 'foo', 'fr': 'bar'})
        self.assertEqual(res.resource_type, ResourceType.WebPage)
        self.assertEqual(res.additional_app_info, {'und': 'browser'})
        self.assertEqual(res.additional_request_info, {'und': 'dont'})
        self.assertIs(res.goc_format, GCContentFormat.Hypertext)
        self.assertIs(res.goc_content_type, GCContentType.Dataset)
        self.assertIs(res.goc_languages, GCLanguage.French)
        self.assertIs(res.purpose, ResourcePurpose.Information)

    def test_set_non_auto_gc_content_format(self):
        res = Resource()
        res.gc_content_format = GCContentFormat.DocumentDOC
        self.assertIs(res.gc_content_format, GCContentFormat.DocumentDOC)

    def test_autodetect_gc_content_format(self):
        # TODO: build as sub-tests
        self.assertIsNone(Resource.autodetect_gc_content_format(None))
        self.assertEqual(GCContentFormat.ArchiveTARGZIP, Resource.autodetect_gc_content_format({'und': 'https://domain.com/file.tar.gz'}))
        self.assertEqual(GCContentFormat.ImageBMP, Resource.autodetect_gc_content_format({'en': 'https://domain.com/file.bmp'}))
        self.assertEqual(GCContentFormat.DocumentPDF, Resource.autodetect_gc_content_format({'fr': 'https://domain.com/file.PDF'}))
        self.assertEqual(GCContentFormat.DocumentDOCX, Resource.autodetect_gc_content_format({'fr': 'https://domain.com\\file.DOCX'}))
        self.assertIsNone(Resource.autodetect_gc_content_format({'fr': 'file://domain.com/file/whatever'}))


class TestIndividual(BaseTestCase):

    def test_basics(self):
        # TODO: move this into its own test case?
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
        self.assertEqual(c._data['individual_name'], 'Joanne Smith')

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
        self.assertEqual(1, len(c._data['web_resources']))
        self.assertIs(res, c._data['web_resources'][0])

    def test_add_telephone(self):
        c = Individual()
        num = TelephoneNumber(
            phone_number_type=TelephoneType.Cell,
            phone_number='613-555-5555'
        )
        c.phone_numbers.append(num)
        self.assertIs(c.phone_numbers[0], num)

    def test_set_address(self):
        c = Individual()
        c.address = {'en': '555 Fake Road', 'fr': '555 rue Fake'}
        c.city = 'Ottawa'
        c.province = 'Ontario'
        c.country = Country.Canada
        c.postal_code = 'H0H0H0'
        self.assertEqual(c._data['delivery_point'], {'en': '555 Fake Road', 'fr': '555 rue Fake'})
        self.assertEqual(c._data['city'], 'Ottawa')
        self.assertEqual(c._data['admin_area'], {'und': 'Ontario'})
        self.assertEqual(c._data['country'], Country.Canada)
        self.assertEqual(c._data['postal_code'], 'H0H0H0')

    def test_set_web_page(self):
        c = Individual()
        c.web_page = QuickWebPage(
            url='https://www.google.com',
            name='hello world',
            description='this is my hello world web page',
            purpose=ResourcePurpose.Information
        )
        self.assertEqual(c._data['web_page'].url, {'und': 'https://www.google.com'})
        self.assertEqual(c._data['web_page'].name, {'und': 'hello world'})
        self.assertEqual(c._data['web_page'].description, {'und': 'this is my hello world web page'})
        self.assertEqual(c._data['web_page'].purpose, ResourcePurpose.Information)
        self.assertEqual(c._data['web_page'].resource_type, ResourceType.SecureWebPage)


class TestOrganization(BaseTestCase):

    def test_name(self):
        o = Organization()
        o.name = 'foobar'
        self.assertEqual(o.name, {'und': 'foobar'})
        self.assertEqual(o._data['organization_name'], {'und': 'foobar'})

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
        self.assertEqual(1, len(o._data['individuals']))
        self.assertIs(x, o._data['individuals'][0])


class TestPosition(BaseTestCase):

    def test_name(self):
        p = Position(name='foobar')
        self.assertEqual(p.name, {'und': 'foobar'})
        self.assertEqual(p._data['position_name'], {'und': 'foobar'})


class TestCitation(BaseTestCase):

    def test_basics(self):
        # TODO: move this into its own test case?
        res = Resource(url='https://www.google.com/')
        cit = Citation(
            title='title',
            alt_title='alt_title',
            details='details',
            edition='edition',
            publication_date=datetime.date(2015, 1, 2),
            revision_date='2015-01-03',
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
        self.assertEqual(cit.publication_date, datetime.date(2015, 1, 2))
        self.assertEqual(cit.revision_date, datetime.date(2015, 1, 3))
        self.assertEqual(cit.creation_date, datetime.date(2015, 1, 1))
        self.assertEqual(cit.isbn, '12345')
        self.assertEqual(cit.issn, '12345X')
        self.assertIs(cit.resource, res)
        self.assertEqual(cit.id_code, '12345')
        self.assertEqual(cit.id_description, {'und': 'code for my luggage'})
        self.assertIs(cit.id_system, IDSystem.DOI)

    def test_resource_none_to_dict(self):
        cit = Citation()
        cit.title = 'foobar'
        map_ = cit.export()
        DatasetMetadata.clean_for_request_body(map_)
        self.assertDictSimilar(map_, {
            'title': {'und': 'foobar'}
        })

    def test_resource_something_to_dict(self):
        cit = Citation()
        cit.title = 'foobar'
        cit.resource = Resource(url='http://foobar.com')
        map_ = cit.export()
        DatasetMetadata.clean_for_request_body(map_)
        self.assertDictSimilar({
            'title': {'und': 'foobar'},
            'resource': {
                'url': {'und': 'http://foobar.com'},
                'goc_format': 'HTML',
                'protocol': ResourceType.WebPage.value,
            }
        }, map_)

    def test_responsibles(self):
        cit = Citation()
        contact = Individual(name='Oscar Wilde')
        cit.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(cit._data['responsibles']))
        self.assertIs(contact, cit._data['responsibles'][0]._data['contact'])
        self.assertEqual(ContactRole.Owner, cit._data['responsibles'][0]._data['role'])


class TestGeneralUseConstraint(BaseTestCase):

    def test_basics(self):
        con = GeneralUseConstraint(
            description='hello',
            plain_text='what'
        )
        self.assertEqual(con.description, {'und': 'hello'})
        self.assertEqual(con.plain_text, {'und': 'what'})

    def test_references(self):
        ref = Citation()
        con = GeneralUseConstraint()
        con.citations.append(ref)
        self.assertIs(ref, con._data['reference'][0])

    def test_responsibles(self):
        con = GeneralUseConstraint()
        contact = Individual(name='Oscar Wilde')
        con.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(con._data['responsibles']))
        self.assertIs(contact, con._data['responsibles'][0]._data['contact'])
        self.assertEqual(ContactRole.Owner, con._data['responsibles'][0]._data['role'])


class TestLegalConstaint(BaseTestCase):

    def test_basics(self):
        con = LegalConstraint(
            access_constraints=[RestrictionCode.Restricted],
            use_constraints=[RestrictionCode.License, RestrictionCode.Confidential],
            other_constraints='oh no'
        )
        self.assertEqual(con.other_constraints, {'und': 'oh no'})
        self.assertEqual(con.access_constraints, {RestrictionCode.Restricted})
        self.assertEqual(con.use_constraints, {RestrictionCode.License, RestrictionCode.Confidential})


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
        self.assertEqual(1, len(server._data['responsibles']))
        self.assertIs(contact, server._data['responsibles'][0]._data['contact'])
        self.assertEqual(ContactRole.Owner, server._data['responsibles'][0]._data['role'])


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
        self.assertIs(res, dc._data['links'][0])

    def test_responsibles(self):
        dc = DistributionChannel()
        contact = Individual(name='Oscar Wilde')
        dc.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(dc._data['responsibles']))
        self.assertIs(contact, dc._data['responsibles'][0]._data['contact'])
        self.assertEqual(ContactRole.Owner, dc._data['responsibles'][0]._data['role'])


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
        md.cf_standard_names.add('air_pressure')
        self.assertEqual(1, len(md.cf_standard_names))
        # no duplicates
        md.cf_standard_names.add('sea_water_temperature')
        self.assertEqual(2, len(md.cf_standard_names))
        md.cf_standard_names.add('sea_water_temperature')
        self.assertEqual(2, len(md.cf_standard_names))
        md.cf_standard_names.add('sea_water_practical_salinity')
        self.assertEqual(3, len(md.cf_standard_names))

    def test_set_spatial_res(self):
        md = DatasetMetadata()
        md.spatial_resolution = SpatialResolution(
            scale=30000,
            level_of_detail='stuff is visibile',
            horizontal_resolution=5,
            horizontal_units=DistanceUnit.Meters,
            vertical_resolution=10,
            vertical_units=DistanceUnit.Kilometers,
            angular_resolution=30,
            angular_units=AngularUnit.ArcMinutes
        )
        self.assertEqual(md._data['spatial_resolution']._data, {
            '_guid': None,
            '_display_names': None,
            'scale': 30000,
            'level_of_detail': {'und': 'stuff is visibile'},
            'distance': 5,
            'distance_units': DistanceUnit.Meters,
            'vertical': 10,
            'vertical_units': DistanceUnit.Kilometers,
            'angular': 30,
            'angular_units': AngularUnit.ArcMinutes
        })

    def test_set_time_res(self):
        md = DatasetMetadata()
        md.temporal_resolution = TemporalResolution(seconds=3.5)
        self.assertEqual(md.temporal_resolution.seconds, 3)
        self.assertIsNone(md.temporal_resolution.years)

    def test_time_resolution_from_iso_format(self):
        good_tests = {
            'P5W': {'days': 35},
            'P3Y': {'years': 3},
            'P4M': {'months': 4},
            'P5D': {'days': 5},
            'PT3H': {'hours': 3},
            'PT9M': {'minutes': 9},
            'PT1M30S': {'minutes': 1, 'seconds': 30},
            'P0000-00-00T01:30:00': {'hours': 1, 'minutes': 30},
        }
        bad_tests = [
            'P00-00-00T00:00:00',
            'P00Z',
            'P0000-00-00T00:00:0',
            'P5WT2H',
            '5W'
        ]
        md = DatasetMetadata()
        for test_val in good_tests:
            result = good_tests[test_val]
            with self.subTest(input_val=test_val):
                md.time_resolution = TemporalResolution.from_iso_format(test_val)
                self.assertIsNotNone(md.time_resolution)
                for key in ('years', 'months', 'days', 'hours', 'minutes', 'seconds'):
                    if key in result:
                        self.assertEqual(getattr(md.time_resolution, key), result[key])
                    else:
                        self.assertIsNone(getattr(md.time_resolution, key))
        for test_val in bad_tests:
            with self.subTest(input_val=test_val):
                with self.assertRaises(ValueError):
                    md.time_resolution = TemporalResolution.from_iso_format(test_val)

    def test_add_eov(self):
        md = DatasetMetadata()
        md.essential_ocean_variables.add(EssentialOceanVariable.OceanSound)
        self.assertIn(EssentialOceanVariable.OceanSound, md._data['cioos_eovs'])

    def test_set_erddap_info(self):
        md = DatasetMetadata()
        md.erddap_servers.append(ERDDAPServer(base_url='https://www.google.com/erddap/'))
        md.erddap_dataset_id = 'foobar'
        md.erddap_dataset_type = ERDDAPDatasetType.NetCDFGrid
        md.erddap_data_file_path = '/cloud_data/stuff'
        md.erddap_data_file_pattern = '*\\.nc'
        self.assertIn('erddap', md._data['_profiles'])
        self.assertEqual(1, len(md._data['erddap_servers']))
        self.assertEqual('*\\.nc', md._data['erddap_data_file_pattern'])
        self.assertEqual('/cloud_data/stuff', md._data['erddap_data_file_path'])
        self.assertEqual('foobar', md._data['erddap_dataset_id'])
        self.assertEqual(ERDDAPDatasetType.NetCDFGrid, md._data['erddap_dataset_type'])

    def test_set_meds_defaults(self):
        # TODO: move this into its own test case
        md = DatasetMetadata()
        md.set_meds_defaults()
        self.assertEqual(md.goc_collection, GCCollectionType.Geospatial)
        self.assertEqual(md.goc_audiences, {GCAudience.Scientists})
        self.assertEqual(md.goc_publication_places, {GCPlace.Ottawa})
        self.assertEqual(md.goc_subject, GCSubject.Oceanography)
        self.assertEqual(md.security_level, 'unclassified')
        self.assertEqual(md.cf_standard_name_vocab, 'CF 1.13')
        self.assertEqual(md.publication_workflow, 'cnodc_publish')
        self.assertEqual(md.activation_workflow, 'cnodc_activation')
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
        md.info_link = QuickWebPage(
            url='http://www.google.com',
            name='hello',
            description='world',
            purpose=ResourcePurpose.Information
        )
        self.assertEqual(md._data['info_link'].url, {'und': 'http://www.google.com'})
        self.assertEqual(md._data['info_link'].name, {'und': 'hello'})
        self.assertEqual(md._data['info_link'].description, {'und': 'world'})
        self.assertEqual(md._data['info_link'].purpose, ResourcePurpose.Information)
        self.assertEqual(md._data['info_link'].resource_type, ResourceType.WebPage)

    def test_update_additional_properties(self):
        md = DatasetMetadata()
        md.custom_metadata.update({'foo': 'bar', 'bar': None, 'hello': 'world'})
        self.assertEqual(md._data['custom_metadata'], {'foo': 'bar', 'hello': 'world', 'bar': None})
        md.custom_metadata.update({'foo': 'bar2', 'bar': 'yes'})
        self.assertEqual(md._data['custom_metadata'], {'foo': 'bar2', 'hello': 'world', 'bar': 'yes'})

    def test_responsibles(self):
        md = DatasetMetadata()
        contact = Individual(name='Oscar Wilde')
        md.add_contact(ContactRole.Owner, contact)
        self.assertEqual(1, len(md._data['responsibles']))
        self.assertIs(contact, md._data['responsibles'][0]._data['contact'])
        self.assertEqual(ContactRole.Owner, md._data['responsibles'][0]._data['role'])

    def test_add_dmd_user(self):
        md = DatasetMetadata()
        md.users.add('test')
        self.assertIn('test', md._data['_users'])

    def test_set_parent_org(self):
        md = DatasetMetadata()
        self.assertIsNone(md._data['_org_name'])
        md.organization_name = 'meds'
        self.assertEqual(md._data['_org_name'], 'meds')

    def test_add_variable_eov_surface(self):
        v = Variable()
        v.cnodc_name = 'parameters/Temperature'
        v.standard_name = 'seawater_temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['seaSurface'])
        self.assertIs(v.ioos_category, IOOSCategory.Temperature)
        self.assertIn('seawater_temperature', md.cf_standard_names)
        self.assertIn(EssentialOceanVariable.SurfaceTemperature, md._data['cioos_eovs'])
        self.assertNotIn(EssentialOceanVariable.SubSurfaceTemperature, md._data['cioos_eovs'])

    def test_add_variable_eov_subsurface(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['subSurface'])
        self.assertIs(v.ioos_category, IOOSCategory.Temperature)
        self.assertNotIn(EssentialOceanVariable.SurfaceTemperature, md._data['cioos_eovs'])
        self.assertIn(EssentialOceanVariable.SubSurfaceTemperature, md._data['cioos_eovs'])

    def test_add_variable_eov_both(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v, ['subSurface', 'seaSurface'])
        self.assertIn(EssentialOceanVariable.SurfaceTemperature, md._data['cioos_eovs'])
        self.assertIn(EssentialOceanVariable.SubSurfaceTemperature, md._data['cioos_eovs'])

    def test_add_variable_eov_none(self):
        v = Variable()
        v.cnodc_name = 'Temperature'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertEqual(len(md.essential_ocean_variables), 0)

    def test_add_variable_ioos_unknown(self):
        v = Variable()
        v.cnodc_name = 'Temp'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)
        self.assertEqual(len(md.essential_ocean_variables), 0)

    def test_add_variable_known_missing(self):
        v = Variable()
        v.cnodc_name = 'CNODCDuplicateId'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)
        self.assertEqual(len(md.essential_ocean_variables), 0)

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
        self.assertSameTime(md.time_coverage_start, datetime.datetime.fromisoformat('1950-01-11T00:00:00+00:00'))
        self.assertSameTime(md.time_coverage_end, datetime.datetime.fromisoformat('1950-01-21T00:00:00+00:00'))

    def test_set_time_from_var_seconds(self):
        v = Variable(axis='T')
        v.actual_min = 90
        v.actual_max = 600
        v.units = 'seconds since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertSameTime(md.time_coverage_start, datetime.datetime.fromisoformat('1950-01-01T00:01:30+00:00'))
        self.assertSameTime(md.time_coverage_end, datetime.datetime.fromisoformat('1950-01-01T00:10:00+00:00'))

    def test_set_time_from_var_minutes(self):
        v = Variable(axis='T')
        v.actual_min = 90
        v.actual_max = 630.5
        v.units = 'minutes since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertSameTime(md.time_coverage_start, datetime.datetime.fromisoformat('1950-01-01T01:30:00+00:00'))
        self.assertSameTime(md.time_coverage_end, datetime.datetime.fromisoformat('1950-01-01T10:30:30+00:00'))

    def test_set_time_from_var_hours(self):
        v = Variable(axis='T')
        v.actual_min = 48
        v.actual_max = 48 + 5 + (30 / 60) + (45/3600)
        v.units = 'hours since 1950-01-01T00:00:00+00:00'
        md = DatasetMetadata()
        md.add_variable(v)
        self.assertSameTime(md.time_coverage_start, datetime.datetime.fromisoformat('1950-01-03T00:00:00+00:00'))
        self.assertSameTime(md.time_coverage_end, datetime.datetime.fromisoformat('1950-01-03T05:30:45+00:00'))

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
        # TODO: move this into its own test case
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.createDimension('N_COUNT',)
            depths = ds.createVariable('depth', 'f8', ('N_COUNT',))
            depths.units = 'm'
            depths.axis = 'Z'
            depths.positive = 'down'
            depths[:] = [5.0,6.0,7.0,8.0]
            temps = ds.createVariable('temp', 'f8', ('N_COUNT',))
            temps.units = 'degrees_C'
            temps.cnodc_standard_name = 'Temperature'
            temps[:] = [1,2,3,4]
            attrs = {
                'locale_default': 'en-CA',
                'locale_others': '_fr: fr-CA',
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
                'contributing_institutions': 'C-PROOF',
            }
            for attr in attrs:
                ds.setncattr(attr, attrs[attr])
            md = DatasetMetadata()
            md.set_meds_defaults()
            md.set_from_netcdf_file(ds, 'en')
            md.is_ongoing = True
        self.assertIs(md.primary_metadata_locale, Locale.CanadianEnglish)
        self.assertIn(Locale.CanadianFrench, md.secondary_metadata_locales)
        self.assertIs(md.primary_data_locale, Locale.CanadianEnglish)
        self.assertIn(Locale.CanadianFrench, md.secondary_data_locales)
        self.assertEqual(md.title, {'en': 'Hello', 'fr': 'Bonjour'})
        self.assertEqual(md.display_name, {'en': 'Hello', 'fr': 'Bonjour'})
        self.assertEqual(md.program, 'Program')
        self.assertEqual(md.project, 'Project')
        self.assertEqual(md.institution, 'dfo')
        self.assertEqual(md.guid, '12345')
        self.assertEqual(md.processing_level, 'raw')
        self.assertEqual(md.geospatial_bounds, 'POINT(1 2)')
        self.assertEqual(md.conventions, {'hello','world','shenanigans'})
        self.assertEqual(md.processing_description, {'en': 'i did stuff'})
        self.assertEqual(md.processing_environment, {'en': 'my computer'})
        self.assertEqual(md.credit, {'en': 'my computer did stuff'})
        self.assertEqual(md.comment, {'en': 'oh no'})
        self.assertEqual(md.references, {'en': 'yes i have them'})
        self.assertEqual(md.source, {'en': 'i made it up'})
        self.assertEqual(md.abstract, {'en': 'what i did but briefly'})
        self.assertEqual(md.purpose, {'en': 'why i made this'})
        self.assertEqual(md.cf_standard_name_vocab, 'CF-1.12')
        self.assertEqual(md.date_issued, datetime.date(2015, 1, 2))
        self.assertEqual(md.date_created, datetime.date(2016, 1 ,2))
        self.assertEqual(md.date_modified, datetime.date(2017, 1, 2))
        self.assertIs(md.feature_type, CommonDataModelType.Profile)
        self.assertIs(md.data_maintenance_frequency, MaintenanceFrequency.Daily)
        self.assertIs(md.metadata_maintenance_frequency, MaintenanceFrequency.AsNeeded)
        self.assertIs(md.status, StatusCode.OnGoing)
        self.assertIs(md.topic_category, TopicCategory.Oceans)
        self.assertIs(md.goc_subject, GCSubject.Oceanography)
        self.assertSetEqual(md.goc_audiences, {GCAudience.Scientists, GCAudience.Parents})
        self.assertSetEqual(md.goc_publication_places, {GCPlace.Ottawa, GCPlace.Nanaimo, GCPlace.StJohns, GCPlace.Burlington})
        self.assertEqual({'en': 'https://dfo-mpo.gc.ca'}, md._data['info_link'].url)
        self.assertEqual('10.1.2.3/456', md.doi)
        self.assertEqual({'en': 'https://cnodc/full_metadata.xml'}, md.alt_metadata[0].resource.url)
        self.assertEqual(md.temporal_resolution.seconds, 60)
        self.assertIs(md.geospatial_crs, CoordinateReferenceSystem.WGS84)
        self.assertIs(md.geospatial_vertical_crs, CoordinateReferenceSystem.MSL_Depth)
        resps: dict[str, tuple[str, _Contact]] = {
            (resp._data['contact'].name if isinstance(resp._data['contact'].name, str) else resp._data['contact'].name['en']): (resp._data['role'], resp._data['contact'])
            for resp in md.responsibles
        }
        with self.subTest(msg='creator'):
            self.assertIn('Erin Turnbull', resps)
            creator_role, creator = resps['Erin Turnbull']
            self.assertIsInstance(creator, Individual)
            self.assertEqual(creator_role, ContactRole.Originator)
            self.assertEqual(creator.name, 'Erin Turnbull')
            self.assertEqual(creator.email, {'en': 'erin@fake.com'})
            self.assertEqual(creator.orcid, '12345')
        with self.subTest(msg='publisher'):
            self.assertIn('Marine Environmental Data Section', resps)
            pub_role, publisher = resps['Marine Environmental Data Section']
            self.assertEqual(pub_role, ContactRole.Publisher)
            self.assertIsInstance(publisher, Organization)
            self.assertEqual(publisher.name, {'en': 'Marine Environmental Data Section', 'fr': 'SDMM'})
            self.assertEqual(publisher.email, {'en': 'meds@fake.com', 'fr': 'sdmm@fake.com'})
            self.assertEqual(publisher.ror, '234567')
            self.assertEqual(publisher.web_page.url, {'en': 'https://meds.com'})
        with self.subTest(msg='contributor1'):
            self.assertIn('Anh Tran', resps)
            role, contact = resps['Anh Tran']
            self.assertIsInstance(contact, Individual)
            self.assertEqual(contact.name, 'Anh Tran')
            self.assertEqual(contact.email, {'en': 'anh@fake.com'})
            self.assertEqual(contact.orcid, '123')
            self.assertEqual(role, ContactRole.Contributor)
        with self.subTest(msg='contributor2'):
            self.assertIn('Jenny Chiu', resps)
            role, contact = resps['Jenny Chiu']
            self.assertIsInstance(contact, Individual)
            self.assertEqual(contact.name, 'Jenny Chiu')
            self.assertEqual(contact.email, {'en': 'jenny@fake.com'})
            self.assertEqual(contact.orcid, '456')
            self.assertEqual(role, ContactRole.Editor)
        with self.subTest(msg='contributor3'):
            self.assertIn('BIO', resps)
            role, contact = resps['BIO']
            self.assertIsInstance(contact, Organization)
            self.assertEqual(role, ContactRole.Funder)
            self.assertEqual(contact.name, {'en': 'BIO'})
            self.assertEqual(contact.email, {'en': 'bio@fake.com'})
            self.assertEqual(contact.ror, '7890')
        with self.subTest(msg='contributor4'):
            self.assertIn('MEDS Coordinator', resps)
            role, contact = resps['MEDS Coordinator']
            self.assertIsInstance(contact, Position)
            self.assertEqual(role, ContactRole.Mediator)
            self.assertEqual(contact.name, {'en': 'MEDS Coordinator', 'fr': 'Coordinateur de SDMM'})
            self.assertEqual(contact.email, {'en': 'coordinator@fake.com'})
            self.assertIsNone(contact.id_code)
        with self.subTest(msg='contributing institution'):
             self.assertIn('C-PROOF', resps)
             role, contact = resps['C-PROOF']
             self.assertIsInstance(contact, Organization)
             self.assertEqual(contact.name, {'en': 'C-PROOF'})
             self.assertEqual(role, ContactRole.Contributor)
             self.assertIsNone(contact.email)
             self.assertIsNone(contact.id_code)
        self.assertEqual(7, len(md.responsibles))
        self.assertEqual(md.geospatial_vertical_min, 5)
        self.assertEqual(md.geospatial_vertical_max, 8)
        body = json.clean_for_json(md.build_request_body())
        self.assertIsInstance(body, dict)
        with open(self.data_file_path('dmd_metadata/english_netcdf.json'), 'r') as h:
            compare_to = json.clean_for_json(json.load_dict(h.read()))
        self.assertDictSimilar(compare_to, body)
        x = json.dumps(body)
        self.assertIsInstance(x, str)

    def test_fresh_cf_names(self):
        md = DatasetMetadata()
        self.assertIsInstance(md.cf_standard_names, set)

    def test_set_bad_time_res_from_iso(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            ds.setncattr('time_coverage_resolution', 'P1W2D')
            md = DatasetMetadata()
            with self.assertLogs('cnodc.dmd.metadata', 'ERROR'):
                md.set_from_netcdf_file(ds, 'en')
            self.assertIsNone(md.temporal_resolution)

    def test_set_from_french_netcdf_file(self):
        with netCDF4.Dataset('inmemory.nc', 'r+', diskless=True) as ds:
            attrs = {
                'locale_default': 'fr-CA',
                'locale_others': '_en: en-CA',
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
                'locale_others': '_en: en-CA',
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
            self.assertEqual(0, len(md.essential_ocean_variables))

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
            self.assertEqual(0, len(md.essential_ocean_variables))

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
            self.assertEqual(md._data['cioos_eovs'], {EssentialOceanVariable.SurfaceTemperature})

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
            self.assertListEqualNoOrder(md._data['cioos_eovs'],{
                EssentialOceanVariable.SurfaceTemperature,
                EssentialOceanVariable.SubSurfaceTemperature
            })

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
            self.assertEqual(md._data['cioos_eovs'], {EssentialOceanVariable.SubSurfaceTemperature})

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
            self.assertListEqualNoOrder(md._data['cioos_eovs'], {
                EssentialOceanVariable.SurfaceTemperature,
                EssentialOceanVariable.SubSurfaceTemperature
            })

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
            self.assertListEqualNoOrder(md._data['cioos_eovs'], {EssentialOceanVariable.SurfaceTemperature})

    def test_bad_org_id_vocab(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'institution', 'DOI', '')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._data['contact'].ror)

    def test_bad_individual_id_vocab(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'individual', 'ROR', '')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._data['contact'].orcid)
            self.assertEqual(md.responsibles[0]._data['contact'].guid, 'hello@hello')

    def test_bad_any_id_for_position(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'position', None, '')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._data['contact'].id_code)

    def test_name_from_und(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({'und': 'me'}, '', '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._data['contact'].name, 'me')

    def test_name_from_fr(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({'fr': 'me'}, '', '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._data['contact'].name, 'me')

    def test_name_from_blank(self):
        md = DatasetMetadata()
        md._add_netcdf_contact({}, '', '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertIsNone(md.responsibles[0]._data['contact'].name)

    def test_email_from_und(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {'und': 'hello@hello.com'}, '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._data['contact'].guid, 'hello@hello.com')

    def test_email_from_fr(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {'fr': 'hello@hello.com'}, '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertEqual(md.responsibles[0]._data['contact'].guid, 'hello@hello.com')

    def test_email_from_blank(self):
        md = DatasetMetadata()
        md._add_netcdf_contact('me', {}, '', None, None, 'editor', 'individual', 'https://orcid.org', '')
        self.assertEqual(len(md.responsibles), 1)
        self.assertIsNone(md.responsibles[0]._data['contact'].guid)

    def test_no_role(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, None, 'institution', 'DOI', '')
            self.assertEqual(len(md.responsibles), 0)

    def test_single_eov(self):
        md = DatasetMetadata()
        v = Variable()
        v.cnodc_name = 'AmmoniaMolar'
        v.source_name = 'foo'
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.DissolvedNutrients)
        self.assertEqual(md._data['cioos_eovs'], {EssentialOceanVariable.Nutrients})

    def test_no_ioos_category(self):
        md = DatasetMetadata()
        v = Variable()
        v.cnodc_name = 'CreationTime'
        v.source_name = 'foo'
        md.add_variable(v)
        self.assertIs(v.ioos_category, IOOSCategory.Other)