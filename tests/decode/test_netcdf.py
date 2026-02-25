import netCDF4 as nc
import yaml

from cnodc.util import CNODCError
from cnodc.util.sanitize import str_to_netcdf, str_to_netcdf_vlen
from core import BaseTestCase
from cnodc.ocproc2.codecs.netcdf import NetCDFCommonMapper, NetCDFCommonDecoderError


class TestNetCDFCommonDecode(BaseTestCase):

    def test_bad_file(self):
        path = self.temp_dir / 'file.txt'
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, path, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_config_file_is_dir(self):
        path = self.temp_dir
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, path, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_no_dict(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, ['foo', 'bar'], 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_no_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds,{
                'data_maps': {}
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_no_map_from_config_dict(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'data_maps': {}
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_bad_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': None,
                'data_maps': {}
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_no_data_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {},
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_bad_data_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {},
                'data_maps': None,
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_bad_config_no_index_var(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {},
                'data_maps': {},
            }, 'test')
            with self.assertRaises(NetCDFCommonDecoderError):
                mapper._load_data()

    def test_good_basic_config(self):
        path = self.temp_dir / 'file.yaml'
        with open(path, 'w') as h:
            yaml.safe_dump({
            'ocproc2_map': {
                'var:test': {
                    'is_index': True,
                    'target': 'metadata/bar',
                },
                'attribute:test2': {
                    'target': 'metadata/bar2',
                    'data_map': 'bar'
                },
                'globalvar:test3': {
                    'target': 'metadata/bar3',
                },
                'test4': {
                    'target': 'metadata/bar4',
                },

            },
            'data_maps': {
                'bar': {
                    '1': 'one',
                    '2': 'two'
                }
            },
        }, h)
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, path, 'test')
            mapper._load_data()
            map_info = mapper._get_ocproc2_map()
            with self.subTest(msg="explicit variable"):
                self.assertIn('var:test', map_info['record'])
                self.assertNotIn('var:test', map_info['global'])

            with self.subTest(msg="explicit attribute"):
                self.assertIn('attribute:test2', map_info['global'])
                self.assertNotIn('attribute:test2', map_info['record'])
                self.assertEqual(map_info['global']['attribute:test2']['source'], 'test2')
                self.assertEqual(map_info['global']['attribute:test2']['mapping_type'], 'attribute')
                self.assertEqual(map_info['global']['attribute:test2']['data_map'], 'bar')

            with self.subTest(msg="explicit global variable"):
                self.assertNotIn('globalvar:test3', map_info['record'])
                self.assertIn('globalvar:test3', map_info['global'])
                self.assertEqual(map_info['global']['globalvar:test3']['source'], 'test3')
                self.assertEqual(map_info['global']['globalvar:test3']['mapping_type'], 'globalvar')

            with self.subTest(msg="implicit variable"):
                self.assertIn('test4', map_info['record'])
                self.assertNotIn('test4', map_info['global'])
                self.assertEqual(map_info['record']['test4']['source'], 'test4')
                self.assertEqual(map_info['record']['test4']['mapping_type'], 'var')

            expected = {
                'mapping_type': 'var',
                'source': 'test',
                'target': 'metadata/bar',
                'is_index': True
            }
            for key in expected:
                with self.subTest(expected_key=key):
                    self.assertIn(key, map_info['record']['var:test'])
                    self.assertEqual(expected[key], map_info['record']['var:test'][key])


    def test_good_basic_config_explicit(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    '1': {
                        'is_index': True,
                        'target': 'metadata/bar',
                        'source': 'test',
                    },
                    '2': {
                        'target': 'metadata/bar2',
                        'data_map': 'bar',
                        'source': 'test2',
                        'mapping_type': 'attribute'
                    },
                    '3': {
                        'target': 'metadata/bar3',
                        'source': 'test3',
                        'mapping_type': 'globalvar'
                    },
                    '4': {
                        'target': 'metadata/bar4',
                        'source': 'test4',
                        'mapping_type': 'var'
                    }

                },
                'data_maps': {
                    'bar': {
                        '1': 'one',
                        '2': 'two'
                    }
                },
            }, 'test')
            mapper._load_data()
            map_info = mapper._get_ocproc2_map()
            with self.subTest(msg="Implicit variable"):
                self.assertIn('1', map_info['record'])
                self.assertNotIn('1', map_info['global'])
                self.assertEqual(map_info['record']['1']['source'], 'test')
                self.assertEqual(map_info['record']['1']['mapping_type'], 'var')
            with self.subTest(msg="Explicit attribute"):
                self.assertIn('2', map_info['global'])
                self.assertNotIn('2', map_info['record'])
                self.assertEqual(map_info['global']['2']['source'], 'test2')
                self.assertEqual(map_info['global']['2']['mapping_type'], 'attribute')
            with self.subTest(msg="Explicit global variable"):
                self.assertIn('3', map_info['global'])
                self.assertNotIn('3', map_info['record'])
                self.assertEqual(map_info['global']['3']['source'], 'test3')
                self.assertEqual(map_info['global']['3']['mapping_type'], 'globalvar')
            with self.subTest(msg="Explicit variable"):
                self.assertIn('4', map_info['record'])
                self.assertNotIn('4', map_info['global'])
                self.assertEqual(map_info['record']['4']['source'], 'test4')
                self.assertEqual(map_info['record']['4']['mapping_type'], 'var')

    def test_bad_mapping_type(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'var2:test': {
                        'is_index': True
                    }
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs('test', 'ERROR'):
                with self.assertRaises(CNODCError):
                    mapper._load_data()

    def test_bad_data_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'var:test': {
                        'is_index': True,
                        'data_map': 'foo'
                    },
                },
                'data_maps': {
                    'bar': {}
                },
            }, 'test')
            with self.assertLogs('test', 'ERROR'):
                mapper._load_data()
            map_info = mapper._get_ocproc2_map()['record']['var:test']
            self.assertNotIn('data_map', map_info)

    def test_basic_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    }
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar', record.metadata)
                    self.assertIn('RecordNumber', record.coordinates)
                    self.assertEqual(values[idx], record.metadata['Bar'].value)
                    self.assertEqual(values[idx], record.coordinates['RecordNumber'].value)

    def test_global_attribute_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'hello world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual('hello world', record.metadata['Bar2'].value)

    def test_global_variable_int_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test2', 'i4')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            ds.variables['test2'][:] = [6]
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'globalvar:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual(6, record.metadata['Bar2'].value)

    def test_global_variable_str_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createDimension('STRING16', 16)
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test2', 'S1', ('STRING16',))
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            ds.variables['test2'][:] = str_to_netcdf('hello world', 16)
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'globalvar:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual("hello world", record.metadata['Bar2'].value)

    def test_global_variable_vlen_str_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test2', str)
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            ds.variables['test2'][:] = str_to_netcdf_vlen('hello world')
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'globalvar:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual("hello world", record.metadata['Bar2'].value)
