import datetime
import logging

import netCDF4 as nc
import yaml

from cnodc.ocproc2 import MultiElement, SingleElement
from cnodc.util import CNODCError
from cnodc.util.sanitize import str_to_netcdf, str_to_netcdf_vlen
from helpers.base_test_case import BaseTestCase
from cnodc.ocproc2.codecs.netcdf import NetCDFCommonMapper, NetCDFCommonDecoderError, NetCDFCommonDecoder


def dynamic_int_processor(data_processor, value, *args, **kwargs):
    return int(value)


class TestNetCDFCommonDecode(BaseTestCase):

    def test_full_decoder(self):
        config_file = self.temp_dir / 'test.yaml'
        with open(config_file, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': {
                            'foo': 'foo1',
                            'bar': 'bar1',
                            'hello': 'hello1',
                            'world': 'world1',
                        }
                    },
                },
                'data_maps': {},
            }, h)
        nc_file = self.temp_dir / "test.nc"
        with nc.Dataset(nc_file, "w") as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
        bytes_iterable = []
        with open(nc_file, 'rb') as h:
            r = h.read(128)
            while r != b'':
                bytes_iterable.append(r)
                r = h.read(128)
        decoder = NetCDFCommonDecoder()
        records = [x for x in decoder.decode_records(bytes_iterable, mapping_file=str(config_file))]
        self.assertEqual(5, len(records))
        for idx, record in enumerate(records):
            with self.subTest(record_no=idx):
                self.assertIn('Bar2', record.metadata)
                self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                values = [x.value for x in record.metadata['Bar2'].all_values()]
                self.assertEqual(4, len(values))
                self.assertIn('foo1', values)
                self.assertIn('bar1', values)
                self.assertIn('hello1', values)
                self.assertIn('world1', values)

    def test_bad_mapping_file(self):
        nc_file = self.temp_dir / "test.nc"
        with nc.Dataset(nc_file, "w") as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
        bytes_iterable = []
        with open(nc_file, 'rb') as h:
            r = h.read(128)
            while r != b'':
                bytes_iterable.append(r)
                r = h.read(128)
        decoder = NetCDFCommonDecoder()
        with self.assertLogs("cnodc.netcdf_common_decoder", "ERROR"):
            _ = [x for x in decoder.decode_records(bytes_iterable)]

    def test_default_mapping_file_and_class(self):
        nc_file = self.temp_dir / "test.nc"
        with nc.Dataset(nc_file, "w") as ds:
            ds.createDimension('FOO')
            v = ds.createVariable('JULD', 'i4', ('FOO',))
            v.setncattr('units', 'seconds since 1950-01-01T00:00:00')
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['JULD'][:] = values
        bytes_iterable = []
        with open(nc_file, 'rb') as h:
            r = h.read(128)
            while r != b'':
                bytes_iterable.append(r)
                r = h.read(128)
        decoder = NetCDFCommonDecoder()
        with self.assertLogs("cnodc.programs.glider.ego_decode", "INFO"):
            records = [x for x in decoder.decode_records(bytes_iterable, mapping_class="cnodc.programs.glider.ego_decode.GliderEGOMapper")]
        self.assertEqual(5, len(records))

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
            ds.createDimension('N_COUNT', )
            ds.createVariable('test', str, ('N_COUNT', ))
            ds.setncattr('test2', '')
            ds.createVariable('test3', str)
            ds.createVariable('test4', str, ('N_COUNT',))
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
                self.assertEqual(map_info['global']['attribute:test2']['data_map'], {'1': 'one', '2': 'two'})

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
                '_targets': ['metadata/bar'],
                'is_index': True
            }
            for key in expected:
                with self.subTest(expected_key=key):
                    self.assertIn(key, map_info['record']['var:test'])
                    self.assertEqual(expected[key], map_info['record']['var:test'][key])


    def test_good_basic_config_explicit(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            ds.createDimension('N_COUNT')
            ds.createVariable('test', str, ('N_COUNT',))
            ds.createVariable('test3', str, ())
            ds.createVariable('test4', str, ('N_COUNT',))
            ds.setncattr('test2', '')
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
            with self.assertLogs('test', 'WARNING'):
                with self.assertRaises(CNODCError):
                    mapper._load_data()

    def test_bad_data_map(self):
        with nc.Dataset("inmemory.nc", "w", diskless=True) as ds:
            ds.createDimension('N_COUNT')
            ds.createVariable('test', str, ('N_COUNT',))
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
            with self.assertLogs('test', 'WARNING'):
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

    def test_global_variable_dynamic_mapping(self):
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
                    'globalvar:test2': {
                        'target': 'metadata/Bar2',
                        'data_processor': 'decode.test_netcdf.dynamic_int_processor'
                    }
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual(6, record.metadata['Bar2'].value)

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
            self.assertEqual('hello world', mapper.var_to_string('test2'))
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

    def test_multiple_target_mapping(self):
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
                    'attribute:test2': {
                        'target': ['metadata/Bar2', 'metadata/Bar3'],
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual('hello world', record.metadata['Bar2'].value)
                    self.assertIn('Bar3', record.metadata)
                    self.assertEqual('hello world', record.metadata['Bar3'].value)

    def test_multiple_value_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'hello world')
            ds.setncattr('test3', 'bonjour le monde')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'allow_multiple': True,
                    },
                    'attribute:test3': {
                        'target': 'metadata/Bar2',
                        'allow_multiple': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    self.assertEqual(2, len(record.metadata['Bar2'].value))
                    data = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertIn('hello world', data)
                    self.assertIn('bonjour le monde', data)

    def test_missing_target_map(self):
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
                    'attribute:test2': 'metadata/Bar2/metadata/Quality',
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs('test', 'WARNING') as info:
                records = [x for x in mapper.build_records()]
                for x in info.records:
                    self.assertEqual(x.levelno, logging.WARNING)
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_missing_target_map_acceptable(self):
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
                    'attribute:test2': {
                        'target': 'metadata/Bar2/metadata/Quality',
                        'nowarn_missing_target': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs('test', 'INFO') as info:
                records = [x for x in mapper.build_records()]
                for x in info.records:
                    self.assertEqual(x.levelno, logging.INFO)
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_no_such_attribute(self):
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
                    },
                    'attribute:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs("test", "WARNING"):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_no_such_global_variable(self):
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
                    },
                    'globalvar:test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs("test", "WARNING"):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_two_variables(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test2', 'i4', ('FOO',))
            values = [1, 2, 3, 4, 5]
            rev_values = [5, 4, 3, 2, 1]
            ds.variables['test'][:] = values
            ds.variables['test2'][:] = rev_values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertEqual(rev_values[idx], record.metadata['Bar2'].value)

    def test_missing_data_variable(self):
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
                    },
                    'test2': 'metadata/Bar2',
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs('test', 'WARNING'):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_adjusted_data_variable(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test_adj', 'i4', ('FOO',))
            values = [1, 2, 3, 4, 5]
            updated = [2, 3, 4, 5, 6]
            ds.variables['test'][:] = values
            ds.variables['test_adj'][:] = updated
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                        'adjusted_source': 'test_adj'
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar', record.metadata)
                    self.assertEqual(updated[idx], record.metadata['Bar'].value)
                    self.assertIn('Unadjusted', record.metadata['Bar'].metadata)
                    self.assertEqual(values[idx], record.metadata['Bar'].metadata['Unadjusted'].value)


    def test_unadjusted_data_variable(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test_adj', 'i4', ('FOO',))
            values = [1, 2, 3, 4, 5]
            original = [2, 3, 4, 5, 6]
            ds.variables['test'][:] = values
            ds.variables['test_adj'][:] = original
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                        'unadjusted_source': 'test_adj'
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar', record.metadata)
                    self.assertEqual(values[idx], record.metadata['Bar'].value)
                    self.assertIn('Unadjusted', record.metadata['Bar'].metadata)
                    self.assertEqual(original[idx], record.metadata['Bar'].metadata['Unadjusted'].value)

    def test_qc_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.createVariable('test_qc', 'i4', ('FOO',))
            values = [1, 2, 3, 4, 9]
            qc_values = [1, 1, 1, 1, 4]
            ds.variables['test'][:] = values
            ds.variables['test_qc'][:] = qc_values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                        'qc_source': 'test_qc'
                    }
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar', record.metadata)
                    self.assertIn('Quality', record.metadata['Bar'].metadata)
                    self.assertEqual(qc_values[idx], record.metadata['Bar'].metadata['Quality'].value)

    def test_unit_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            v = ds.createVariable('test', 'i4', ('FOO',))
            setattr(v, 'units', 'm')
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
                    self.assertIn('Units', record.metadata['Bar'].metadata)
                    self.assertEqual("m", record.metadata['Bar'].metadata['Units'].value)

    def test_good_unit_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            v = ds.createVariable('test', 'i4', ('FOO',))
            setattr(v, 'units', 'm')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'coordinates/Depth',
                        'is_index': True,
                    }
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Depth', record.coordinates)
                    self.assertIn('Units', record.coordinates['Depth'].metadata)
                    self.assertEqual("m", record.coordinates['Depth'].metadata['Units'].value)

    def test_bad_unit_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            v = ds.createVariable('test', 'i4', ('FOO',))
            setattr(v, 'units', 's')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'coordinates/Depth',
                        'is_index': True,
                    }
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Depth', record.coordinates)
                    self.assertIn('Units', record.coordinates['Depth'].metadata)
                    self.assertEqual("s", record.coordinates['Depth'].metadata['Units'].value)

    def test_empty_global_attribute_mapping(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', '')
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
                    self.assertNotIn('Bar2', record.metadata)

    def test_add_metadata(self):
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
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'metadata': {
                            'foo': 'bar',
                            'extended': {
                                '_value': 500,
                                '_metadata': {
                                    'Units': 'm',
                                },
                            }
                        }
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIn('foo', record.metadata['Bar2'].metadata)
                    self.assertEqual('bar', record.metadata['Bar2'].metadata['foo'].value)
                    self.assertIn('extended', record.metadata['Bar2'].metadata)
                    self.assertEqual(500, record.metadata['Bar2'].metadata['extended'].value)
                    self.assertIn('Units', record.metadata['Bar2'].metadata['extended'].metadata)
                    self.assertEqual('m', record.metadata['Bar2'].metadata['extended'].metadata['Units'].value)

    def test_split_value_simple(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo', values)
                    self.assertIn('bar', values)
                    self.assertIn('hello', values)
                    self.assertIn('world', values)


    def test_split_value_empty(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', '')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertNotIn('Bar2', record.metadata)

    def test_process_value_empty(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            self.assertIsNone(mapper._process_value(';;;', {
                'separator': ';'
            }))

    def test_split_value_one_list(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': False,
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], SingleElement)
                    self.assertEqual(4, len(record.metadata['Bar2'].value))
                    self.assertIn('foo', record.metadata['Bar2'].value)
                    self.assertIn('bar', record.metadata['Bar2'].value)
                    self.assertIn('hello', record.metadata['Bar2'].value)
                    self.assertIn('world', record.metadata['Bar2'].value)


    def test_split_value_regex(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar,hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';|,',
                        'allow_multiple': True,
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo', values)
                    self.assertIn('bar', values)
                    self.assertIn('hello', values)
                    self.assertIn('world', values)

    def test_data_mapping_local(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': {
                            'foo': 'foo1',
                            'bar': 'bar1',
                            'hello': 'hello1',
                            'world': 'world1',
                        }
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo1', values)
                    self.assertIn('bar1', values)
                    self.assertIn('hello1', values)
                    self.assertIn('world1', values)

    def test_data_mapping_reuseable(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': 'foobar',
                    },
                },
                'data_maps': {
                    'foobar': {
                        'foo': 'foo1',
                        'bar': 'bar1',
                        'hello': 'hello1',
                        'world': 'world1',
                    }
                },
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo1', values)
                    self.assertIn('bar1', values)
                    self.assertIn('hello1', values)
                    self.assertIn('world1', values)

    def test_data_mapping_bad_data_map(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': 'foobar2',
                    },
                },
                'data_maps': {
                    'foobar': {
                        'foo': 'foo1',
                        'bar': 'bar1',
                        'hello': 'hello1',
                        'world': 'world1',
                    }
                },
            }, 'test')
            with self.assertLogs('test', 'WARNING'):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo', values)
                    self.assertIn('bar', values)
                    self.assertIn('hello', values)
                    self.assertIn('world', values)

    def test_data_mapping_missing_value(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': 'foobar',
                    },
                },
                'data_maps': {
                    'foobar': {
                        'bar': 'bar1',
                        'hello': 'hello1',
                        'world': 'world1',
                    }
                },
            }, 'test')
            with self.assertLogs('test', 'ERROR'):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo', values)
                    self.assertIn('bar1', values)
                    self.assertIn('hello1', values)
                    self.assertIn('world1', values)

    def test_data_mapping_subkey(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': {
                            'foo': {'key': 'foo1'},
                            'bar': {'key': 'bar1'},
                            'hello': {'key': 'hello1'},
                            'world': {'key': 'world1'},
                        },
                        'data_map_key': 'key',
                    },
                },
                'data_maps': {},
            }, 'test')
            records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo1', values)
                    self.assertIn('bar1', values)
                    self.assertIn('hello1', values)
                    self.assertIn('world1', values)

    def test_data_mapping_missing_subkey(self):
        with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
            ds.createDimension('FOO')
            ds.createVariable('test', 'i4', ('FOO',))
            ds.setncattr('test2', 'foo;bar;hello;world')
            values = [1, 2, 3, 4, 5]
            ds.variables['test'][:] = values
            mapper = NetCDFCommonMapper(ds, {
                'ocproc2_map': {
                    'test': {
                        'target': 'metadata/Bar',
                        'is_index': True,
                    },
                    'attribute:test2': {
                        'target': 'metadata/Bar2',
                        'separator': ';',
                        'allow_multiple': True,
                        'data_map': {
                            'foo': {'key2': 'foo1'},
                            'bar': {'key': 'bar1'},
                            'hello': {'key': 'hello1'},
                            'world': {'key': 'world1'},
                        },
                        'data_map_key': 'key',
                    },
                },
                'data_maps': {},
            }, 'test')
            with self.assertLogs('test', 'ERROR'):
                records = [x for x in mapper.build_records()]
            self.assertEqual(5, len(records))
            for idx, record in enumerate(records):
                with self.subTest(record_no=idx):
                    self.assertIn('Bar2', record.metadata)
                    self.assertIsInstance(record.metadata['Bar2'], MultiElement)
                    values = [x.value for x in record.metadata['Bar2'].all_values()]
                    self.assertEqual(4, len(values))
                    self.assertIn('foo', values)
                    self.assertIn('bar1', values)
                    self.assertIn('hello1', values)
                    self.assertIn('world1', values)

    def test_datetime_mapping(self):
        for increment in ('seconds', 'minutes', 'hours', 'days', 'weeks', 'SECONDS', 'MINUTES', 'HOURS', 'DAYS', 'WEEKS'):
            with self.subTest(increment=increment):
                with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
                    ds.createDimension('FOO')
                    v = ds.createVariable('test', 'i4', ('FOO',))
                    setattr(v, 'units', f'{increment} since 1960-01-01T00:00:00+0000')
                    values = [50, 100, 150, 200, 250]
                    ds.variables['test'][:] = values
                    mapper = NetCDFCommonMapper(ds, {
                        'ocproc2_map': {
                            'test': {
                                'target': 'coordinates/Time',
                                'data_processor': '_time_since',
                                'is_index': True,
                            }
                        },
                        'data_maps': {},
                    }, 'test')
                    base = datetime.datetime.fromisoformat('1960-01-01T00:00:00+0000')
                    records = [x for x in mapper.build_records()]
                    self.assertEqual(5, len(records))
                    for idx, record in enumerate(records):
                        with self.subTest(record_no=idx):
                            self.assertIn('Time', record.coordinates)
                            self.assertTrue(record.coordinates['Time'].is_iso_datetime())
                            self.assertFalse(record.coordinates['Time'].is_empty())
                            dt = record.coordinates['Time'].to_datetime()
                            self.assertIsInstance(dt, datetime.datetime)
                            self.assertEqual((base + datetime.timedelta(**{increment.lower(): values[idx]})), dt)

    def test_invalid_increment(self):
        for invalid_unit in ('foobar since 1960-01-01T00:00:00', 'foobar', 'seconds', 'seconds since 1960-13-30T00:00:00', None, 'seconds since ', 'i dont know'):
            with self.subTest(invalid_unit=invalid_unit):
                with nc.Dataset("inmemory.nc", "r+", diskless=True) as ds:
                    ds.createDimension('FOO')
                    v = ds.createVariable('test', 'i4', ('FOO',))
                    if invalid_unit:
                        setattr(v, 'units', invalid_unit)
                    values = [50, 100, 150, 200, 250]
                    ds.variables['test'][:] = values
                    mapper = NetCDFCommonMapper(ds, {
                        'ocproc2_map': {
                            'test': {
                                'target': 'coordinates/Time',
                                'data_processor': '_time_since',
                                'is_index': True,
                            }
                        },
                        'data_maps': {},
                    }, 'test')
                    with self.assertRaises(NetCDFCommonDecoderError):
                        _ = [x for x in mapper.build_records()]
