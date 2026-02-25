import netCDF4 as nc
import yaml

from cnodc.util import CNODCError
from core import BaseTestCase
from cnodc.ocproc2.codecs.netcdf import NetCDFCommonDecoder, NetCDFCommonMapper, NetCDFCommonDecoderError


class TestNetCDFCommonDecode(BaseTestCase):

    def test_bad_file(self):
        path = self.temp_dir / 'file.txt'
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_config_file_is_dir(self):
        path = self.temp_dir
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_bad_config_no_map(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'data_maps': {}
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_bad_config_bad_map(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': None,
                'data_maps': {}
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_bad_config_no_data_map(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {},
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_bad_config_bad_data_map(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {},
                'data_maps': None,
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_bad_config_no_index_var(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {},
                'data_maps': {},
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertRaises(NetCDFCommonDecoderError):
            mapper._load_data()

    def test_good_basic_config(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
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
        ds = nc.Dataset("inmemory.nc", "w")
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
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
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
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
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
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {
                    'var2:test': {
                        'is_index': True
                    }
                },
                'data_maps': {},
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertLogs('test', 'ERROR'):
            with self.assertRaises(CNODCError):
                mapper._load_data()

    def test_bad_data_map(self):
        path = self.temp_dir / 'file.txt'
        with open(path, "w") as h:
            yaml.safe_dump({
                'ocproc2_map': {
                    'var:test': {
                        'is_index': True,
                        'data_map': 'foo'
                    },
                },
                'data_maps': {
                    'bar': {}
                },
            }, h)
        ds = nc.Dataset("inmemory.nc", "w")
        mapper = NetCDFCommonMapper(ds, path, 'test')
        with self.assertLogs('test', 'ERROR'):
            mapper._load_data()
        map_info = mapper._get_ocproc2_map()['record']['var:test']
        self.assertNotIn('data_map', map_info)
