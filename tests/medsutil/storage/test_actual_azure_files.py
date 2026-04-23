import datetime
import random
import unittest
from unittest import SkipTest

from zirconium import ApplicationConfig
from autoinject import injector

from medsutil.storage.azure_files import AzureFileHandle
from medsutil.storage.interface import PathType
from tests.helpers.base_test_case import BaseTestCase, skip_long_test
from azure.storage.fileshare import ShareClient

@skip_long_test
class TestLiveAzureFileShare(BaseTestCase):

    @classmethod
    @injector.inject
    def setUpClass(cls, config: ApplicationConfig = None):
        account_name = config.as_str(("tests", "azure_storage", "account_name"))
        share_name = config.as_str(("tests", "azure_storage", "file_share_name"))
        connection_str = config.as_str(("tests", "azure_storage", "connection_string"))
        if not (account_name and connection_str and share_name):
            raise SkipTest("no azure file share for tests has been configured")
        cls.base_azure_share = f'https://{account_name}.file.core.windows.net/{share_name}/'
        cls.file_data = {}
        try:
            sc: ShareClient = ShareClient.from_connection_string(connection_str, share_name)
            directories = ['subdir', 'subdir/subdir2']
            for dir_name in directories:
                d = sc.get_directory_client(dir_name)
                if not d.exists():
                    d.create_directory()
                x = d.get_directory_properties()
                cls.file_data[dir_name] = (None, x.last_modified)
            files = {
                'test.txt': b'hello',
                'subdir/test2.txt': b'hello world',
                'subdir/subdir2/test5.txt': b'stuff2',
            }
            for file_name in files:
                f = sc.get_file_client(file_name)
                if not f.exists():
                    f.upload_file(files[file_name])
                f = f.get_file_properties()
                cls.file_data[file_name] = (f.size, f.last_modified)
            not_file = ['not_a_file.txt', 'new.txt', 'new_dir/new.txt']
            for file_name in not_file:
                f = sc.get_file_client(file_name)
                if f.exists():
                    f.delete_file()
            not_subdir = ['new_dir', 'not_a_subdir']
            for dir_name in not_subdir:
                f = sc.get_directory_client(dir_name)
                if f.exists():
                    f.delete_directory()
        except Exception as e:
            raise SkipTest(str(e))

    def test_dir_properties(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}subdir')
        self.assertEqual(file.full_path_within_share(), 'subdir')
        self.assertTrue(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name, 'subdir')
        self.assertIsNone(file.size())
        self.assertIsNotNone(file.modified_datetime())
        self.assertSameTime(file.modified_datetime(), self.file_data['subdir'][1])

    def test_file_properties(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}test.txt')
        self.assertEqual(file.full_path_within_share(), 'test.txt')
        self.assertFalse(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name, 'test.txt')
        self.assertEqual(file.size(), self.file_data['test.txt'][0])
        self.assertSameTime(file.modified_datetime(), self.file_data['test.txt'][1])

    def test_exists(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}test.txt')
        self.assertTrue(file.exists())
        file = AzureFileHandle.build(f'{self.base_azure_share}not_a_file.txt')
        self.assertFalse(file.exists())
        subdir = AzureFileHandle.build(f'{self.base_azure_share}subdir')
        self.assertTrue(subdir.exists())
        subdir = AzureFileHandle.build(f'{self.base_azure_share}not_a_subdir')
        self.assertFalse(subdir.exists())

    def test_make_and_remove_file(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}new.txt')
        try:
            self.assertFalse(file.exists())
            file.touch()
            self.assertTrue(file.exists())
            file.remove()
            self.assertFalse(file.exists())
        finally:
            if file.exists():
                file.remove()

    def test_make_and_remove_dir(self):
        subdir = AzureFileHandle.build(f'{self.base_azure_share}new_dir/')
        file = subdir / 'new.txt'
        try:
            self.assertFalse(subdir.exists())
            subdir.mkdir()
            self.assertTrue(subdir.exists())
            self.assertFalse(file.exists())
            file.touch()
            self.assertTrue(file.exists())
            file.remove()
            subdir.remove()
            self.assertFalse(file.exists())
            self.assertFalse(subdir.exists())
        finally:
            if file.exists():
                file.remove()
            if subdir.exists():
                subdir.remove()

    def test_download(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}subdir/test2.txt')
        local = self.temp_dir / 'local.txt'
        file.download(local)
        self.assertTrue(local.exists())
        with open(local, 'r') as h:
            self.assertEqual(h.read(), 'hello world')

    def test_upload(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}new.txt')
        try:
            self.assertFalse(file.exists())
            local = self.temp_dir / 'local.txt'
            with open(local, 'w') as h:
                h.write('what what!')
            file.upload(local, buffer_size=3)
            self.assertTrue(file.exists())
            with file.open('rb') as h:
                self.assertEqual(h.read(), b'what what!')
        finally:
            if file.exists():
                file.remove()

    @skip_long_test
    def test_large_upload(self):
        def _generate_lots_of_data():
            for _ in range(0, 5):
                yield random.randbytes(1024 * 1024 * 6)
        file = AzureFileHandle.build(f'{self.base_azure_share}new.txt')
        try:
            temp_file = self.temp_dir / 'test.dat'
            with open(temp_file, 'wb') as h:
                for x in _generate_lots_of_data():
                    h.write(x)
            file.upload(temp_file)
            self.assertTrue(file.exists())
            self.assertEqual(file.size(), 1024 * 1024 * 30)
            temp_file2 = self.temp_dir / 'download.dat'
            file.download(temp_file2)
            with open(temp_file, 'rb') as h:
                with open(temp_file2, 'rb') as h2:
                    self.assertEqual(h.read(), h2.read())
            file.remove()
        finally:
            if file.exists():
                file.remove()

    def test_metadata(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}new.txt')
        try:
            self.assertFalse(file.exists())
            local = self.temp_dir / 'local.txt'
            with open(local, 'w') as h:
                h.write('what what!')
            file.upload(local, metadata={
                'hello': 'world'
            })
            self.assertTrue(file.exists())
            with file.open('rb') as h:
                self.assertEqual(h.read(), b'what what!')
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')
        finally:
            if file.exists():
                file.remove()

    def test_directory_metadata(self):
        subdir = AzureFileHandle.build(f'{self.base_azure_share}new_dir/')
        try:
            self.assertFalse(subdir.exists())
            subdir.mkdir()
            subdir.set_metadata({'hello': 'world'})
            md = subdir.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')
        finally:
            if subdir.exists():
                subdir.remove()

    def test_file_metadata(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}new.txt')
        try:
            self.assertFalse(file.exists())
            file.touch()
            self.assertTrue(file.exists())
            file.set_metadata({'hello': 'world'})
            self.assertTrue(file.exists())
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')
        finally:
            if file.exists():
                file.remove()

    def test_walk_recursive(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}')
        files = [f.name for f in file.iterdir(path_types=PathType.FILE)]
        self.assertIn('test.txt', files)
        self.assertIn('test2.txt', files)
        self.assertIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)

    def test_walk_non_recursive(self):
        file = AzureFileHandle.build(f'{self.base_azure_share}')
        files = [f.name for f in file.iterdir(False, path_types=PathType.FILE)]
        self.assertIn('test.txt', files)
        self.assertNotIn('test2.txt', files)
        self.assertNotIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)
