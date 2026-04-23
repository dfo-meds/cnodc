import datetime
import random
import unittest
from unittest import SkipTest

from azure.storage.blob import BlobClient
from zirconium import ApplicationConfig
from autoinject import injector

from medsutil.storage import StorageTier
from medsutil.storage.azure_blob import AzureBlobHandle
from medsutil.storage.interface import PathType
from tests.helpers.base_test_case import BaseTestCase, skip_long_test

@skip_long_test
class TestLiveAzureBlobs(BaseTestCase):

    @classmethod
    @injector.inject
    def setUpClass(cls, config: ApplicationConfig = None):
        account_name = config.as_str(("tests", "azure_storage", "account_name"))
        container_name = config.as_str(("tests", "azure_storage", "container_name"))
        connection_str = config.as_str(("tests", "azure_storage", "connection_string"))
        if not (account_name and connection_str and container_name):
            raise SkipTest("no azure file share for tests has been configured")
        cls.base_azure_share = f'https://{account_name}.blob.core.windows.net/{container_name}/'
        cls.file_data: dict[str, tuple[int, datetime.datetime]] = {}
        try:
            files = {
                'test.txt': b'hello',
                'subdir/test2.txt': b'hello world',
                'subdir/subdir2/test5.txt': b'stuff2',
            }
            for file in files:
                blob = BlobClient.from_connection_string(connection_str, container_name, file)
                if not blob.exists():
                    blob.upload_blob(files[file])
                props = blob.get_blob_properties()
                cls.file_data[file] = (props.size, props.last_modified)
            remove_files = ['new.txt', 'test3.txt']
            for file in remove_files:
                blob = BlobClient.from_connection_string(connection_str, container_name, file)
                if blob.exists():
                    blob.delete_blob()
        except Exception as ex:
            raise SkipTest("error setting up container") from ex

    def test_properties(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}test.txt')
        self.assertTrue(file.supports_tiering())
        self.assertTrue(file.supports_metadata())

    def test_set_metadata(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}new.txt')
        try:
            file.upload([b'12345'])
            md = file.get_metadata()
            self.assertNotIn('hello', md)
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual('world', md['hello'])
        finally:
            if file.exists():
                file.remove()

    def test_set_tier(self):
        for x in (StorageTier.ARCHIVAL, StorageTier.INFREQUENT, StorageTier.FREQUENT):
            with self.subTest(tier=x):
                file = AzureBlobHandle.build(f'{self.base_azure_share}new.txt')
                try:
                    if x == StorageTier.FREQUENT:
                        file.upload(b'12345', storage_tier=StorageTier.INFREQUENT)
                    else:
                        file.upload(b'12345')
                    file.set_tier(x)
                    self.assertIs(file.get_tier(), x)
                finally:
                    if file.exists():
                        file.remove()

    def test_create_and_delete_file(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}new.txt')
        try:
            file.upload(b'12345')
            self.assertTrue(file.exists())
            file.remove()
            self.assertFalse(file.exists())
        finally:
            if file.exists():
                file.remove()

    #@skip_long_test
    @unittest.skip
    def test_large_upload(self):
        def _generate_lots_of_data():
            for _ in range(0, 5):
                yield random.randbytes(1024 * 1024 * 6)
        file = AzureBlobHandle.build(f'{self.base_azure_share}new.txt')
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

    def test_delete_dir(self):
        handle = AzureBlobHandle.build(f'{self.base_azure_share}test_delete_dir/')
        self.assertTrue(handle.is_dir())
        self.assertTrue(handle.exists())
        handle.mkdir()
        self.assertTrue(handle.exists())

    def test_existing_file(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}test.txt')
        self.assertTrue(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name, 'test.txt')
        self.assertEqual(file.size(), self.file_data['test.txt'][0])
        self.assertSameTime(file.modified_datetime(), self.file_data['test.txt'][1])

    def test_download(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}test.txt')
        self.assertTrue(file.exists())
        p = self.temp_dir / "file.txt"
        file.download(p)
        self.assertTrue(file.exists())
        self.assertEqual(file.read_bytes(), b'hello')

    def test_non_existing_file(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}test3.txt')
        self.assertFalse(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name, 'test3.txt')
        self.assertIsNone(file.size())
        self.assertIsNone(file.modified_datetime())

    def test_dir(self):
        file = AzureBlobHandle.build(f'{self.base_azure_share}test/foo/bar/')
        self.assertTrue(file.exists())
        self.assertTrue(file.is_dir())
        self.assertEqual(file.name, 'bar')

    def test_upload_with_tiers(self):
        tests = [
            (None, 'frequent', StorageTier.FREQUENT),
            (StorageTier.ARCHIVAL, 'archival', StorageTier.ARCHIVAL),
            (StorageTier.INFREQUENT, 'infrequent', StorageTier.INFREQUENT),
            (StorageTier.FREQUENT, 'frequent', StorageTier.FREQUENT),
        ]
        for storage_tier_arg, expected_metadata, expected_tier in tests:
            with self.subTest(storage_tier_arg=storage_tier_arg):
                file = AzureBlobHandle.build(f'{self.base_azure_share}new.txt')
                try:
                    fp = self.temp_dir / 'bar.txt'
                    with open(fp, 'w') as h:
                        h.write('hello world')
                    self.assertFalse(file.exists())
                    file.upload(fp, storage_tier=storage_tier_arg)
                    self.assertTrue(file.exists())
                    self.assertIs(expected_tier, file.get_tier())
                    md = file.get_metadata()
                    self.assertIn('StorageTier', md)
                    self.assertEqual(md['StorageTier'], expected_metadata)
                finally:
                    if file.exists():
                        file.remove()

    def test_walk(self):
        blob = AzureBlobHandle.build(f'{self.base_azure_share}/')
        files = [file.path() for file in blob.iterdir(path_types=PathType.FILE)]
        self.assertIn(f"{self.base_azure_share}test.txt", files)
        self.assertIn(f"{self.base_azure_share}subdir/test2.txt", files)
        self.assertIn(f"{self.base_azure_share}subdir/subdir2/test5.txt", files)
        self.assertNotIn(f"{self.base_azure_share}subdir/", files)

    def test_walk_on_file(self):
        blob = AzureBlobHandle.build(f'{self.base_azure_share}/')
        blob.walk_max_memory = 10
        files = [file.path() for file in blob.iterdir(path_types=PathType.FILE)]
        self.assertIn(f"{self.base_azure_share}test.txt", files)
        self.assertIn(f"{self.base_azure_share}subdir/test2.txt", files)
        self.assertIn(f"{self.base_azure_share}subdir/subdir2/test5.txt", files)
        self.assertNotIn(f"{self.base_azure_share}subdir/", files)
