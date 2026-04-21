from autoinject import injector
from azure.core.exceptions import AzureError, ClientAuthenticationError, ResourceNotFoundError, ResourceExistsError
from urllib3.exceptions import ConnectTimeoutError
from zirconium import test_with_config

from tests.helpers.azure_mock import AzureMockClientPool, make_and_wrap_exception
from medsutil.storage import StorageController, StorageTier
from medsutil.storage.azure_blob import AzureBlobHandle
from medsutil.storage.azure import AzureClientPool
from medsutil.storage.interface import PathType
from tests.helpers.base_test_case import BaseTestCase


class BlobTest(BaseTestCase):

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_bad_az_blob_domain(self):
        bad_connections = [
            ("https://hello.files.core.windows.net/container", ''),
            ("https://hello.blob.core.windows.net", ''),
        ]
        for conn, error_name in bad_connections:
            with self.subTest(bad_blob_connection_info=conn):
                b = AzureBlobHandle(conn)
                with self.assertRaisesCNODCError(error_name, False):
                    b.client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("azure", "storage", "test", "connection_string"), "ValueError")
    def test_bad_az_blob_conn_details(self):
        b = AzureBlobHandle("https://test.blob.core.windows.net/ValueError")
        with self.assertRaisesCNODCError():
            b.client()
        with self.assertRaisesCNODCError():
            b.container_client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_wrap_azure_errors(self):
        errors = [
            (ConnectTimeoutError, '', True),
            (ConnectionError, '', True),
        ]
        for err, code, is_transient in errors:
            with self.subTest(error_type=err.__name__):
                with self.assertRaisesCNODCError(code, is_transient) as h:
                    make_and_wrap_exception(AzureError("oh no2", error=err("oh no")))

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_wrap_pure_azure_errors(self):
        errors = [
            (ClientAuthenticationError, '', False),
            (ResourceNotFoundError, '', False),
            (ResourceExistsError, '', False),
            (AzureError, '', False),
        ]
        for err, code, is_transient in errors:
            with self.subTest(error_type=err.__name__):
                with self.assertRaisesCNODCError(code, is_transient) as h:
                    make_and_wrap_exception(err("oh no"))

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_properties(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test.txt')
        self.assertTrue(file.supports_tiering())
        self.assertTrue(file.supports_metadata())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_set_metadata(self):
        with self.temp_data_file('azure_containers/container/test99.txt', metadata=True):
            file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test99.txt')
            file.upload([b'12345'])
            md = file.get_metadata()
            self.assertNotIn('hello', md)
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual('world', md['hello'])

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_set_tier(self):
        for x in (StorageTier.ARCHIVAL, StorageTier.INFREQUENT, StorageTier.FREQUENT):
            with self.subTest(tier=x):
                with self.temp_data_file('azure_containers/container/test99.txt', metadata=True):
                    file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test99.txt')
                    if x == StorageTier.FREQUENT:
                        file.upload([b'12345'], storage_tier=StorageTier.INFREQUENT)
                    else:
                        file.upload([b'12345'])
                    file.set_tier(x)
                    self.assertIs(file.get_tier(), x)

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_delete_file(self):
        with self.temp_data_file('azure_containers/container/test99.txt', metadata=True) as (p, mp):
            p.touch()
            mp.touch()
            file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test99.txt')
            self.assertTrue(file.exists())
            self.assertTrue(p.exists())
            self.assertTrue(mp.exists())
            file.remove()
            self.assertFalse(file.exists())
            self.assertFalse(p.exists())
            self.assertFalse(mp.exists())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_delete_dir(self):
        with self.temp_data_dir('azure_containers/container/test_delete_dir') as d:
            d.mkdir()
            handle = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test_delete_dir/')
            self.assertTrue(handle.exists())
            self.assertTrue(d.exists())
            self.assertTrue(d.is_dir())
            self.assertFalse(d.is_file())
            handle.remove()
            self.assertTrue(d.exists())
            self.assertTrue(handle.exists())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("azure", "storage", "test", "connection_string"), "GoodString")
    def test_existing_file(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test.txt')
        self.assertTrue(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name, 'test.txt')
        self.assertEqual(file.size(), 5)
        self.assertIsNotNone(file.modified_datetime())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("azure", "storage", "test", "connection_string"), "GoodString")
    def test_download(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test.txt')
        self.assertTrue(file.exists())
        p = self.temp_dir / "file.txt"
        file.download(p)
        self.assertTrue(file.exists())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_non_existing_file(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test3.txt')
        self.assertFalse(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name, 'test3.txt')
        self.assertIsNone(file.size())
        self.assertIsNone(file.modified_datetime())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_dir(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test/foo/bar/')
        self.assertTrue(file.exists())
        self.assertTrue(file.is_dir())
        self.assertEqual(file.name, 'bar')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_get_handle(self):
        sc = StorageController()
        handle = sc.get_filepath('https://test.blob.core.windows.net/container/test/foo/bar/')
        self.assertIsInstance(handle, AzureBlobHandle)

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_upload_with_tiers(self):
        tests = [
            (None, 'frequent', StorageTier.FREQUENT),
            (StorageTier.ARCHIVAL, 'archival', StorageTier.ARCHIVAL),
            (StorageTier.INFREQUENT, 'infrequent', StorageTier.INFREQUENT),
            (StorageTier.FREQUENT, 'frequent', StorageTier.FREQUENT),
        ]
        for storage_tier_arg, expected_metadata, expected_tier in tests:
            with self.subTest(storage_tier_arg=storage_tier_arg):
                with self.temp_data_file('azure_containers/container/test2.txt', metadata=True) as (af, amf):
                    fp = self.temp_dir / 'bar.txt'
                    with open(fp, 'w') as h:
                        h.write('hello world')
                    file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test2.txt')
                    self.assertFalse(file.exists())
                    self.assertFalse(af.exists())
                    file.upload(fp, storage_tier=storage_tier_arg)
                    self.assertTrue(af.exists())
                    self.assertTrue(file.exists())
                    self.assertIs(expected_tier, file.get_tier())
                    md = file.get_metadata()
                    self.assertIn('StorageTier', md)
                    self.assertEqual(md['StorageTier'], expected_metadata)

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_walk(self):
        blob = AzureBlobHandle.build('https://test.blob.core.windows.net/container')
        files = [file.path() for file in blob.iterdir(path_types=PathType.FILE)]
        self.assertIn("https://test.blob.core.windows.net/container/test.txt", files)
        self.assertIn("https://test.blob.core.windows.net/container/subdir/test2.txt", files)
        self.assertIn("https://test.blob.core.windows.net/container/subdir/subdir2/test5.txt", files)
        self.assertNotIn("https://test.blob.core.windows.net/container/subdir/", files)

