from autoinject import injector
from zirconium import test_with_config

from medsutil.storage.interface import PathType
from tests.helpers.mock_azure import AzureMockClientPool
from medsutil.storage.azure import wrap_azure_errors, AzureClientPool
from medsutil.storage.azure_files import AzureFileHandle
from tests.helpers.base_test_case import BaseTestCase
import datetime



@wrap_azure_errors
def make_and_wrap_exception(ex):
    raise ex

class AzureFileTest(BaseTestCase):

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_bad_connection_details(self):
        tests = [
            ("https://hello.blob.core.windows.net/share", ''),
            ("https://test.file.core.windows.net", '')
        ]
        for conn, err_code in tests:
            with self.subTest(conn=conn):
                b = AzureFileHandle(conn)
                with self.assertRaisesCNODCError():
                    b.file_client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_bad_az_file_share_file(self):
        b = AzureFileHandle("https://test.file.core.windows.net/ValueError")
        with self.assertRaisesCNODCError():
            b.file_client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_bad_az_file_share_dir(self):
        b = AzureFileHandle("https://test.file.core.windows.net/ValueError/")
        with self.assertRaisesCNODCError():
            b.directory_client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("storage", "azure", "test", 'connection_string'), 'ValueError')
    def test_bad_az_file_share_file(self):
        with self.subTest(msg='file test'):
            b = AzureFileHandle("https://test.file.core.windows.net/share/file.txt")
            with self.assertRaisesCNODCError():
                b.file_client()
        with self.subTest(msg='dir test'):
            b = AzureFileHandle("https://test.file.core.windows.net/share/")
            with self.assertRaisesCNODCError():
                b.directory_client()

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_general_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test.txt')
        self.assertTrue(file.supports_metadata())
        self.assertFalse(file.supports_tiering())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("storage", "azure", "test", "connection_string"), "GoodString")
    def test_dir_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir/')
        self.assertEqual(file.full_path_within_share(), 'subdir')
        self.assertTrue(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name, 'subdir')
        d = self.data_file_path('azure_file_shares/share/subdir')
        self.assertSameTime(file.modified_datetime(), datetime.datetime.fromtimestamp(d.stat().st_mtime).astimezone())
        self.assertIsNone(file.size())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    @test_with_config(("azure", "storage", "test", "connection_string"), "GoodString")
    def test_file_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test.txt')
        self.assertEqual(file.full_path_within_share(), 'test.txt')
        self.assertFalse(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name, 'test.txt')
        d = self.data_file_path('azure_file_shares/share/test.txt')
        self.assertSameTime(file.modified_datetime(), datetime.datetime.fromtimestamp(d.stat().st_mtime).astimezone())
        self.assertEqual(file.size(), d.stat().st_size)

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_remove_file(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new.txt')
        with self.temp_data_file('azure_file_shares/share/new.txt') as real_file:
            real_file.touch()
            self.assertTrue(file.exists())
            file.remove()
            self.assertFalse(file.exists())
            self.assertFalse(real_file.exists())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_make_and_remove_dir(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/real_dir/')
        with self.temp_data_dir('azure_file_shares/share/real_dir') as real_dir:
            real_file = real_dir / 'new.txt'
            self.assertFalse(real_dir.exists())
            file.mkdir()
            self.assertTrue(real_dir.exists())
            self.assertFalse(real_file.exists())
            real_file.touch()
            self.assertTrue(file.exists())
            file.remove()
            self.assertFalse(file.exists())
            self.assertFalse(real_dir.exists())
            self.assertFalse(real_file.exists())

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_supports(self):
        self.assertTrue(AzureFileHandle.supports('https://test.file.core.windows.net/share/new.txt'))
        self.assertFalse(AzureFileHandle.supports('https://test.blob.core.windows.net/share/new.txt'))
        self.assertFalse(AzureFileHandle.supports('ftp://something/somewhere.txt'))
        self.assertFalse(AzureFileHandle.supports('C:/local/file.txt'))

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_download(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir/test2.txt')
        local = self.temp_dir / 'local.txt'
        file.download(local)
        self.assertTrue(local.exists())
        with open(local, 'r') as h:
            self.assertEqual(h.read(), 'hello world')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_upload(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new_test.txt')
        with self.temp_data_file('azure_file_shares/share/new_test.txt', metadata=True) as (real_file, real_md):
            self.assertFalse(real_file.exists())
            local = self.temp_dir / 'local.txt'
            with open(local, 'w') as h:
                h.write('what what!')
            file.upload(local)
            self.assertTrue(real_file.exists())
            with open(real_file, 'r') as h:
                self.assertEqual(h.read(), 'what what!')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new_test.txt')
        with self.temp_data_file('azure_file_shares/share/new_test.txt', metadata=True) as (real_file, real_md):
            self.assertFalse(real_file.exists())
            local = self.temp_dir / 'local.txt'
            with open(local, 'w') as h:
                h.write('what what!')
            file.upload(local, metadata={
                'hello': 'world'
            })
            self.assertTrue(real_file.exists())
            with open(real_file, 'r') as h:
                self.assertEqual(h.read(), 'what what!')
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_directory_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir2/')
        with self.temp_data_dir('azure_file_shares/share/subdir2', metadata=True) as (real_file, real_md):
            real_file.mkdir()
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_file_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test99.txt')
        with self.temp_data_file('azure_file_shares/share/test99.txt', metadata=True) as (real_file, real_md):
            real_file.touch()
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_walk_recursive(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/')
        files = [f.name for f in file.iterdir(path_types=PathType.FILE)]
        self.assertIn('test.txt', files)
        self.assertIn('test2.txt', files)
        self.assertIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)

    @injector.test_case({
        AzureClientPool: AzureMockClientPool
    })
    def test_walk_non_recursive(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/')
        files = [f.name for f in file.iterdir(False, path_types=PathType.FILE)]
        self.assertIn('test.txt', files)
        self.assertNotIn('test2.txt', files)
        self.assertNotIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)
