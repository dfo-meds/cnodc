import json
import os
import pathlib
import shutil
import typing
import unittest as ut
from importlib.metadata import metadata
from urllib.parse import urlparse

import zirconium
from autoinject import injector
from azure.core.exceptions import AzureError, ClientAuthenticationError, ResourceNotFoundError, ResourceExistsError
from azure.storage.blob import StandardBlobTier
from urllib3.exceptions import ConnectTimeoutError
from zirconium import test_with_config

from cnodc.storage import StorageController, StorageTier
from cnodc.storage.azure_blob import AzureBlobHandle, wrap_azure_errors
from cnodc.storage.azure_files import AzureFileHandle
from cnodc.storage.base import StorageError
from helpers.base_test_case import BaseTestCase
import datetime


class _Downloader:

    def __init__(self, p: pathlib.Path):
        self._file = p

    def chunks(self) -> typing.Iterable[bytes]:
        with open(self._file, 'rb') as h:
            while b := h.read(1024):
                yield b


class _ShareObjectProperties:

    def __init__(self, name, last_modified, metadata):
        self.name = name
        self.last_modified = datetime.datetime.fromtimestamp(last_modified).astimezone()
        self.metadata = metadata
        self.is_directory = True


class _FileProperties(_ShareObjectProperties):

    def __init__(self, name, last_modified, metadata, size):
        super().__init__(name, last_modified, metadata)
        self.size = size
        self.is_directory = False


class _DirProperties(_ShareObjectProperties): pass


def get_metadata(real_path: pathlib.Path):
    md = {}
    md_file = real_path.parent / f"{real_path.name}.metadata"
    if md_file.exists():
        with open(md_file, 'r') as h:
            md = json.loads(h.read())
    return md


def save_metadata(real_path: pathlib.Path, md: dict):
    md_file = real_path.parent / f"{real_path.name}.metadata"
    with open(md_file, 'w') as h:
        h.write(json.dumps(md))


class _AzureFileClient:

    def __init__(self, account_url: str, share_name: str, file_path: str, real_path: pathlib.Path):
        self.file_path = file_path.split('/')
        self.name = self.file_path[-1]
        self.share_name = share_name
        self.url = f"{account_url}/{share_name}/{file_path}"
        self.directory_name = '/'.join(self.file_path[:-1])

        self._real_path = real_path

    def exists(self):
        return self._real_path.exists()

    def get_file_properties(self) -> _FileProperties:
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        st = self._real_path.stat()
        return _FileProperties(self._real_path.name, st.st_mtime, get_metadata(self._real_path), st.st_size)

    def delete_file(self):
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        self._real_path.unlink()

    def upload_file(self, data: typing.Iterable[bytes]):
        with open(self._real_path, 'wb') as h:
            for b in data:
                h.write(b)

    def download_file(self) -> _Downloader:
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        return _Downloader(self._real_path)

    def set_file_metadata(self, md: dict):
        save_metadata(self._real_path, md)


class _AzureDirectoryClient:

    def __init__(self, account_url: str, share_name: str, file_path: str, real_path: pathlib.Path):
        self.share_name = share_name
        self.directory_path = file_path
        self.url = f"{account_url}/{share_name}/{file_path}"
        self._real_path = real_path

    def exists(self):
        return self._real_path.exists()

    def get_directory_properties(self) -> _DirProperties:
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        return _DirProperties(self._real_path.name, self._real_path.stat().st_mtime, get_metadata(self._real_path))

    def list_directories_and_files(self) -> typing.Iterable[_ShareObjectProperties]:
        for x in os.scandir(self._real_path):
            stat = x.stat()
            if x.is_file():
                yield _FileProperties(x.name, stat.st_mtime, get_metadata(pathlib.Path(x.path)), stat.st_size)
            else:
                yield _DirProperties(x.name, stat.st_mtime, get_metadata(pathlib.Path(x.path)))

    def delete_directory(self):
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        shutil.rmtree(self._real_path)

    def create_directory(self):
        self._real_path.mkdir()

    def set_directory_metadata(self, md: dict):
        save_metadata(self._real_path, md)


class _AzureFileShare:

    def __init__(self, base_url: str, base_path: pathlib.Path, share_name: str):
        self._base_url = base_url.lstrip('/\\')
        self._base_path = base_path
        self._share_name = share_name

    def get_file_client(self, file_path: str):
        file_path = file_path.rstrip('/\\')
        return _AzureFileClient(
            file_path=file_path,
            real_path=self._base_path / file_path,
            share_name=self._share_name,
            account_url='https://test.file.core.windows.net'
        )

    def get_directory_client(self, directory_path: str):
        directory_path = directory_path.rstrip('/\\')
        return _AzureDirectoryClient(
            file_path=directory_path,
            real_path=self._base_path / directory_path,
            share_name=self._share_name,
            account_url='https://test.file.core.windows.net'
        )


@wrap_azure_errors
def make_and_wrap_exception(ex):
    raise ex

AZURE_FILE_SHARES = pathlib.Path(__file__).absolute().resolve().parent.parent / 'test_data/azure_file_shares'


class AzureFilesFixture:

    data: dict[str, _AzureFileShare] = {
        'shares': {}
    }

    @staticmethod
    def build_share(share_name: str):
        if share_name == 'ValueError':
            raise ValueError('oh no')
        if share_name not in AzureFilesFixture.data:
            AzureFilesFixture.data[share_name] = _AzureFileShare('https://test.file.core.windows.net/' + share_name, AZURE_FILE_SHARES / share_name, share_name)
        return AzureFilesFixture.data[share_name]

    @staticmethod
    def from_file_url(url):
        up = urlparse(url)
        pieces = [x for x in up.path.split('/') if x]
        return AzureFilesFixture.build_share(pieces[0]).get_file_client('/'.join(pieces[1:]))

    @staticmethod
    def from_dir_connection_string(conn_str, share_name, directory_path):
        if conn_str == "ValueError":
            raise ValueError("test")
        return AzureFilesFixture.build_share(share_name).get_directory_client(directory_path.strip('/'))

    @staticmethod
    def from_directory_url(url):
        up = urlparse(url)
        pieces = [x for x in up.path.split('/') if x]
        return AzureFilesFixture.build_share(pieces[0]).get_directory_client('/'.join(pieces[1:]))

    @staticmethod
    def from_file_connection_string(conn_str, share_name, file_path):
        if conn_str == "ValueError":
            raise ValueError("test")
        return AzureFilesFixture.build_share(share_name).get_file_client(file_path.strip('/'))


class AzureFileTest(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.originals = {
            '_client_from_connection_string': AzureFileHandle._client_from_connection_string,
            '_client_from_file_url': AzureFileHandle._client_from_file_url,
            '_directory_from_connection_string': AzureFileHandle._directory_from_connection_string,
            '_directory_from_url': AzureFileHandle._directory_from_url
        }
        AzureFileHandle._client_from_connection_string = AzureFilesFixture.from_file_connection_string
        AzureFileHandle._client_from_file_url = AzureFilesFixture.from_file_url
        AzureFileHandle._directory_from_url = AzureFilesFixture.from_directory_url
        AzureFileHandle._directory_from_connection_string = AzureFilesFixture.from_dir_connection_string

    @classmethod
    def tearDownClass(cls):
        super().setUpClass()
        for m in cls.originals:
            setattr(AzureFileHandle, m, cls.originals[m])

    def test_bad_connection_Details(self):
        tests = [
            ("https://hello.blob.core.windows.net/share", 'STORAGE-4001'),
            ("https://test.file.core.windows.net", 'STORAGE-4002')
        ]
        for conn, err_code in tests:
            with self.subTest(conn=conn):
                b = AzureFileHandle(conn)
                with self.assertRaisesCNODCError(err_code):
                    b._get_connection_details()
                with self.assertRaisesCNODCError(err_code):
                    b.file_client()

    def test_bad_az_file_share_file(self):
        b = AzureFileHandle("https://test.file.core.windows.net/ValueError")
        with self.assertRaisesCNODCError('STORAGE-4002'):
            b.file_client()

    def test_bad_az_file_share_dir(self):
        b = AzureFileHandle("https://test.file.core.windows.net/ValueError/")
        with self.assertRaisesCNODCError('STORAGE-4003'):
            b.directory_client()

    @injector.test_case
    @test_with_config(("azure", "storage", "test", 'connection_string'), 'ValueError')
    def test_bad_az_file_share_file(self):
        with self.subTest(msg='file test'):
            b = AzureFileHandle("https://test.file.core.windows.net/share/file.txt")
            with self.assertRaisesCNODCError('STORAGE-4005'):
                b.file_client()
        with self.subTest(msg='dir test'):
            b = AzureFileHandle("https://test.file.core.windows.net/share/")
            with self.assertRaisesCNODCError('STORAGE-4003'):
                b.directory_client()

    def test_general_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test.txt')
        self.assertTrue(file.supports_metadata())
        self.assertFalse(file.supports_tiering())

    @injector.test_case
    @test_with_config(("azure", "storage", "test", "connection_string"), "GoodString")
    def test_dir_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir/')
        self.assertEqual(file.full_path_within_share(), 'subdir')
        self.assertTrue(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name(), 'subdir')
        d = self.testdata_path('azure_file_shares/share/subdir')
        self.assertSameTime(file.modified_datetime(), datetime.datetime.fromtimestamp(d.stat().st_mtime).astimezone())
        self.assertIsNone(file.size())
        with self.assertRaisesCNODCError('STORAGE-4000'):
            file.file_client()

    @injector.test_case
    @test_with_config(("azure", "storage", "test", "connection_string"), "GoodString")
    def test_file_properties(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test.txt')
        self.assertEqual(file.full_path_within_share(), 'test.txt')
        self.assertFalse(file.is_dir())
        self.assertTrue(file.exists())
        self.assertEqual(file.name(), 'test.txt')
        d = self.testdata_path('azure_file_shares/share/test.txt')
        self.assertSameTime(file.modified_datetime(), datetime.datetime.fromtimestamp(d.stat().st_mtime).astimezone())
        self.assertEqual(file.size(), d.stat().st_size)
        with self.assertRaisesCNODCError('STORAGE-4004'):
            file.directory_client()

    def test_remove_file(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new.txt')
        with self.temporary_test_file('azure_file_shares/share/new.txt') as real_file:
            real_file.touch()
            self.assertTrue(file.exists())
            file.remove()
            self.assertFalse(file.exists())
            self.assertFalse(real_file.exists())

    def test_make_and_remove_dir(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/real_dir/')
        with self.temporary_test_directory('azure_file_shares/share/real_dir') as real_dir:
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

    def test_supports(self):
        self.assertTrue(AzureFileHandle.supports('https://test.file.core.windows.net/share/new.txt'))
        self.assertFalse(AzureFileHandle.supports('https://test.blob.core.windows.net/share/new.txt'))
        self.assertFalse(AzureFileHandle.supports('ftp://something/somewhere.txt'))
        self.assertFalse(AzureFileHandle.supports('C:/local/file.txt'))

    def test_download(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir/test2.txt')
        local = self.temp_dir / 'local.txt'
        file.download(local)
        self.assertTrue(local.exists())
        with open(local, 'r') as h:
            self.assertEqual(h.read(), 'hello world')

    def test_upload(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new_test.txt')
        with self.temporary_test_file('azure_file_shares/share/new_test.txt', metadata=True) as (real_file, real_md):
            self.assertFalse(real_file.exists())
            local = self.temp_dir / 'local.txt'
            with open(local, 'w') as h:
                h.write('what what!')
            file.upload(local)
            self.assertTrue(real_file.exists())
            with open(real_file, 'r') as h:
                self.assertEqual(h.read(), 'what what!')

    def test_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/new_test.txt')
        with self.temporary_test_file('azure_file_shares/share/new_test.txt', metadata=True) as (real_file, real_md):
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

    def test_directory_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/subdir2/')
        with self.temporary_test_directory('azure_file_shares/share/subdir2', metadata=True) as (real_file, real_md):
            real_file.mkdir()
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')

    def test_file_metadata(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/test99.txt')
        with self.temporary_test_file('azure_file_shares/share/test99.txt', metadata=True) as (real_file, real_md):
            real_file.touch()
            file.set_metadata({'hello': 'world'})
            md = file.get_metadata()
            self.assertIn('hello', md)
            self.assertEqual(md['hello'], 'world')

    def test_walk_recursive(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/')
        files = [f.name() for f in file.walk()]
        self.assertIn('test.txt', files)
        self.assertIn('test2.txt', files)
        self.assertIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)

    def test_walk_non_recursive(self):
        file = AzureFileHandle.build('https://test.file.core.windows.net/share/')
        files = [f.name() for f in file.walk(False)]
        self.assertIn('test.txt', files)
        self.assertNotIn('test2.txt', files)
        self.assertNotIn('test5.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)
