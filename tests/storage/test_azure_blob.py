import json
import os
import pathlib
import unittest as ut
from urllib.parse import urlparse

from azure.storage.blob import StandardBlobTier

from cnodc.storage.azure_blob import AzureBlobHandle
from cnodc.storage.azure_files import AzureFileHandle
from core import BaseTestCase


class _FakeBlobDownloader:

    def __init__(self, local_path):
        self.local_path = local_path

    def chunks(self):
        with open(self.local_path, 'rb') as h:
            data = h.read(1024)
            while data != b'':
                yield data
                data = h.read(1024)


class _BlobProperties:

    def __init__(self, name, size, lmt, metadata, blob_tier):
        self.name = name
        self.size = size
        self.last_modified = lmt
        self.metadata = metadata
        self.blob_Tier = blob_tier

class _AzureBlob:

    def __init__(self, container, name:str, local_path: pathlib.Path):
        self.url = container._base_url + name
        self.container = container
        self.name = name
        self.blob_name = name
        self.local_path = local_path

    def get_blob_properties(self):
        md = self._load_metadata()
        t = StandardBlobTier.HOT
        if '__tier' in md:
            if md['__tier'] == 'a':
                t = StandardBlobTier.ARCHIVE
            elif md['__tier'] == 'c':
                t = StandardBlobTier.COOL
        stats = self.local_path.stat()
        return _BlobProperties(
            self.blob_name,
            stats.st_size,
            stats.st_mtime,
            {k: md[k] for k in md if k != '__tier'},
            t
        )

    def exists(self):
        return self.local_path.exists()

    def download_blob(self):
        return _FakeBlobDownloader(self.local_path)

    def _load_metadata(self):
        metadata_path = pathlib.Path(str(self.local_path) + ".metadata")
        if not metadata_path.exists():
            return {}
        with open(metadata_path, "r") as h:
            json.loads(h.read())

    def update_metadata(self, new_metadata):
        md = self._load_metadata()
        md.update(new_metadata)
        metadata_path = pathlib.Path(str(self.local_path) + ".metadata")
        with open(metadata_path, "w") as h:
            h.write(json.dumps(md))

    def upload_blob(self, data, metadata=None, standard_blob_tier=None):
        if not self.local_path.parent.exists():
            self.local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.local_path, 'wb') as h:
            for chunk in data:
                h.write(chunk)
        if metadata:
            self.update_metadata(metadata)
        if standard_blob_tier:
            self.set_standard_blob_tier(standard_blob_tier)

    def set_standard_blob_tier(self, tier):
        if tier:
            metadata = self._load_metadata()
            if tier == StandardBlobTier.ARCHIVE:
                metadata['__tier'] = 'a'
            elif tier == StandardBlobTier.COOL:
                metadata['__tier'] = 'c'
            elif tier == StandardBlobTier.HOT:
                metadata['__tier'] = 'h'
            self.update_metadata(metadata)

    def delete_blob(self):
        self.local_path.unlink(True)
        pathlib.Path(str(self.local_path) + ".metadata").unlink()


class _AzureContainer:

    def __init__(self, base_url: str, base_path: pathlib.Path):
        self._base_url = base_url
        self._base_path = base_path
        self._blobs = {}

    def list_blobs(self, name_starts_with: str):
        work = [self._base_path.absolute()]
        bp_len = len(str(work[0]))
        while work:
            dir_path = work.pop()
            for file in os.scandir(dir_path):
                full_path = dir_path / file.name
                if full_path.is_dir():
                    work.append(full_path)
                else:
                    rel_path = str(full_path)[bp_len:].replace("\\", "/")
                    if (not name_starts_with) or rel_path.startswith(name_starts_with):
                        yield self.get_blob_client(rel_path).get_blob_properties()


    def get_blob_client(self, blob_name) -> _AzureBlob:
        if blob_name not in self._blobs:
            self._blobs[blob_name] = _AzureBlob(self, blob_name, self._base_path / blob_name.strip('/'))
        return self._blobs[blob_name]


class AzureBlobFixture:

    data = {
        'containers': {}
    }

    @staticmethod
    def build_container(container_name: str):
        if container_name not in AzureBlobFixture.data:
            path = pathlib.Path(__file__).absolute().parent / 'azure_test'
            AzureBlobFixture.data[container_name] = _AzureContainer('https://test.blob.core.windows/' + container_name, path / container_name)
        return AzureBlobFixture.data[container_name]

    @staticmethod
    def from_blob_url(url):
        up = urlparse(url)
        pieces = [x for x in up.path.split('/') if x]
        return AzureBlobFixture.build_container(pieces[0]).get_blob_client('/'.join(pieces[1:]))

    @staticmethod
    def from_blob_connection_string(conn_str, container_name, blob_name):
        return AzureBlobFixture.build_container(container_name).get_blob_client(blob_name)

    @staticmethod
    def from_container_url(url):
        up = urlparse(url)
        pieces = [x for x in up.path.split('/') if x]
        return AzureBlobFixture.build_container(pieces[0])

    @staticmethod
    def from_container_connection_string(conn_str, container_name):
        return AzureBlobFixture.build_container(container_name)


class BlobTest(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.originals = {
            '_blob_client_from_url': AzureBlobHandle._blob_client_from_url,
            '_blob_client_from_connection_string': AzureBlobHandle._blob_client_from_connection_string,
            '_container_client_from_url': AzureBlobHandle._container_client_from_url,
            '_container_client_from_connection_string': AzureBlobHandle._container_client_from_connection_string
        }
        AzureBlobHandle._blob_client_from_url = AzureBlobFixture.from_blob_url
        AzureBlobHandle._blob_client_from_connection_string = AzureBlobFixture.from_blob_connection_string
        AzureBlobHandle._container_client_from_url = AzureBlobFixture.from_container_url
        AzureBlobHandle._container_client_from_connection_string = AzureBlobFixture.from_container_connection_string

    @classmethod
    def tearDownClass(cls):
        super().setUpClass()
        for m in cls.originals:
            setattr(AzureBlobHandle, m, cls.originals[m])

    def test_properties(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test.txt')
        self.assertTrue(file.supports_tiering())
        self.assertTrue(file.supports_metadata())

    def test_existing_file(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test.txt')
        self.assertTrue(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name(), 'test.txt')
        self.assertEqual(file.size(), 5)
        self.assertIsNotNone(file.modified_datetime())

    def test_non_existing_file(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test3.txt')
        self.assertFalse(file.exists())
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name(), 'test3.txt')
        self.assertIsNone(file.size())
        self.assertIsNone(file.modified_datetime())

    def test_dir(self):
        file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test/foo/bar/')
        self.assertTrue(file.exists())
        self.assertTrue(file.is_dir())
        self.assertEqual(file.name(), 'bar')

    def test_upload(self):
        blob_path = pathlib.Path(__file__).absolute().parent / 'azure_test' / 'container' / 'test2.txt'
        metadata_path = pathlib.Path(__file__).absolute().parent / 'azure_test' / 'container' / 'test2.txt.metadata'
        try:
            fp = self.temp_dir / 'bar.txt'
            with open(fp, 'w') as h:
                h.write('hello world')
            file = AzureBlobHandle.build('https://test.blob.core.windows.net/container/test2.txt')
            self.assertFalse(file.exists())
            self.assertFalse(blob_path.exists())
            file.upload(fp)
            self.assertTrue(blob_path.exists())
            self.assertTrue(file.exists())
        finally:
            blob_path.unlink(True)
            metadata_path.unlink(True)

    def test_walk(self):
        blob = AzureBlobHandle.build('https://test.blob.core.windows.net/container')
        files = [file.path() for file in blob.walk()]
        self.assertIn("https://test.blob.core.windows/container/test.txt", files)
        self.assertIn("https://test.blob.core.windows/container/subdir/test2.txt", files)
        self.assertIn("https://test.blob.core.windows/container/subdir/subdir2/test5.txt", files)
        self.assertNotIn("https://test.blob.core.windows/container/subdir/", files)

