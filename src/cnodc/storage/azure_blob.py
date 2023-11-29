import pathlib
import datetime
from .base import UrlBaseHandle, StorageTier, DirFileHandle
from azure.storage.blob import BlobClient, StandardBlobTier, ContainerClient, BlobProperties
from cnodc.util import HaltFlag
import typing as t
from urllib.parse import urlparse


class AzureBlobHandle(UrlBaseHandle):

    def __init__(self, url, properties=None):
        super().__init__(url)
        self._cached_properties['properties'] = properties

    def client(self) -> BlobClient:
        # TODO: doesn't actually work, use from_connection_string and load credentials
        # from config
        return BlobClient.from_blob_url(self._url)

    def container_client(self) -> ContainerClient:
        # TODO
        pass

    def _exists(self) -> bool:
        return self.client().exists()

    def _is_dir(self) -> bool:
        part1, _ = self._split_url()
        return part1.endswith('/')

    def _name(self) -> str:
        part1, _ = self._split_url()
        if part1.endswith('/'):
            part1 = part1[:-1]
        last_slash = part1.rfind('/')
        return part1[last_slash+1:]

    def _read_chunks(self, buffer_size: int = None, halt_flag: HaltFlag = None) -> t.Iterable[bytes]:
        stream = self.client().download_blob()
        for chunk in stream.chunks():
            halt_flag.check_continue(True)
            yield chunk

    def _write_chunks(self, chunks: t.Iterable[bytes], halt_flag: HaltFlag = None):
        pass

    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None,
               halt_flag: t.Optional[HaltFlag] = None):
        DirFileHandle.add_default_metadata(metadata, storage_tier)
        args = {
            'data': DirFileHandle._local_read_chunks(local_path, buffer_size, halt_flag),
        }
        if metadata:
            args['metadata'] = metadata
        if storage_tier == StorageTier.ARCHIVAL:
            args['standard_blob_tier'] = StandardBlobTier.ARCHIVE
        elif storage_tier == StorageTier.INFREQUENT:
            args['standard_blob_tier'] = StandardBlobTier.COOL
        elif storage_tier == StorageTier.FREQUENT:
            args['standard_blob_tier'] = StandardBlobTier.HOT
        client_ = self.client()
        client_.upload_blob(**args)
        self.clear_cache()

    def remove(self):
        self.client().delete_blob()
        self.clear_cache()

    def full_name(self):
        return self.client().blob_name

    def supports_metadata(self) -> bool:
        return True

    def supports_tiering(self) -> bool:
        return True

    def walk(self, recursive: bool = True, files_only: bool = True, halt_flag: HaltFlag = None) -> t.Iterable:
        client = self.container_client()
        full_name = self.full_name()
        if full_name[-1] != '/':
            full_name += '/'
        for blob_properties in client.list_blobs(name_starts_with=full_name):
            halt_flag.check_continue(True)
            # TODO: recursive=False isn't handled
            # TODO: files_only=False isn't handled
            bc = client.get_blob_client(blob_properties.name)
            yield AzureBlobHandle(bc.url, blob_properties)

    def properties(self, clear_cache: bool = False) -> BlobProperties:
        return self._with_cache('properties', self._properties, clear_cache=clear_cache)

    def _properties(self):
        return self.client().get_blob_properties()

    def size(self, clear_cache: bool = False) -> int:
        return self.properties(clear_cache).size

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        return self.properties(clear_cache).last_modified

    def set_metadata(self, metadata: dict[str, str]):
        self.client().set_blob_metadata(metadata)
        self.clear_cache()

    def get_metadata(self, clear_cache: bool = False) -> dict[str, str]:
        return self.properties(clear_cache).metadata

    def get_tier(self, clear_cache: bool = False) -> t.Optional[StorageTier]:
        tier = self.properties(clear_cache).blob_tier
        if tier == StandardBlobTier.HOT:
            return StorageTier.FREQUENT
        elif tier == StandardBlobTier.COOL:
            return StorageTier.INFREQUENT
        elif tier == StandardBlobTier.ARCHIVE:
            return StorageTier.ARCHIVAL
        return None

    def set_tier(self, tier: StorageTier):
        current_tier = self.get_tier()
        if current_tier != tier:
            if tier == StorageTier.ARCHIVAL:
                self.client().set_standard_blob_tier(StandardBlobTier.ARCHIVE)
                self.clear_cache()
            elif tier == StorageTier.INFREQUENT:
                self.client().set_standard_blob_tier(StandardBlobTier.COOL)
                self.clear_cache()
            elif tier == StorageTier.FREQUENT:
                self.client().set_standard_blob_tier(StandardBlobTier.HOT)
                self.clear_cache()

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname.endswith(".blob.core.windows.net")
