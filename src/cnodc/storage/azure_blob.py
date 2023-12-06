import functools
import datetime
import requests
import urllib3.exceptions
from .base import UrlBaseHandle, StorageTier, BaseStorageHandle, StorageError
from azure.storage.blob import BlobClient, StandardBlobTier, ContainerClient, BlobProperties
from azure.identity import DefaultAzureCredential
from cnodc.util import HaltFlag, CNODCError
import typing as t
from urllib.parse import urlparse
import azure.core.exceptions as ace
import zirconium as zr
from autoinject import injector


def wrap_azure_errors(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except ace.AzureError as ex:
            if ex.inner_exception is not None:
                if isinstance(ex.inner_exception, urllib3.exceptions.ConnectTimeoutError):
                    raise StorageError(f"Azure: Connection timeout error: {ex.__class__.__name__}: {str(ex)}", 2001, True) from ex
                elif isinstance(ex.inner_exception, requests.ConnectionError):
                    raise StorageError(f"Azure: Connection error: {ex.__class__.__name__}: {str(ex)}", 2002, True) from ex
                elif isinstance(ex, ace.ClientAuthenticationError):
                    raise StorageError(f"Azure: lient authentication error: {ex.__class__.__name__}: {str(ex)}", 2003, True) from ex
                elif isinstance(ex, ace.ResourceNotFoundError):
                    raise StorageError(f"Azure: Resource not found error: {ex.__class__.__name__}: {str(ex)}", 2004, True) from ex
                elif isinstance(ex, ace.ResourceExistsError):
                    raise StorageError(f"Azure: Resource already exists error: {ex.__class__.__name__}: {str(ex)}", 2005, True) from ex

            raise StorageError(f"Azure: {ex.__class__.__name__}: {str(ex)}", 2000) from ex

    return _inner


class AzureBlobHandle(UrlBaseHandle):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, url, properties=None, *args, **kwargs):
        super().__init__(url, *args, **kwargs)
        self._cached_properties['properties'] = properties

    def get_connection_details(self) -> dict:
        return self._with_cache("connection_details", self._get_connection_details)

    def _get_connection_details(self) -> dict:
        url_parts = self.parse_url()
        domain = url_parts.hostname
        if not domain.endswith(".blob.core.windows.net"):
            raise CNODCError(f"Invalid hostname", "AZBLOB", 1001)
        path_parts = [x for x in url_parts.path.lstrip('/').split('/')]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise CNODCError(f"Missing container name", "AZBLOB", 1002)
        kwargs = {
            "storage_account": domain[:-22],
            "storage_url": domain,
            "container_name": path_parts[0],
            "connection_string": self.config.as_str(("azure", "storage", domain[:-22], "connection_string"), default=None),
            "blob_name": "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        }
        return kwargs

    def client(self) -> BlobClient:
        try:
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return BlobClient.from_connection_string(
                    conn_str=connection_info["connection_string"],
                    container_name=connection_info["container_name"],
                    blob_name=connection_info["blob_name"]
                )
            else:
                return BlobClient.from_blob_url(self._url, credential=DefaultAzureCredential())
        except ValueError as ex:
            raise CNODCError(f"Could not create blob client", "AZBLOB", 1000) from ex

    def container_client(self) -> ContainerClient:
        try:
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return ContainerClient.from_connection_string(
                    conn_str=connection_info["connection_string"],
                    container_name=connection_info["container_name"]
                )
            else:
                return ContainerClient.from_container_url(
                    container_url=f"https://{connection_info['storage_url']}/{connection_info['container_name']}",
                    credential=DefaultAzureCredential()
                )
        except ValueError as ex:
            raise CNODCError(f"Could not create container client", "AZBLOB", 1001) from ex

    @wrap_azure_errors
    def _exists(self) -> bool:
        if self._is_dir():
            return True
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

    @wrap_azure_errors
    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.client().download_blob()
        for chunk in HaltFlag.iterate(stream.chunks(), self._halt_flag, True):
            yield chunk

    def _write_chunks(self, chunks: t.Iterable[bytes], halt_flag: HaltFlag = None):
        pass

    @wrap_azure_errors
    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None,
               halt_flag: t.Optional[HaltFlag] = None):
        self._add_default_metadata(metadata, storage_tier)
        args = {
            'data': self._local_read_chunks(local_path, buffer_size),
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

    @wrap_azure_errors
    def remove(self):
        if not self._is_dir():
            self.client().delete_blob()
            self.clear_cache()
        else:
            raise NotImplementedError(f"Deleting a blob directory isn't supported")

    @wrap_azure_errors
    def full_name(self):
        return self.client().blob_name

    def supports_metadata(self) -> bool:
        return True

    def supports_tiering(self) -> bool:
        return True

    @wrap_azure_errors
    def walk(self, recursive: bool = True, files_only: bool = True) -> t.Iterable:
        client = self.container_client()
        full_name = self.full_name()
        if full_name[-1] != '/':
            full_name += '/'
        if not recursive:
            raise NotImplementedError(f"Non-recursive iteration on blobs not implemented")
        if not files_only:
            raise NotImplementedError(f"Returning directories while iterating on blobs not implemented")
        for blob_properties in HaltFlag.iterate(client.list_blobs(name_starts_with=full_name), self._halt_flag, True):
            # TODO: recursive=False isn't handled
            # TODO: files_only=False isn't handled
            bc = client.get_blob_client(blob_properties.name)
            yield AzureBlobHandle(bc.url, blob_properties, halt_flag=self._halt_flag)

    def properties(self, clear_cache: bool = False) -> BlobProperties:
        return self._with_cache('properties', self._properties, clear_cache=clear_cache)

    @wrap_azure_errors
    def _properties(self):
        return self.client().get_blob_properties()

    def size(self, clear_cache: bool = False) -> int:
        return self.properties(clear_cache).size

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        return self.properties(clear_cache).last_modified

    @wrap_azure_errors
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

    @wrap_azure_errors
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
