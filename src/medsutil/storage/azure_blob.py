"""Support for Azure Blob Storage.

There are three methods of providing Azure credentials to authenticate:

#1: Provide them in the Zirconium configuration file:

```
[azure.storage.STORAGE_ACCOUNT_NAME]
connection_string = "..."
```


#2: Provide your credentials in a method supported by DefaultAzureCredentials

#3: Include the SAS token in the URL (not recommended)


"""
import pathlib

from medsutil.awaretime import AwareDateTime
from medsutil.storage.azure import wrap_azure_errors, AzureBaseHandle
from medsutil.storage.interface import StorageError, FeatureFlag, StatResult
from medsutil.storage import StorageTier
from azure.storage.blob import BlobClient, StandardBlobTier, ContainerClient
import typing as t
from urllib.parse import urlparse
import zirconium as zr
from autoinject import injector
import medsutil.types as ct


class AzureBlobHandle(AzureBaseHandle):
    """Handle class for Azure blobs"""

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, supports=FeatureFlag.DEFAULT | FeatureFlag.TIERING | FeatureFlag.METADATA, **kwargs)

    def get_connection_details(self) -> dict:
        """Get connection information from the configuration about the URL."""
        return self._with_cache("connection_details", self._get_connection_details)

    def _get_connection_details(self) -> dict:
        url_parts = self.parse_url()
        domain = url_parts.hostname
        if domain is None or not domain.endswith(".blob.core.windows.net"):
            raise StorageError(f"Invalid hostname", 3006)
        path_parts = [x for x in url_parts.path.lstrip('/').split('/')]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise StorageError(f"Missing container name", 3007)
        kwargs = self._get_storage_account_connection_details(domain)
        kwargs.update({
            "container_name": path_parts[0],
            "blob_name": "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        })
        return kwargs

    def client(self) -> BlobClient:
        """Build a blob client."""
        try:
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureBlobHandle._blob_client_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    container_name=connection_info["container_name"],
                    blob_name=connection_info["blob_name"]
                )
            else:
                return AzureBlobHandle._blob_client_from_url(self._path)
        except ValueError as ex:
            raise StorageError(f"Could not create blob client", 3008) from ex

    def container_client(self) -> ContainerClient:
        """Build a container client."""
        try:
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureBlobHandle._container_client_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    container_name=connection_info["container_name"]
                )
            else:
                return AzureBlobHandle._container_client_from_url(
                    f"https://{connection_info['storage_url']}/{connection_info['container_name']}",
                )
        except ValueError as ex:
            raise StorageError(f"Could not create container client", 3009) from ex

    def code_tier(self, tier: StandardBlobTier | None) -> StorageTier | None:
        if tier == StandardBlobTier.HOT:
            return StorageTier.FREQUENT
        elif tier == StandardBlobTier.COOL:
            return StorageTier.INFREQUENT
        elif tier == StandardBlobTier.ARCHIVE:
            return StorageTier.ARCHIVAL
        return None

    def decode_tier(self, tier: StorageTier | None) -> StandardBlobTier:
        if tier is StorageTier.FREQUENT:
            return StandardBlobTier.HOT
        elif tier == StorageTier.INFREQUENT:
            return StandardBlobTier.COOL
        elif tier == StorageTier.ARCHIVAL:
            return StandardBlobTier.ARCHIVE
        return StandardBlobTier.HOT

    @wrap_azure_errors
    def _stat(self) -> StatResult:
        client = self.client()
        if client.exists():
            props = self.client().get_blob_properties()
            return StatResult(
                exists=True,
                is_file=True,
                is_dir=False,
                metadata=props.metadata,
                st_size=props.size,
                st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                tier=self.code_tier(props.blob_tier)
            )
        elif self._no_remote_is_dir():
            return StatResult(exists=True, is_dir=True, is_file=False)
        return StatResult(exists=False)

    @wrap_azure_errors
    def streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.client().download_blob()
        yield from self._halt_flag.iterate(stream.chunks())

    def streaming_write(self, chunks: t.Iterable[bytes]):
        self._upload(chunks)

    @wrap_azure_errors
    def _upload(self,
               local_path: pathlib.Path | ct.SupportsBinaryRead | t.Iterable[t.ByteString],
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        args: dict[str, t.Any] = {
            'data': self._local_read_chunks(local_path, buffer_size),
            'standard_blob_tier': self.decode_tier(storage_tier)
        }
        if metadata:
            args['metadata'] = metadata
        client_ = self.client()
        client_.upload_blob(**args)
        self.clear_cache()

    @wrap_azure_errors
    def remove(self):
        if not self.is_dir():
            self.client().delete_blob()
            self.clear_cache()

    @wrap_azure_errors
    def full_name(self):
        return self.client().blob_name

    @wrap_azure_errors
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        client = self.container_client()
        full_name = self.full_name()
        if not full_name:
            full_name = '/'
        if full_name[-1] != '/':  # pragma: no coverage (just a fallback)
            full_name += '/'
        organized = {
            '_full_name': full_name,
            '_dirs': {},
            '_files': []
        }
        for blob_properties in self._halt_flag.iterate(client.list_blobs(name_starts_with=full_name)):
            bc = client.get_blob_client(blob_properties.name)
            relative_name = bc.blob_name[len(full_name):]
            dir_list = relative_name.split('/')
            file_name = dir_list.pop(-1)
            current = organized
            for d in dir_list:
                if d not in current["_dirs"]:
                    current["_dirs"][d] = {'_dirs': {}, '_files': [], '_full_name': f'{current['_full_name']}/{d}'}
                current = current["_dirs"][d]
            current['_files'].append(file_name)
        yield from self._yield_all_dir_tuples(organized)

    def _yield_all_dir_tuples(self, d: dict[str, t.Any]) -> t.Iterable[tuple[str, list[str], list[str]]]:
        yield d['_full_name'], list(d['_dirs'].keys()), d['_files']
        for subd_key in d['_dirs']:
            yield from self._yield_all_dir_tuples(d['_dirs'][subd_key])

    @wrap_azure_errors
    def set_metadata(self, metadata: dict[str, str]):
        self.client().set_blob_metadata(metadata)
        self.clear_cache()

    def mkdir(self, mode=0o777, parents: bool = True):
        # never need to worry about making directories here.
        pass

    @wrap_azure_errors
    def set_tier(self, tier: StorageTier):
        current_tier = self.get_tier()
        if current_tier != tier:
            self.client().set_standard_blob_tier(self.decode_tier(tier))

    @staticmethod
    def _blob_client_from_url(url: str, *args, **kwargs):
        return BlobClient.from_blob_url(url, *args, **kwargs)  # pragma: no coverage (hard to test without sub)

    @staticmethod
    def _blob_client_from_connection_string(conn_str: str, container_name: str, blob_name: str):
        return BlobClient.from_connection_string(conn_str, container_name, blob_name)  # pragma: no coverage (hard to test without sub)

    @staticmethod
    def _container_client_from_url(url: str, *args, **kwargs):
        return ContainerClient.from_container_url(url, *args, **kwargs)  # pragma: no coverage (hard to test without sub)

    @staticmethod
    def _container_client_from_connection_string(conn_str: str, container_name: str):
        return ContainerClient.from_connection_string(conn_str, container_name)  # pragma: no coverage (hard to test without sub)

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname is not None and pieces.hostname.endswith(".blob.core.windows.net")
