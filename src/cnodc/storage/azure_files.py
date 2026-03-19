"""Support for Azure File Shares.


There are three methods of providing Azure credentials to authenticate:

#1: Provide them in the Zirconium configuration file:

```
[azure.storage.STORAGE_ACCOUNT_NAME]
connection_string = "..."
```


#2: Provide your credentials in a method supported by DefaultAzureCredentials

#3: Include the SAS token in the URL (not recommended)

"""
import datetime
from .base import UrlBaseHandle, StorageTier, StorageError
from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient, FileProperties, DirectoryProperties
from azure.core.exceptions import ResourceNotFoundError
from cnodc.util import HaltFlag, CNODCError
import typing as t
from urllib.parse import urlparse
import zirconium as zr
from autoinject import injector
from .azure_blob import wrap_azure_errors
from ..util.awaretime import awaretime, AwareDateTime


class AzureFileHandle(UrlBaseHandle):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, url, properties=None, *args, **kwargs):
        super().__init__(url, *args, **kwargs)
        self._cached_properties['properties'] = properties

    def get_connection_details(self) -> dict:
        """Get connection information from the configuration about the URL."""
        return self._with_cache("connection_details", self._get_connection_details)

    def _get_connection_details(self) -> dict:
        url_parts = self.parse_url()
        domain = url_parts.hostname
        if not domain.endswith(".file.core.windows.net"):
            raise StorageError(f"Invalid hostname", 4001)
        path_parts = [x.strip() for x in url_parts.path.lstrip('/').split('/') if x.strip()]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise StorageError(f"Missing share name", 4002)
        kwargs = {
            "storage_account": domain[:-22],
            "storage_url": domain,
            "share_name": path_parts[0],
            "connection_string": self.config.as_str(("azure", "storage", domain[:-22], "connection_string"), default=None),
            "file_path": "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        }
        return kwargs

    def file_client(self) -> ShareFileClient:
        """Build a file share client."""
        try:
            if self._is_dir():
                raise StorageError(f"Cannot make file client on a directory", 4000)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureFileHandle._client_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    file_path=connection_info["file_path"]
                )
            else:
                return AzureFileHandle._client_from_file_url(self._url)
        except ValueError as ex:
            raise StorageError(f"Could not create file client", 4005) from ex

    def directory_client(self) -> ShareDirectoryClient:
        """Build a directory client."""
        try:
            if not self._is_dir():
                raise StorageError(f"Cannot make directory client on a file", 4004)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureFileHandle._directory_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    directory_path=connection_info["file_path"]
                )
            else:
                return AzureFileHandle._directory_from_url(self._url)
        except ValueError as ex:
            raise StorageError(f"Could not create directory client", 4003) from ex

    def _mkdir(self, mode):
        self.directory_client().create_directory()

    def _exists(self) -> bool:
        if self.is_dir():
            client = self.directory_client()
            return client.exists()
        else:
            client = self.file_client()
            return client.exists()

    def _is_dir(self) -> bool:
        part1, _ = self._split_url()
        return part1.endswith('/')

    def _name(self) -> str:
        part1, _ = self._split_url()
        if part1.endswith('/'):
            part1 = part1[:-1]
        pieces = part1.split('/')
        return pieces[-1]

    @wrap_azure_errors
    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.file_client().download_file()
        for chunk in HaltFlag._iterate(stream.chunks(), self._halt_flag, True):
            yield chunk

    @wrap_azure_errors
    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        storage_tier = None
        metadata = metadata or {}
        self._add_default_metadata(metadata, storage_tier)
        args = {
            'data': self._local_read_chunks(local_path, buffer_size),
        }
        client_ = self.file_client()
        client_.upload_file(**args)
        if metadata:
            client_.set_file_metadata(metadata)
        self.clear_cache()

    @wrap_azure_errors
    def remove(self):
        if self._is_dir():
            self.directory_client().delete_directory()
        else:
            self.file_client().delete_file()
        self.clear_cache()

    @wrap_azure_errors
    def full_path_within_share(self):
        if self._is_dir():
            return self.directory_client().directory_path
        else:
            return '/'.join(self.file_client().file_path)

    def supports_metadata(self) -> bool:
        return True

    def supports_tiering(self) -> bool:
        return False

    @wrap_azure_errors
    def walk(self, recursive: bool = True) -> t.Iterable[t.Self]:
        client = self.directory_client()
        more_work: list[AzureFileHandle] = []
        for file in HaltFlag._iterate(client.list_directories_and_files(), self._halt_flag, True):
            if not file.is_directory:
                yield self.child(file.name, False)
            elif recursive:
                dh = self.child(file.name, True)
                if recursive:
                    more_work.append(dh)
        for sub_dir in more_work:
            yield from sub_dir.walk(recursive)

    def file_properties(self, clear_cache: bool = False) -> FileProperties:
        """Retrieve the file properties from Azure."""
        return self._with_cache('file_properties', self._file_properties, clear_cache=clear_cache)

    def dir_properties(self, clear_cache: bool = False) -> DirectoryProperties:
        """Retrieve the directory properties from Azure."""
        return self._with_cache('dir_properties', self._directory_properties, clear_cache=clear_cache)

    @wrap_azure_errors
    def _file_properties(self):
        return self.file_client().get_file_properties()

    @wrap_azure_errors
    def _directory_properties(self):
        return self.directory_client().get_directory_properties()

    def size(self, clear_cache: bool = False) -> t.Optional[int]:
        if self._is_dir():
            return None
        return self.file_properties(clear_cache).size

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        if self._is_dir():
            return AwareDateTime.from_datetime(self.dir_properties(clear_cache).last_modified, 'Etc/UTC')
        return AwareDateTime.from_datetime(self.file_properties(clear_cache).last_modified, 'Etc/UTC')

    @wrap_azure_errors
    def set_metadata(self, metadata: dict[str, str]):
        if self._is_dir():
            self.directory_client().set_directory_metadata(metadata)
        else:
            self.file_client().set_file_metadata(metadata)
        self.clear_cache()

    def get_metadata(self, clear_cache: bool = False) -> dict[str, str]:
        if self._is_dir():
            return self.dir_properties(clear_cache).metadata
        else:
            return self.file_properties(clear_cache).metadata

    @staticmethod
    def _client_from_connection_string(conn_str, share_name, file_path): #pragma: no coverage (requires connectivity)
        return ShareFileClient.from_connection_string(conn_str, share_name, file_path)

    @staticmethod
    def _client_from_file_url(url): #pragma: no coverage (requires connectivity)
        return ShareFileClient.from_file_url(url)

    @staticmethod
    def _directory_from_connection_string(conn_str, share_name, directory_path): #pragma: no coverage (requires connectivity)
        return ShareDirectoryClient.from_connection_string(conn_str, share_name, directory_path)

    @staticmethod
    def _directory_from_url(url):  #pragma: no coverage (requires connectivity)
        return ShareDirectoryClient.from_directory_url(url)

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname.endswith(".file.core.windows.net")
