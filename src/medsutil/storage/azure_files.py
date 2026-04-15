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
import pathlib
import typing as t
from urllib.parse import urlparse

from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient

import medsutil.types as ct
from medsutil.awaretime import AwareDateTime
from medsutil.storage.azure import AzureBaseHandle, wrap_azure_errors
from medsutil.storage.interface import StorageError, StorageTier, FeatureFlag, StatResult


class AzureFileHandle(AzureBaseHandle):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, supports=FeatureFlag.DEFAULT | FeatureFlag.METADATA, **kwargs)

    def get_connection_details(self) -> dict:
        """Get connection information from the configuration about the URL."""
        return self._with_cache("connection_details", self._get_connection_details)

    def _get_connection_details(self) -> dict:
        url_parts = self.parse_url()
        domain = url_parts.hostname
        if domain is None or not domain.endswith(".file.core.windows.net"):
            raise StorageError(f"Invalid hostname", 4001)
        path_parts = [x.strip() for x in url_parts.path.lstrip('/').split('/') if x.strip()]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise StorageError(f"Missing share name", 4002)
        kwargs = self._get_storage_account_connection_details(domain)
        kwargs.update({
            "share_name": path_parts[0],
            "file_path": "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        })
        return kwargs

    def file_client(self) -> ShareFileClient:
        """Build a file share client."""
        try:
            if self._no_remote_is_dir() is not True:
                raise StorageError(f"Cannot make file client on a directory", 4003)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureFileHandle._client_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    file_path=connection_info["file_path"]
                )
            else:
                return AzureFileHandle._client_from_file_url(self._path)
        except ValueError as ex:
            raise StorageError(f"Could not create file client", 4101) from ex

    def directory_client(self) -> ShareDirectoryClient:
        """Build a directory client."""
        try:
            if self._no_remote_is_dir() is True:
                raise StorageError(f"Cannot make directory client on a file", 4004)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return AzureFileHandle._directory_from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    directory_path=connection_info["file_path"]
                )
            else:
                return AzureFileHandle._directory_from_url(self._path)
        except ValueError as ex:
            raise StorageError(f"Could not create directory client", 4003) from ex

    def _mkdir(self, mode: int = 0o777):
        self.directory_client().create_directory()

    @wrap_azure_errors
    def streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.file_client().download_file()
        yield from self._halt_flag.iterate(stream.chunks())

    @wrap_azure_errors
    def _upload(self,
               local_path: pathlib.Path | str | ct.SupportsBinaryRead | t.Iterable[t.ByteString],
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        args: dict[str, t.Any] = {
            'data': self._local_read_chunks(local_path, buffer_size),
        }
        if metadata:
            args['metadata'] = metadata
        client_ = self.file_client()
        client_.upload_file(**args)
        self.clear_cache()

    def streaming_write(self, chunks: t.Iterable[t.ByteString]):
        self._upload(chunks)

    @wrap_azure_errors
    def remove(self):
        if self.is_dir():
            self.directory_client().delete_directory()
        else:
            self.file_client().delete_file()
        self.clear_cache()

    @wrap_azure_errors
    def full_path_within_share(self):
        if self.is_dir():
            return self.directory_client().directory_path
        else:
            return '/'.join(self.file_client().file_path)

    @wrap_azure_errors
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        client = self.directory_client()
        files: list[str] = []
        dirs: list[str] = []
        for file in self._halt_flag.iterate(client.list_directories_and_files()):
            if not file.is_directory:
                files.append(file.name)
            else:
                dirs.append(file.name)
        yield self._path, dirs, files
        for d in dirs:
            yield from self.subdir(d)._walk()

    def _stat(self) -> StatResult:
        if self._no_remote_is_dir() is True:
            with self.directory_client() as directory:
                if not directory.exists():
                    return StatResult(exists=False)
                props = directory.get_directory_properties()
                return StatResult(
                    exists=True,
                    st_size=None,
                    st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                    is_dir=True,
                    is_file=False,
                    metadata=props.metadata,
                )
        with self.file_client() as client:
            if not client.exists():
                return StatResult(exists=False)
            props = client.get_file_properties()
            return StatResult(
                exists=True,
                is_dir=False,
                is_file=True,
                metadata=props.metadata,
                st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                st_size=props.size
            )

    @wrap_azure_errors
    def set_metadata(self, metadata: dict[str, str]):
        if self.is_dir():
            self.directory_client().set_directory_metadata(metadata)
        else:
            self.file_client().set_file_metadata(metadata)
        self.clear_cache()

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
        return pieces.hostname is not None and pieces.hostname.endswith(".file.core.windows.net")
