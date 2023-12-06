import datetime
from .base import UrlBaseHandle, StorageTier, BaseStorageHandle
from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient, FileProperties, DirectoryProperties
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from cnodc.util import HaltFlag, CNODCError
import typing as t
from urllib.parse import urlparse
import zirconium as zr
from autoinject import injector
from .azure_blob import wrap_azure_errors


class AzureFileHandle(UrlBaseHandle):

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
        if not domain.endswith(".file.core.windows.net"):
            raise CNODCError(f"Invalid hostname", "AZFILE", 1001)
        path_parts = [x for x in url_parts.path.lstrip('/').split('/')]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise CNODCError(f"Missing share name", "AZFILE", 1002)
        kwargs = {
            "storage_account": domain[:-22],
            "storage_url": domain,
            "share_name": path_parts[0],
            "connection_string": self.config.as_str(("azure", "storage", domain[:-22], "connection_string"), default=None),
            "file_path": "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
        }
        return kwargs

    def file_client(self) -> ShareFileClient:
        try:
            if self._is_dir():
                raise CNODCError(f"Cannot make file client on a directory", "AZFILE", 1005)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return ShareFileClient.from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    file_path=connection_info["file_path"]
                )
            else:
                return ShareFileClient.from_file_url(self._url, credential=DefaultAzureCredential())
        except ValueError as ex:
            raise CNODCError(f"Could not create file client", "AZFILE", 1000) from ex

    def directory_client(self) -> ShareDirectoryClient:
        try:
            if not self._is_dir():
                raise CNODCError(f"Cannot make directory client on a file", "AZFILE", 1004)
            connection_info = self.get_connection_details()
            if connection_info["connection_string"]:
                return ShareDirectoryClient.from_connection_string(
                    conn_str=connection_info["connection_string"],
                    share_name=connection_info["share_name"],
                    directory_path=connection_info["file_path"][:-1]
                )
            else:
                return ShareDirectoryClient.from_directory_url(self._url, credential=DefaultAzureCredential())
        except ValueError as ex:
            raise CNODCError(f"Could not create directory client", "AZFILE", 1003) from ex

    def _exists(self) -> bool:
        if self.is_dir():
            client = self.directory_client()
            return client.exists()
        else:
            client = self.file_client()
            try:
                client.get_file_properties()
                return True
            except ResourceNotFoundError:
                return False

    def _is_dir(self) -> bool:
        part1, _ = self._split_url()
        return part1.endswith('/')

    def _name(self) -> str:
        part1, _ = self._split_url()
        if part1.endswith('/'):
            part1 = part1[:-1]
        if '/' in part1:
            last_slash = part1.rfind('/')
            return part1[last_slash+1:]
        return part1

    @wrap_azure_errors
    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.file_client().download_file()
        for chunk in HaltFlag.iterate(stream.chunks(), self._halt_flag, True):
            yield chunk

    def _write_chunks(self, chunks: t.Iterable[bytes]):
        pass

    @wrap_azure_errors
    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        storage_tier = None
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
    def full_name(self):
        if self._is_dir():
            return self.directory_client().directory_path
        else:
            return self.file_client().file_path

    def supports_metadata(self) -> bool:
        return True

    def supports_tiering(self) -> bool:
        return False

    @wrap_azure_errors
    def walk(self, recursive: bool = True, files_only: bool = True) -> t.Iterable:
        client = self.directory_client()
        more_work: list[AzureFileHandle] = []
        for file in HaltFlag.iterate(client.list_directories_and_files(), self._halt_flag, True):
            if isinstance(file, FileProperties):
                yield self.child(file.name, False)
            elif isinstance(file, DirectoryProperties):
                if recursive or not files_only:
                    dh = self.child(file.name, True)
                    if recursive:
                        more_work.append(dh)
                    if not files_only:
                        yield dh
            else:
                raise CNODCError(f"Unknown type of file listing results [{file.__class__.__name__}]", "AZFILE", 1005)
        for sub_dir in more_work:
            yield from sub_dir.walk(recursive, files_only)

    def file_properties(self, clear_cache: bool = False) -> FileProperties:
        return self._with_cache('file_properties', self._file_properties, clear_cache=clear_cache)

    def dir_properties(self, clear_cache: bool = False) -> DirectoryProperties:
        return self._with_cache('dir_properties', self._directory_properties, clear_cache=clear_cache)

    @wrap_azure_errors
    def _file_properties(self):
        return self.file_client().get_file_properties()

    @wrap_azure_errors
    def _directory_properties(self):
        return self.directory_client().get_directory_properties()

    def size(self, clear_cache: bool = False) -> int:
        if self._is_dir():
            return -1
        return self.file_properties(clear_cache).size

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        if self._is_dir():
            return self.dir_properties(clear_cache).last_modified
        else:
            return self.file_properties(clear_cache).last_modified

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
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname.endswith(".file.core.windows.net")
