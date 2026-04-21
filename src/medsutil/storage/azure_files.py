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
import io
import tempfile
import typing as t
from urllib.parse import urlparse

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient

import medsutil.types as ct
from medsutil.awaretime import AwareDateTime
from medsutil.byteseq import ByteSequenceReader
from medsutil.storage.azure import AzureBaseHandle, wrap_azure_errors
from medsutil.storage.interface import FeatureFlag, StatResult


class AzureFileHandle(AzureBaseHandle):

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         essential_domain='.file.core.windows.net',
                         supports=FeatureFlag.DEFAULT | FeatureFlag.METADATA,
                         log_name='az_share',
                         **kwargs)
        self._file_client = None
        self._dir_client = None

    def file_client(self) -> ShareFileClient:
        """Build a file share client."""
        return self._get_client('file')

    def directory_client(self) -> ShareDirectoryClient:
        """Build a directory client."""
        return self._get_client('directory')

    def _mkdir(self, mode: int = 0o777):
        try:
            self.directory_client().create_directory()
            self._update_stat(is_dir=True, is_file=False, exists=True)
        except ResourceExistsError: ...

    @wrap_azure_errors
    def _streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        stream = self.file_client().download_file()
        yield from self._halt_flag.iterate(stream.chunks())

    def _upload_from_seekable_file(self, readable: ct.SupportsBinarySeek, chunk_size: int = None, **kwargs):
        readable.seek(0, io.SEEK_END)
        file_size = readable.tell()
        readable.seek(0)
        client_ = self.file_client()
        new_metadata = kwargs.pop('metadata', None)
        try:
            new_properties = client_.create_file(file_size, metadata=new_metadata or {})
        except ResourceExistsError:
            if new_metadata is not None:
                client_.set_file_metadata(new_metadata)
            new_properties = client_.resize_file(file_size)

        bsr = ByteSequenceReader(readable, halt_flag=self._halt_flag)
        offset = 0
        while not bsr.at_eof():
            chunk = bsr.consume(1024 * 1024 * 2)
            s = len(chunk)
            new_properties = client_.upload_range(chunk, offset, s)
            offset += s
        self._update_stat(
            is_file=True,
            is_dir=False,
            exists=True,
            st_mtime=new_properties['last_modified'],
            st_size = file_size,
            metadata = new_metadata
        )

    @wrap_azure_errors
    def _streaming_write(self, chunks: t.Iterable[t.ByteString], **kwargs):
        with tempfile.SpooledTemporaryFile(mode='w+b') as stf:
            self._halt_flag.write_all(stf, chunks)
            self._upload_from_seekable_file(stf, **kwargs)

    @wrap_azure_errors
    def _remove(self):
        try:
            self.directory_client().delete_directory()
            self._update_stat(is_dir=None, is_file=None, exists=False, st_size=None, st_mtime=None, metadata=None)
            return
        except ResourceNotFoundError: ...

        try:
            self.file_client().delete_file()
            self._update_stat(is_dir=None, is_file=None, exists=False, st_size=None, st_mtime=None, metadata=None)
        except ResourceNotFoundError: ...

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
        directory = self.directory_client()
        try:
            props = directory.get_directory_properties()
            return StatResult(
                exists=True,
                st_size=None,
                st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                is_dir=props.is_directory,
                is_file=not props.is_directory,
                metadata=props.metadata,
            )
        except ResourceNotFoundError: ...

        client = self.file_client()
        try:
            props = client.get_file_properties()
            return StatResult(
                exists=True,
                is_dir=props.is_directory,
                is_file=not props.is_directory,
                metadata=props.metadata,
                st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                st_size=props.size
            )
        except ResourceNotFoundError: ...

        return StatResult(exists=False)

    @wrap_azure_errors
    def _set_metadata(self, metadata: dict[str, str]):
        try:
            self.directory_client().set_directory_metadata(metadata)
            self._update_stat(metadata=metadata)
        except ResourceNotFoundError:
            self.file_client().set_file_metadata(metadata)
            self._update_stat(metadata=metadata)

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname is not None and pieces.hostname.endswith(".file.core.windows.net")
