from __future__ import annotations
import functools
import pathlib
import datetime
import typing as t
import fnmatch
import enum
from urllib.parse import urlparse, ParseResult

from cnodc.util.constants import DEFAULT_CHUNK_SIZE
from cnodc.util import HaltFlag, CNODCError, HaltInterrupt



class StorageTier(enum.Enum):
    """Storage tiers that generally map to concepts with Azure/Amazon/etc."""

    FREQUENT = "frequent"
    INFREQUENT = "infrequent"
    ARCHIVAL = "archival"
    NA = 'na'


class StorageError(CNODCError):
    """Error class specifically for storage errors."""

    def __init__(self, msg, code, is_transient: bool = False):
        super().__init__(msg, "STORAGE", code, is_transient=is_transient)

def _convert_local_error(ex):
    if isinstance(ex, NotADirectoryError):
        raise StorageError(f"Local directory is not a directory", 1005) from ex
    elif isinstance(ex, FileNotFoundError):
        raise StorageError(f"Local file not found", 1002) from ex
    elif isinstance(ex, IsADirectoryError):
        raise StorageError(f"Local file is a directory", 1004) from ex
    elif isinstance(ex, PermissionError):
        raise StorageError(f"Access to local file denied", 1003, True) from ex
    else:
        raise StorageError(f"Exception processing local file: {ex.__class__.__name__}: {str(ex)}", 1006) from ex


def local_file_error_wrap(cb):
    """Converts typical local file-system errors into appropriate CNODCErrors with recoverable set properly."""

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except OSError as ex:
            _convert_local_error(ex)

    return _inner


def local_file_generator_error_wrap(cb):
    """Converts typical local file-system errors into appropriate CNODCErrors with recoverable set properly."""

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            yield from cb(*args, **kwargs)
        except OSError as ex:
            _convert_local_error(ex)

    return _inner


class BaseStorageHandle:

    def __init__(self, *args, halt_flag: HaltFlag = None, **kwargs):
        self._cached_properties = {}
        self._halt_flag = halt_flag
        self._new_child_args = {
            'halt_flag': halt_flag
        }

    def __str__(self):
        return self.path()

    def __repr__(self):
        return f'<{self.__class__.__name__}: {str(self)}>'

    def clear_cache(self):
        """Clear the local cache of all values."""
        self._cached_properties = {}

    def _with_cache(self, key: str, callback: callable, *args, clear_cache: bool = False, **kwargs):
        if clear_cache or key not in self._cached_properties:
            self._cached_properties[key] = callback(*args, **kwargs)
        return self._cached_properties[key]

    def path(self) -> str:
        """Get a string representation of this path that could be used to rebuild it."""
        raise NotImplementedError  # pragma: no coverage

    def _default_buffer_size(self):
        """Override this to set the default buffer size for reading/writing."""
        return DEFAULT_CHUNK_SIZE

    def download(self, local_path: pathlib.Path, allow_overwrite: bool = False, buffer_size: int = None):
        """Download the file to the given local path."""
        if (not allow_overwrite) and local_path.exists():
            raise StorageError(f"Path [{local_path}] already exists, cannot download from [{self}]", 1000, True)
        self._download(local_path, buffer_size)

    def _download(self, local_path: pathlib.Path, buffer_size: int = None):
        if buffer_size is None:
            buffer_size = self._default_buffer_size()
        try:
            self._local_write_chunks(local_path, self._read_chunks(buffer_size))
            self._complete_download(local_path)
        except Exception as ex:
            local_path.unlink(True)
            raise ex from ex

    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        """Read the file in chunks given a buffer size."""
        raise NotImplementedError  # pragma: no coverage

    def _complete_download(self, local_path: pathlib.Path):
        """Override to implement behaviour after the download is complete."""
        pass  # pragma: no coverage

    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        """Upload a local file to the location represented by this handle."""
        if (not allow_overwrite) and self.exists():
            raise StorageError(f"Path [{self.name()}] already exists, cannot overwrite", 1001, True)
        if self.supports_tiering():
            storage_tier = storage_tier or StorageTier.FREQUENT
        else:
            storage_tier = None
        if self.supports_metadata():
            metadata = metadata or {}
            self._add_default_metadata(metadata, storage_tier)
        else:
            metadata = None
        self._upload(local_path, buffer_size, metadata, storage_tier)

    def _add_default_metadata(self, metadata: dict, storage_tier: t.Optional[StorageTier] = None):
        """Add default metadata to the file based on NODB standards."""
        from cnodc.storage import StorageController
        StorageController.apply_default_metadata(metadata, storage_tier=storage_tier)

    def mkdir(self, mode=0o777, parents: bool = True):
        if self.exists():
            return
        if parents:
            parent = self.parent()
            if parent is not None and not parent.exists():
                parent.mkdir(mode, parents)
        self._mkdir(mode)
        self.clear_cache()

    def _mkdir(self, mode):
        raise NotImplementedError

    def _upload(self,
               local_path,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        """General implementation of uploading."""
        if buffer_size is None:
            buffer_size = self._default_buffer_size()
        try:
            self._write_chunks(self._local_read_chunks(local_path, buffer_size))
            self._complete_upload(local_path)
            if self.supports_metadata() and metadata is not None:  # pragma: no coverage (azure blobs handle this differently)
                self.set_metadata(metadata)
            if self.supports_tiering() and storage_tier is not None:  # pragma: no coverage (azure blobs handle this differently)
                self.set_tier(storage_tier)
        except Exception as ex:
            self.remove()
            raise ex from ex  # pragma: no coverage

    def _write_chunks(self, chunks: t.Iterable[bytes]):
        """Write an iterable bytes to the given file."""
        raise NotImplementedError  # pragma: no coverage

    def _complete_upload(self, local_path: pathlib.Path):
        """Override to specify behaviour after an upload is complete."""
        self.clear_cache()

    @local_file_error_wrap
    def _local_read_chunks(self, local_path, buffer_size: t.Optional[int] = None) -> t.Iterable[bytes]:
        """Local implementation of reading chunks from a file."""
        if buffer_size is None:
            buffer_size = DEFAULT_CHUNK_SIZE
        if isinstance(local_path, (bytes, bytearray)):
            yield local_path
        elif isinstance(local_path, (str, pathlib.Path)):
            with open(local_path, "rb") as src:
                yield from self._read_in_chunks(src, buffer_size)
        elif hasattr(local_path, 'read'):
            yield from self._read_in_chunks(local_path, buffer_size)
        elif hasattr(local_path, '__iter__'):
            yield from HaltFlag._iterate(local_path, self._halt_flag, True)

    @local_file_error_wrap
    def _read_in_chunks(self, readable, buffer_size: int) -> t.Iterable[bytes]:
        """Read in chunks from a readable object."""
        while x := readable.read(buffer_size):
            yield x
            if self._halt_flag:
                self._halt_flag.breakpoint()

    @local_file_error_wrap
    def _local_write_chunks(self, local_path: pathlib.Path, chunks: t.Iterable[bytes]):
        """Write chunks to a local file."""
        try:
            with open(local_path, "wb") as dest:
                for chunk in HaltFlag._iterate(chunks, self._halt_flag, True):
                    dest.write(chunk)
        except HaltInterrupt as ex:
            local_path.unlink(True)
            raise ex from ex

    def search(self, pattern: t.Optional[str] = None, recursive: bool = True) -> t.Iterable[BaseStorageHandle]:
        """Find all files that match the given pattern."""
        for file in self.walk(recursive):
            if pattern is None or fnmatch.fnmatch(file.name(), pattern):
                yield file

    def walk(self, recursive: bool = True) -> t.Iterable[BaseStorageHandle]:
        """Find all files, optionally recursively."""
        raise NotImplementedError  # pragma: no coverage

    def exists(self, clear_cache: bool = False) -> bool:
        """Check if the handle exists."""
        return self._with_cache('exists', self._exists, clear_cache=clear_cache)

    def _exists(self) -> bool:
        raise NotImplementedError  # pragma: no coverage

    def is_dir(self, clear_cache: bool = False) -> bool:
        """Check if the handle represents a directory."""
        return self._with_cache('is_dir', self._is_dir, clear_cache=clear_cache)

    def _is_dir(self) -> bool:
        raise NotImplementedError  # pragma: no coverage

    def child(self, sub_path: str, as_dir: bool = False) -> BaseStorageHandle:
        """Create a child of the current directory."""
        raise NotImplementedError  # pragma: no coverage

    def parent(self) -> t.Self:
        raise NotImplementedError  # pragma: no coverage

    def subdir(self, sub_path: str) -> BaseStorageHandle:
        """Create a child of the current directory, as a directory."""
        return self.child(sub_path, True)

    def remove(self):
        """Remove the file or directory, if it exists."""
        raise NotImplementedError  # pragma: no coverage

    def name(self) -> str:
        """Get the name of the handle."""
        return self._with_cache('name', self._name)

    def _name(self) -> str:
        raise NotImplementedError  # pragma: no coverage

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        """Get the last modified time of the entry."""
        return self._with_cache('modified_datetime', self._modified_datetime, clear_cache=clear_cache)

    def _modified_datetime(self) -> t.Optional[datetime.datetime]:
        return None  # pragma: no coverage

    def supports_metadata(self) -> bool:
        """Check if the handle supports metadata setting."""
        return False  # pragma: no coverage

    def set_metadata(self, metadata: dict[str, str]):
        """Set metadata."""
        pass  # pragma: no coverage

    def get_metadata(self, clear_cache: bool = False) -> dict[str, str]:
        """Retrieve metadata"""
        return self._with_cache('get_metadata', self._get_metadata, clear_cache=clear_cache)

    def _get_metadata(self) -> dict[str, str]:
        return {}  # pragma: no coverage

    def supports_tiering(self) -> bool:
        """Check if the handle supports tiering"""
        return False  # pragma: no coverage

    def set_tier(self, tier: StorageTier):
        """Set the storage tier."""
        pass  # pragma: no coverage

    def get_tier(self, clear_cache: bool = False) -> t.Optional[StorageTier]:
        """Retrieve the storage tier"""
        return self._with_cache('get_tier', self._get_tier, clear_cache=clear_cache)

    def _get_tier(self) -> t.Optional[StorageTier]:
        return None  # pragma: no coverage

    def size(self, clear_cache: bool = False) -> int:
        """Retrieve the size of the file."""
        return self._with_cache('size', self._size, clear_cache=clear_cache)

    def _size(self) -> t.Optional[int]:
        return None  # pragma: no coverage

    def _build_descriptor(self, *args, **kwargs):
        return self.__class__(*args, **self._new_child_args, **kwargs)

    @staticmethod
    def supports(file_path: str) -> bool:
        """Check if this handle class supports the given file path."""
        raise NotImplementedError  # pragma: no coverage

    @classmethod
    def build(cls, file_path: str, halt_flag: HaltFlag = None) -> BaseStorageHandle:
        """Construct a handle from the given file path."""
        return cls(file_path, halt_flag=halt_flag)


class UrlBaseHandle(BaseStorageHandle):
    """General implementation of url-based file handles."""

    def __init__(self, url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url = url

    def child(self, sub_path: str, as_dir: bool = False) -> UrlBaseHandle:
        part1, part2 = self._split_url()
        if not part1.endswith('/'):  # pragma: no coverage (usually fine, is here just in case)
            part1 += '/'
        return self._build_descriptor(
            f"{part1}{sub_path.strip('/')}{'' if not as_dir else '/'}{part2}"
        )

    def parent(self) -> t.Optional[UrlBaseHandle]:
        parts = self.parse_url()
        pieces = [x for x in parts.path.split('/') if x]
        if not pieces:
            return None
        return self._build_descriptor(f'{parts.scheme}://{parts.netloc}/{'/'.join(pieces[:-1])}/')

    def parse_url(self) -> ParseResult:
        """Get the parts of the URL."""
        return self._with_cache('_parse_url', urlparse, self._url)

    def _split_url(self):
        return self._with_cache('_split_url', self._split_url_actual)

    def _split_url_actual(self):
        url_end = None
        if '?' in self._url:  # pragma: no coverage (not relevant for FTP, may be relevant elsewhere)
            url_end = self._url.find("?")
        if '#' in self._url:  # pragma: no coverage (not relevant for FTP, may be relevant elsewhere)
            percent_end = self._url.find('#')
            if url_end is None or url_end > percent_end:
                url_end = percent_end
        part1 = self._url if url_end is None else self._url[:url_end]
        part2 = "" if url_end is None else self._url[url_end:]
        return part1, part2

    def _name(self) -> str:
        parts = self.parse_url()
        pieces = [x for x in parts.path.split('/') if x]
        if not pieces:
            return ''
        return pieces[-1]


    def path(self) -> str:
        return self._url
