import functools
import os
import pathlib
import shutil
from urllib.parse import urlparse, ParseResult

from autoinject import injector

from cnodc.util import HaltFlag, CNODCError
import datetime
import typing as t
import fnmatch
import enum


DEFAULT_CHUNK_SIZE = 4194304


class StorageTier(enum.Enum):

    FREQUENT = "frequent"
    INFREQUENT = "infrequent"
    ARCHIVAL = "archival"
    NA = 'na'


class StorageError(CNODCError):

    def __init__(self, msg, code, is_recoverable: bool = False):
        super().__init__(msg, "STORAGE", code, is_recoverable=is_recoverable)


def local_file_error_wrap(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except FileNotFoundError as ex:
            raise StorageError(f"Local file not found", 1002) from ex
        except PermissionError as ex:
            raise StorageError(f"Access to local file denied", 1003, True) from ex
        except IsADirectoryError as ex:
            raise StorageError(f"Local file is a directory", 1004) from ex
        except NotADirectoryError as ex:
            raise StorageError(f"Local directory is not a directory", 1005) from ex
        except Exception as ex:
            raise StorageError(f"Exception processing local file: {ex.__class__.__name__}: {str(ex)}", 1005) from ex

    return _inner


class StorageFileHandle(t.Protocol):

    def clear_cache(self):
        raise NotImplementedError

    def path(self) -> str:
        raise NotImplementedError

    def download(self, local_path: pathlib.Path, allow_overwrite: bool = False, buffer_size: int = None):
        raise NotImplementedError

    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        raise NotImplementedError

    def exists(self, clear_cache: bool = False) -> bool:
        raise NotImplementedError

    def is_dir(self, clear_cache: bool = False) -> bool:
        raise NotImplementedError

    def child(self, sub_path: str, as_dir: bool = False):
        raise NotImplementedError

    def subdir(self, sub_path: str):
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        raise NotImplementedError

    def supports_metadata(self) -> bool:
        raise NotImplementedError

    def set_metadata(self, metadata: dict[str, str]):
        raise NotImplementedError

    def get_metadata(self, clear_cache: bool = False) -> dict[str, str]:
        raise NotImplementedError

    def supports_tiering(self) -> bool:
        raise NotImplementedError

    def set_tier(self, tier: StorageTier):
        raise NotImplementedError

    def get_tier(self, clear_cache: bool = False) -> t.Optional[StorageTier]:
        raise NotImplementedError

    def size(self, clear_cache: bool = False) -> int:
        raise NotImplementedError


class StorageDirectoryHandle(StorageFileHandle):

    def search(self, pattern: str, recursive: bool = True, files_only: bool = True) -> t.Iterable[StorageFileHandle]:
        raise NotImplementedError

    def walk(self, recursive: bool = True, files_only: bool = True) -> t.Iterable[StorageFileHandle]:
        raise NotImplementedError


class BaseStorageHandle:

    def __init__(self, *args, halt_flag: HaltFlag = None, **kwargs):
        self._cached_properties = {}
        self._halt_flag = halt_flag

    def clear_cache(self):
        self._cached_properties = {}

    def _with_cache(self, key: str, callback: callable, *args, clear_cache: bool = False, **kwargs):
        if clear_cache or key not in self._cached_properties:
            self._cached_properties[key] = callback(*args, **kwargs)
        return self._cached_properties[key]

    def __str__(self):
        return self.path()

    def path(self) -> str:
        raise NotImplementedError()

    def _default_buffer_size(self):
        return 8 * 1024

    def download(self, local_path: pathlib.Path, allow_overwrite: bool = False, buffer_size: int = None):
        if (not allow_overwrite) and local_path.exists():
            raise CNODCError(f"Path [{local_path}] already exists, cannot download from [{self}]", "STORAGE", 1000, is_recoverable=True)
        self._download(local_path, buffer_size)

    def _download(self, local_path: pathlib.Path, buffer_size: int = None):
        if buffer_size is None:
            buffer_size = self._default_buffer_size()
        try:
            self._local_write_chunks(local_path, self._read_chunks(buffer_size))
            self._complete_download(local_path)
        except Exception as ex:
            local_path.unlink(True)
            raise ex

    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        raise NotImplementedError()

    def _complete_download(self, local_path: pathlib.Path):
        pass

    def upload(self,
               local_path,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        if (not allow_overwrite) and self.exists():
            raise CNODCError(f"Path [{self}] already exists, cannot upload from [{local_path}]", "STORAGE", 1001, is_recoverable=True)
        self._add_default_metadata(metadata, storage_tier)
        self._upload(local_path, buffer_size, metadata, storage_tier)

    def _add_default_metadata(self, metadata: dict, storage_tier: t.Optional[StorageTier] = None):
        if 'AccessLevel' not in metadata:
            metadata['AccessLevel'] = 'GENERAL'
        if 'SecurityLabel' not in metadata:
            metadata['SecurityLabel'] = 'UNCLASSIFIED'
        if storage_tier is not None and self.supports_tiering():
            if storage_tier == StorageTier.ARCHIVAL:
                metadata['StoragePlan'] = 'ARCHIVAL'
            elif storage_tier == StorageTier.FREQUENT:
                metadata['StoragePlan'] = 'HOT'
            elif storage_tier == StorageTier.INFREQUENT:
                metadata['StoragePlan'] = 'COOL'
        if 'PublicationPlan' not in metadata:
            metadata['PublicationPlan'] = 'NONE'

    def _upload(self,
               local_path,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        if buffer_size is None:
            buffer_size = self._default_buffer_size()
        try:
            self._write_chunks(self._local_read_chunks(local_path, buffer_size))
            self._complete_upload(local_path)
            if self.supports_metadata() and metadata is not None:
                self.set_metadata(metadata)
            if self.supports_tiering() and storage_tier is not None:
                self.set_tier(storage_tier)
        except Exception as ex:
            self.remove()
            raise ex

    def _write_chunks(self, chunks: t.Iterable[bytes]):
        raise NotImplementedError()

    def _complete_upload(self, local_path: pathlib.Path):
        self.clear_cache()

    @local_file_error_wrap
    def _local_read_chunks(self, local_path, buffer_size: t.Optional[int] = None) -> t.Iterable[bytes]:
        if buffer_size is None:
            buffer_size = DEFAULT_CHUNK_SIZE
        if isinstance(local_path, (bytes, bytearray)):
            yield local_path
        if isinstance(local_path, (str, pathlib.Path)):
            with open(local_path, "rb") as src:
                yield from self._read_in_chunks(src, buffer_size)
        elif hasattr(local_path, 'read'):
            yield from self._read_in_chunks(local_path, buffer_size)
        elif hasattr(local_path, '__iter__'):
            yield from HaltFlag.iterate(local_path, self._halt_flag, True)

    @local_file_error_wrap
    def _read_in_chunks(self, readable, buffer_size: int):
        if self._halt_flag:
            self._halt_flag.check_continue(True)
        x = readable.read(buffer_size)
        while x != b'':
            yield x
            if self._halt_flag:
                self._halt_flag.check_continue(True)
            x = readable.read(buffer_size)

    @local_file_error_wrap
    def _local_write_chunks(self, local_path: pathlib.Path, chunks: t.Iterable[bytes]):
        with open(local_path, "wb") as dest:
            for chunk in HaltFlag.iterate(chunks, self._halt_flag, True):
                dest.write(chunk)

    def search(self, pattern: str, recursive: bool = True, files_only: bool = True) -> t.Iterable[StorageDirectoryHandle]:
        for file in self.walk(recursive, files_only):
            if pattern is None or fnmatch.fnmatch(file.name(), pattern):
                yield file

    def walk(self, recursive: bool = True, files_only: bool = True) -> t.Iterable[StorageDirectoryHandle]:
        raise NotImplementedError()

    def exists(self, clear_cache: bool = False) -> bool:
        return self._with_cache('exists', self._exists, clear_cache=clear_cache)

    def _exists(self) -> bool:
        raise NotImplementedError()

    def is_dir(self, clear_cache: bool = False) -> bool:
        return self._with_cache('is_dir', self._is_dir, clear_cache=clear_cache)

    def _is_dir(self) -> bool:
        raise NotImplementedError()

    def child(self, sub_path: str, as_dir: bool = False) -> StorageFileHandle:
        raise NotImplementedError()

    def subdir(self, sub_path: str):
        return self.child(sub_path, True)

    def remove(self):
        raise NotImplementedError()

    def name(self) -> str:
        return self._with_cache('name', self._name)

    def _name(self) -> str:
        raise NotImplementedError()

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        return self._with_cache('modified_datetime', self._modified_datetime, clear_cache=clear_cache)

    def _modified_datetime(self) -> t.Optional[datetime.datetime]:
        return None

    def supports_metadata(self) -> bool:
        return False

    def set_metadata(self, metadata: dict[str, str]):
        pass

    def get_metadata(self, clear_cache: bool = False) -> dict[str, str]:
        return self._with_cache('get_metadata', self._get_metadata, clear_cache=clear_cache)

    def _get_metadata(self) -> dict[str, str]:
        return {}

    def supports_tiering(self) -> bool:
        return False

    def set_tier(self, tier: StorageTier):
        pass

    def get_tier(self, clear_cache: bool = False) -> t.Optional[StorageTier]:
        return self._with_cache('get_tier', self._get_tier, clear_cache=clear_cache)

    def _get_tier(self) -> t.Optional[StorageTier]:
        return None

    def size(self, clear_cache: bool = False) -> int:
        return self._with_cache('size', self._size, clear_cache=clear_cache)

    def _size(self) -> t.Optional[int]:
        return None

    @staticmethod
    def supports(file_path: str) -> bool:
        raise NotImplementedError()

    @classmethod
    def build(cls, file_path: str, halt_flag: HaltFlag = None):
        return cls(file_path, halt_flag=halt_flag)


class UrlBaseHandle(BaseStorageHandle):

    def __init__(self, url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url = url

    def child(self, sub_path: str, as_dir: bool = False):
        part1, part2 = self._split_url()
        if not part1.endswith('/'):
            part1 += '/'
        return self.__class__(
            f"{part1}{sub_path.strip('/')}{'' if not as_dir else '/'}{part2}",
            halt_flag=self._halt_flag
        )

    def parse_url(self) -> ParseResult:
        return self._with_cache('_parse_url', urlparse)

    def _split_url(self):
        return self._with_cache('_split_url', self._split_url_actual)

    def _split_url_actual(self):
        url_end = None
        if '?' in self._url:
            url_end = self._url.find("?")
        if '#' in self._url:
            percent_end = self._url.find('#')
            if url_end is None or url_end > percent_end:
                url_end = percent_end
        part1 = self._url if url_end is None else self._url[:url_end]
        part2 = "" if url_end is None else self._url[url_end:]
        return part1, part2

    def path(self):
        return self._url
