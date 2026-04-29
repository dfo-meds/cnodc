import abc
import functools
import os
import pathlib
import tempfile
import typing as t
import fnmatch
import urllib.parse
from abc import ABC
from contextlib import contextmanager
from types import EllipsisType
from urllib.parse import urlsplit

import zrlog

from medsutil.awaretime import AwareDateTime
from medsutil.byteseq import ByteSequenceReader
from medsutil.cached import CachedObjectMixin
from medsutil.halts import HaltFlag, DummyHaltFlag
import medsutil.types as ct
from medsutil.storage import StorageTier, interface
from medsutil.storage.interface import StatResult, StorageError, FeatureFlag


def _convert_local_error(ex):
    if isinstance(ex, NotADirectoryError):
        raise StorageError(f"Local directory is not a directory", 1000) from ex
    elif isinstance(ex, FileNotFoundError):
        raise StorageError(f"Local file not found", 1001) from ex
    elif isinstance(ex, IsADirectoryError):
        raise StorageError(f"Local file is a directory", 1002) from ex
    elif isinstance(ex, PermissionError):
        raise StorageError(f"Access to local file denied", 1003, is_transient=True) from ex
    else:
        raise StorageError(f"Exception processing local file: {ex.__class__.__name__}: {str(ex)}", 1004) from ex


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


class _BaseStorageIO:

    def __init__(self, handle: BaseStorageHandle):
        self.handle = handle
        self.closed = True

    def __enter__(self) -> t.Self:
        self.closed = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._flush()
        self.close()

    def _flush(self): ...

    def seek(self):
        raise OSError

    def isatty(self):
        return False

    def readable(self):
        return False

    def writable(self):
        return False

    def read(self, size=-1):
        raise OSError

    def readline(self, size=-1):
        raise OSError

    def readlines(self, hint=-1):
        raise OSError

    def write(self, value: bytes):
        raise OSError

    def close(self):
        self.closed = True

    def seekable(self):
        return False

    def writeable(self):
        return False

    def tell(self):
        raise OSError

    def truncate(self):
        raise OSError

    def flush(self): ...

    def __iter__(self):
        return iter(self.readlines())


class _StorageBinaryReader(_BaseStorageIO):

    def __init__(self, handle: BaseStorageHandle, buffering: int = None):
        super().__init__(handle)
        self._reader = None
        self._buffer_size = buffering
        self._index = 0

    def _get_reader(self):
        if self.closed:
            raise StorageError("File handle is closed", 1100)
        if self._reader is None:
            self._reader = ByteSequenceReader(
                self.handle.streaming_read(buffer_size=self._buffer_size),
            )
        return self._reader

    def readall(self) -> bytes:
        return self.read(-1)

    def read(self, size=-1) -> bytes:
        reader = self._get_reader()
        if size < 1:
            return reader.consume_all()
        else:
            return reader.consume(size)

    def readline(self, size=-1) -> bytes:
        reader = self._get_reader()
        return reader.consume_until(b'\n', include_target=True)

    def readinto(self, b: bytearray):
        reader = self._get_reader()
        a = 0
        for x in reader.iterate_rest():
            a += len(x)
            b.extend(x)
        return a

    def readlines(self, hint=-1) -> t.Iterable[bytes]:
        reader = self._get_reader()
        while not reader.at_eof():
            yield reader.consume_until(b'\n', include_target=True)

    def readable(self):
        return True


class _StorageBinaryWriter(_BaseStorageIO):

    def __init__(self, handle: BaseStorageHandle):
        super().__init__(handle)
        self._buffer: tempfile.SpooledTemporaryFile | None = None

    def _get_buffer(self) -> tempfile.SpooledTemporaryFile:
        if self._buffer is None:
            self._buffer = tempfile.SpooledTemporaryFile(1024*1024, 'w+b')
        return t.cast(tempfile.SpooledTemporaryFile, self._buffer)

    def write(self, value: bytes):
        if self.closed:
            raise StorageError("File handle is closed", 1101)
        self._get_buffer().write(value)
        self.handle.breakpoint()

    def writable(self):
        return True

    def close(self):
        super().close()

    def _flush(self):
        if self._buffer is not None:
            try:
                self._buffer.seek(0)
                self.handle.upload(self._buffer)
            finally:
                self._buffer.close()


class BaseStorageHandle(CachedObjectMixin, interface.FilePath, abc.ABC):

    def __init__(self, path: str, force_is_dir: bool = None, *, log_name: str = 'unknown', supports: FeatureFlag = FeatureFlag.DEFAULT, halt_flag: HaltFlag = None, max_chunk_size: int = None):
        super().__init__()
        self._path: str = path
        self._force_is_dir: bool | None = force_is_dir
        self._supports: FeatureFlag = supports
        self._halt_flag: HaltFlag = halt_flag or DummyHaltFlag()
        self._max_chunk_size = max_chunk_size
        self._new_child_args = {
            'halt_flag': halt_flag,
        }
        self._log = zrlog.get_logger(f'storage_{log_name}')
        self._open_as_context = False
        self._open_context_handlers: dict[str, t.Any] = {}

    def __enter__(self):
        self._open_as_context = True
        return self

    def __exit__(self, *args, **kwargs):
        self._open_as_context = False
        self._close_connection(*args, **kwargs)

    @contextmanager
    def persistent_connection[Q](self, name: str, builder: t.Callable[..., Q], *args, **kwargs) -> t.Generator[Q, None, None]:
        if self._open_as_context:
            if name not in self._open_context_handlers:
                self._open_context_handlers[name] = builder(*args, **kwargs)
                self._open_context_handlers[name].__enter__()
            yield self._open_context_handlers[name]
        else:
            with builder(*args, **kwargs) as x:
                yield x

    def _close_connection(self, *args, **kwargs):
        for x in self._open_context_handlers.values():
            x.__exit__(*args, **kwargs)

    def __str__(self) -> str:
        return self.path()

    def __eq__(self, other: BaseStorageHandle) -> bool:
        return other.path() == self.path()

    def __ne__(self, other: BaseStorageHandle) -> bool:
        return other.path() != self.path()

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {str(self)}>'

    def __truediv__(self, other: str):
        return self.child(other, None)

    def breakpoint(self):
        if self._halt_flag:
            self._halt_flag.breakpoint()

    def supports_feature(self, ff: FeatureFlag) -> bool:
        res = ff in self._supports
        self._log.trace('Feature support check [%s] [%s] = %s', self._path, ff, res)
        return res

    def supports_metadata(self) -> bool:
        """Check if the handle supports metadata setting."""
        return self.supports_feature(FeatureFlag.METADATA)

    def supports_tiering(self) -> bool:
        """Check if the handle supports tiering"""
        return self.supports_feature(FeatureFlag.TIERING)

    def read_bytes(self) -> bytes:
        with self.open('rb') as h:
            return h.read()

    def download(self, destination: pathlib.Path, allow_overwrite: bool = False, buffer_size: int | None = None):
        """Download the file to the given local path."""
        if (not allow_overwrite) and destination.exists():
            raise StorageError(f"Path [{destination}] already exists, cannot download from [{self}]", 1200)
        self._log.trace('download started from [%s] to [%s]', self._path, destination)
        self._download(destination, buffer_size)
        self._log.trace('completed download')

    def _download(self, local_path: ct.SupportsByteStreamWriting, buffer_size: int = None):
        self._halt_flag.write_all(local_path, self._streaming_read(buffer_size))
        self._complete_download(local_path)

    def streaming_read(self, buffer_size: int = None):
        yield from self._streaming_read(buffer_size)

    def _complete_download(self, local_path: ct.SupportsByteStreamWriting): ...

    def upload(self,
               source: pathlib.Path | ct.SupportsBinaryRead | t.Iterable[t.ByteString] | t.ByteString,
               allow_overwrite: bool = False,
               buffer_size: t.Optional[int] = None,
               metadata: t.Optional[dict[str, str]] = None,
               storage_tier: t.Optional[StorageTier] = None):
        """Upload a local file to the location represented by this handle."""
        if (not allow_overwrite) and self.exists():
            raise StorageError(f"Path [{self.name}] already exists, cannot overwrite", 1201)
        if self.supports_tiering():
            storage_tier = storage_tier or StorageTier.FREQUENT
        else:
            storage_tier = None
        if self.supports_feature(FeatureFlag.METADATA):
            metadata: dict = metadata or {}
            self._add_default_metadata(metadata, storage_tier)
        else:
            metadata = None
        if buffer_size is None or (self._max_chunk_size is not None and buffer_size > self._max_chunk_size):
            buffer_size = self._max_chunk_size
        self._upload(source, buffer_size, metadata, storage_tier)

    def _upload(self,
                readable: ct.SupportsByteStreaming,
                buffer_size: t.Optional[int] = None,
                metadata: t.Optional[dict[str, str]] = None,
                storage_tier: t.Optional[StorageTier] = None,
                remove_on_error: bool = True):
        """General implementation of uploading."""
        try:
            self._log.trace("Starting upload from [%s] to [%s]", readable, self._path)
            self._upload_from_any(readable, chunk_size=buffer_size, metadata=metadata, storage_tier=storage_tier)
            self._complete_upload(readable)
            self._log.trace("Upload complete")
        except Exception:
            if remove_on_error:
                self._log.trace("Error during upload, removing any uploaded file")
                self.remove()
            raise  # pragma: no coverage

            if metadata is not None and self.supports_feature(FeatureFlag.METADATA):
                self.set_metadata(metadata)
            if storage_tier is not None and self.supports_feature(FeatureFlag.TIERING):
                self.set_tier(storage_tier)
    def _upload_from_any(self,
                         readable: ct.SupportsByteStreaming,
                         **kwargs):
        if ct.is_binary_readable(readable):
            self._log.trace("Source is a read()able object")
            self._upload_from_file(readable, **kwargs)
        elif ct.is_local_path(readable):
            self._log.trace("Source is a local file path")
            self._upload_from_file_path(readable, **kwargs)
        elif isinstance(readable, (bytes, bytearray, memoryview)):
            self._log.trace("Source is a bytes object")
            self._upload_from_bytes(readable, **kwargs)
        elif isinstance(readable, t.Iterable):
            self._log.trace("Source is assumed to be an iterable of bytes")
            self._upload_from_byte_stream(readable, **kwargs)
        else:
            raise TypeError(f'Invalid object for reading: {type(readable)}')

    def _upload_from_bytes(self, readable: t.ByteString, **kwargs):
        self._upload_from_byte_stream([readable], **kwargs)

    def _upload_from_byte_stream(self, stream: t.Iterable[t.ByteString], **kwargs):
        self.streaming_write(stream, **kwargs)

    def _upload_from_file_path(self, file_name: os.PathLike | str, **kwargs):
        with open(file_name, 'rb') as h:
            self._upload_from_file(h, **kwargs)

    def _upload_from_file(self, readable: ct.SupportsBinaryRead, **kwargs):
        if ct.is_binary_seekable(readable):
            self._log.trace("Source is a seekable object")
            self._upload_from_seekable_file(readable, **kwargs)
        else:
            self._log.trace("Source is not a seekable object")
            self._upload_from_non_seekable_file(readable, **kwargs)

    def _upload_from_seekable_file(self, readable: ct.SupportsBinarySeek, chunk_size: int = None, **kwargs):
        self.streaming_write(self._halt_flag.read_all(readable, chunk_size), **kwargs)

    def _upload_from_non_seekable_file(self, readable: ct.SupportsBinaryRead, chunk_size: int = None, **kwargs):
        self.streaming_write(self._halt_flag.read_all(readable, chunk_size), **kwargs)

    def streaming_write(self, chunks: t.Iterable[t.ByteString], **kwargs):
        self._log.trace('writing chunks to %s...', self._path)
        self._streaming_write(chunks, **kwargs)
        self._log.trace('done')

    @abc.abstractmethod
    def _streaming_write(self, chunks: t.Iterable[t.ByteString], **kwargs): raise NotImplementedError

    def _complete_upload(self, local_path: ct.SupportsByteStreaming): ...

    def open(self, mode: t.Literal['rb', 'wb'], buffering: int | None = None) -> _StorageBinaryReader | _StorageBinaryWriter:
        if mode == 'rb':
            self._log.trace('opening file for binary read')
            return _StorageBinaryReader(self, buffering=buffering)
        elif mode == 'wb':
            self._log.trace('opening file for binary write')
            return _StorageBinaryWriter(self)
        else:
            raise StorageError('Invalid open mode, must be rb or wb', 1102)

    def write_bytes(self, b: t.ByteString):
        self.upload([b])

    def touch(self, mode: int = 0o666):
        self.write_bytes(b'\x00')
        if self.supports_feature(FeatureFlag.CHMOD):
            self.chmod(mode)

    def mkdir(self, mode: int = 0o777, parents: bool = True):
        if self.supports_feature(FeatureFlag.FOLDERS):
            if parents:
                self._mkdir_and_parents(mode, parents)
            else:
                self._log.trace("Making directory [%s]", self._path)
                self._mkdir(mode)

    def _mkdir_and_parents(self, mode: int = 0o777, parents: bool = True):
        """ Slow implementation that works with existing interface - override with faster if available. """
        parent = self._parent()
        if parent is None:
            self._log.trace('No parent directory available')
        elif parent == self:
            self._log.trace('Currently at top level directory')
            return
        else:
            parent.mkdir(mode, parents)
        self._mkdir(mode)

    def path(self) -> str:
        return self._path

    def search(self, pattern: t.Optional[str] = None, recursive: bool = True, case_sensitive: bool = False, path_types: interface.PathType = interface.PathType.BOTH) -> t.Iterable[t.Self]:
        """Find all files that match the given pattern."""
        self._log.trace("Searching for files matching [%s][path_types=%s] in [%s][%s]", pattern, path_types, self._path, recursive)
        if pattern is not None and not case_sensitive:
            pattern = pattern.lower()
        for file in self.iterdir(recursive, path_types):
            if pattern is None:
                yield file
            elif (not case_sensitive) and fnmatch.fnmatchcase(file.name.lower(), pattern):
                yield file
            elif case_sensitive and fnmatch.fnmatchcase(file.name, pattern):
                yield file

    def iterdir(self, recursive: bool = True, path_types: interface.PathType = interface.PathType.BOTH) -> t.Iterable[t.Self]:
        for dir_name, dir_names, file_names in self.walk():
            if not dir_name.endswith('/'):
                dir_name += '/'
            if interface.PathType.FILE in path_types:
                for f in file_names:
                    yield self.from_absolute_path(dir_name + f, as_dir=False)
            if interface.PathType.DIRECTORY in path_types:
                for d in dir_names:
                    yield self.from_absolute_path(dir_name + d, as_dir=True)
            if not recursive:
                break

    def _update_stat(self, **kwargs):
        # Don't build the cache if we don't need to.
        s = self._from_cache_only('stat')
        if s is not None:
            for kw in kwargs:
                if kwargs[kw] is not None and hasattr(s, kw):
                    setattr(s, kw, kwargs[kw])

    def walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        if self.supports_feature(FeatureFlag.WALK):
            yield from self._walk()
        else:
            self._log.trace("Walk not supported")

    def exists(self) -> bool:
        """Check if the handle exists."""
        return self.stat().exists

    def is_dir(self) -> bool | None:
        """Check if the handle represents a directory."""
        s = self.stat()
        if s.exists:
            return s.is_dir
        return self._no_remote_is_dir()

    def _no_remote_is_dir(self):
        return self._force_is_dir

    def is_file(self) -> bool | None:
        s = self.stat()
        if s.exists:
            return s.is_file
        return self._no_remote_is_file()

    def _no_remote_is_file(self):
        _is_dir = self._no_remote_is_dir()
        if _is_dir is None:
            return None
        return not _is_dir

    def subdir(self, sub_path: str) -> t.Self:
        """Create a child of the current directory, as a directory."""
        return self.child(sub_path, True)

    def subfile(self, sub_path: str) -> t.Self:
        return self.child(sub_path, False)

    @property
    def name(self) -> str:
        """Get the name of the handle."""
        return self._with_cache('name', self._name)

    def modified_datetime(self) -> AwareDateTime | None:
        """Get the last modified time of the entry."""
        if self.supports_feature(FeatureFlag.MODIFIED_TIME):
            return self.stat().st_mtime
        return None

    def set_metadata(self, metadata: dict[str, str]):
        """Set metadata."""
        if self.supports_feature(FeatureFlag.METADATA):
            self._set_metadata(metadata)

    def get_metadata(self) -> dict[str, str]:
        """Retrieve metadata"""
        if self.supports_feature(FeatureFlag.METADATA):
            return self.stat().metadata or {}
        return {}

    def get_tier(self) -> t.Optional[StorageTier]:
        """Retrieve the storage tier"""
        if self.supports_feature(FeatureFlag.TIERING):
            return self.stat().tier
        return None

    def set_tier(self, tier: StorageTier):
        """Set the storage tier."""
        if self.supports_feature(FeatureFlag.TIERING):
            self._set_tier(tier)

    def size(self) -> int | None:
        """Retrieve the size of the file."""
        if self.supports_feature(FeatureFlag.SIZE):
            return self.stat().st_size
        return None

    def stat(self) -> StatResult:
        return self._with_cache('stat', self._stat)

    def unlink(self):
        self.remove()

    def rmdir(self):
        self.remove()

    def remove(self):
        if self.supports_feature(FeatureFlag.REMOVAL):
            self._remove()

    def chmod(self, mode: int):
        if self.supports_feature(FeatureFlag.CHMOD):
            self._chmod(mode)

    def copy_into(self, destination_dir: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        return self.copy(destination_dir.subfile(self.name), allow_overwrite)

    def copy(self, destination: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        if type(destination) == type(self):
            return self._local_fast_copy(destination, allow_overwrite)
        else:
            return self._copy(destination, allow_overwrite)

    def _local_fast_copy[T](self: T, destination: T, allow_overwrite: bool = False) -> T:
        return self._copy(destination, allow_overwrite)

    def _copy(self, destination: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        kwargs = {}
        if self.supports_feature(FeatureFlag.METADATA) and destination.supports_feature(FeatureFlag.METADATA):
            kwargs['metadata'] = self.get_metadata()
        with self.open('rb') as src:
            destination.upload(
                source=src,
                allow_overwrite=allow_overwrite,
                **kwargs
            )
        if self.supports_feature(FeatureFlag.TIERING) and destination.supports_feature(FeatureFlag.TIERING):
            tier = self.get_tier()
            if tier is not None:
                destination.set_tier(tier)
        return destination

    def rename(self, destination: interface.DestinationType) -> interface.DestinationType:
        return self.move(destination, allow_overwrite=False)

    def replace(self, destination: interface.DestinationType) -> interface.DestinationType:
        return self.move(destination, allow_overwrite=True)

    def move_into(self, destination_dir: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        return self.move(destination_dir.subfile(self.name), allow_overwrite)

    def move(self, destination: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        if type(destination) == type(self):
            self._local_fast_move(destination, allow_overwrite)
        else:
            self._move(destination, allow_overwrite)
        return destination

    def _local_fast_move[T](self: T, destination: T, allow_overwrite: bool = False) -> T:
        return self._move(destination, allow_overwrite)

    def _move(self, destination: interface.DestinationType, allow_overwrite: bool = False) -> interface.DestinationType:
        kwargs = {}
        if self.supports_feature(FeatureFlag.METADATA) and destination.supports_feature(FeatureFlag.METADATA):
            kwargs['metadata'] = self.get_metadata()
        with self.open('rb') as src:
            destination.upload(
                source=src,
                allow_overwrite=allow_overwrite,
                **kwargs
            )
        if self.supports_feature(FeatureFlag.TIERING) and destination.supports_feature(FeatureFlag.TIERING):
            tier = self.get_tier()
            if tier is not None:
                destination.set_tier(tier)
        if self.supports_feature(FeatureFlag.REMOVAL):
            self.remove()
        return destination

    def _build_descriptor(self, path: str, as_dir: bool = None, **kwargs):
        return self.__class__(path, as_dir, **self._new_child_args, **kwargs)

    @property
    def stem(self) -> str:
        return self._with_cache('stem', self._stem)

    def _stem(self) -> str:
        n = self.name
        if "." in n:
            return n[:n.rfind(".")]
        return n

    @property
    def suffixes(self) -> list[str]:
        return self._with_cache('suffixes', self._suffixes)

    def _suffixes(self) -> list[str]:
        pieces = self.name.split(".")
        return pieces[1:]

    @property
    def parents[T](self: T) -> t.Sequence[T]:
        return self._with_cache('parents', self._parents)

    def _parents[T](self: T) -> t.Sequence[T]:
        x = self.parent
        parents: list[T] = [x]
        while (y := x.parent) != x:
            parents.append(y)
            x = y
        return tuple(parents)

    @property
    def parent(self) -> t.Self:
        return self._with_cache('parent', self._parent)

    def joinpath[T](self: T, *args, last_as_dir: bool = None) -> T:
        x = self
        for y in args[0:-1]:
            x = x.child(y, True)
        return x.child(args[-1], last_as_dir)

    def with_name(self, name: str, as_dir: bool | None | EllipsisType = ...) -> t.Self:
        if isinstance(as_dir, EllipsisType):
            return self.parent.child(name, self.is_dir())
        else:
            return self.parent.child(name, as_dir)

    def with_stem(self, stem: str, as_dir: bool | None | EllipsisType = ...) -> t.Self:
        suffixes = self.suffixes
        return self.with_name(f"{stem}.{suffixes[-1] if suffixes else ''}", as_dir)

    @classmethod
    def build(cls, file_path: str, force_is_dir: bool = None, halt_flag: HaltFlag = None) -> t.Self:
        """Construct a handle from the given file path."""
        return cls(file_path, force_is_dir, halt_flag=halt_flag)

    @staticmethod
    def supports(file_path: str) -> bool:
        """Check if this handle class supports the given file path."""
        raise NotImplementedError  # pragma: no coverage

    @staticmethod
    def _add_default_metadata(metadata: dict, storage_tier: t.Optional[StorageTier] = None):
        """Add default metadata to the file based on NODB standards."""
        from medsutil.storage import StorageController
        StorageController.apply_default_metadata(metadata, storage_tier=storage_tier)

    # These methods are optional

    def _chmod(self, mode: int): raise NotImplementedError
    def _mkdir(self, mode: int = 0o777): raise NotImplementedError
    def _remove(self): raise NotImplementedError
    def _set_metadata(self, metadata: dict[str, str]): raise NotImplementedError
    def _set_tier(self, tier: StorageTier): raise NotImplementedError
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]: ...

    # These are required

    @abc.abstractmethod
    def _stat(self) -> StatResult: raise NotImplementedError

    @abc.abstractmethod
    def _streaming_read(self, buffer_size: int = None) -> t.Iterable[t.ByteString]: raise NotImplementedError

    @abc.abstractmethod
    def _parent(self) -> t.Self: raise NotImplementedError

    @abc.abstractmethod
    def child(self, name: str, as_dir: bool | None = None) -> t.Self: raise NotImplementedError

    @abc.abstractmethod
    def from_absolute_path(self, path: str, as_dir: bool | None = None) -> t.Self: raise NotImplementedError

    @abc.abstractmethod
    def _name(self) -> str: raise NotImplementedError


class UrlBaseHandle(BaseStorageHandle, ABC):
    """General implementation of url-based file handles."""

    def __init__(self, path: str, force_is_dir: bool = None, **kwargs):
        p = urllib.parse.urlsplit(path)
        _path = p.path
        if force_is_dir is None:
            if (not _path) or _path.endswith('/'):
                force_is_dir = True
        path = self._replace_path_in_url(path, _path, force_is_dir)
        super().__init__(path, force_is_dir, **kwargs)

    def from_absolute_path(self, path: str, as_dir: bool | None = None) -> t.Self:
        if '://' in path:
            return self._build_descriptor(path, as_dir)
        return self._build_descriptor(self._replace_path(path, as_dir), as_dir=as_dir)

    def child(self, name: str, as_dir: bool | None = None) -> t.Self:
        path = self.url_path()
        if not path.endswith('/'):
            path += '/'
        path += name
        return self._build_descriptor(self._replace_path(path, as_dir), as_dir=as_dir)

    def _parent(self) -> t.Self | None:
        path = [x for x in self.url_path().split('/') if x]
        return self._build_descriptor(self._replace_path('/'.join(path[:-1]), True), as_dir=True)

    def _replace_path(self, new_path: str, as_dir: bool | None) -> str:
        return self._replace_path_in_url(self._path, new_path, as_dir)

    @staticmethod
    def _replace_path_in_url(url: str, new_path: str, as_dir: bool | None) -> str:
        ended_with_slash = new_path.endswith('/')
        new_path = '/'.join(x for x in new_path.split('/') if x)
        if new_path and as_dir and not new_path.endswith('/'):
            new_path += '/'
        elif ended_with_slash:
            new_path += '/'
        res = urllib.parse.urlunsplit(urllib.parse.urlsplit(url)._replace(path=new_path))
        if isinstance(res, str):
            return res
        else:
            return res.decode('utf-8')

    def parse_url(self) -> urllib.parse.SplitResult:
        """Get the parts of the URL."""
        return t.cast(urllib.parse.SplitResult, self._with_cache('_parse_url', urlsplit, self._path))

    def url_path(self):
        return self.parse_url().path

    def _name(self) -> str:
        parts = self.parse_url()
        pieces = [x for x in parts.path.split('/') if x]
        if not pieces:
            return ''
        return pieces[-1]
