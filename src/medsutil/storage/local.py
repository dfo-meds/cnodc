"""Local file handle"""
import pathlib
import typing as t
import shutil

from medsutil.awaretime import AwareDateTime
from medsutil.storage.base import BaseStorageHandle, local_file_error_wrap, local_file_generator_error_wrap
from medsutil.storage.interface import StatResult, FeatureFlag
import stat


class LocalHandle(BaseStorageHandle):
    """Handle for a file stored on a local disk or accessible network drive.

        The underlying functionality is based on pathlib.Path with additional
        caching layers.
    """

    def __init__(self, path: str | pathlib.Path, force_is_dir: bool = None, **kwargs):
        path: str = str(path)
        if path.startswith("file://"):
            path = path[7:]
        path = str(pathlib.Path(path).expanduser().absolute().resolve()).replace('\\', '/')
        if force_is_dir is None:
            if path.endswith('/'):
                force_is_dir = True
        else:
            if force_is_dir and not path.endswith('/'):
                path += '/'
            elif not force_is_dir:
                path.rstrip('/')
        super().__init__(
            path,
            force_is_dir,
            supports=FeatureFlag.DEFAULT | FeatureFlag.CHMOD,
            log_name='local',
            **kwargs
        )

    @property
    def pathlib_path(self) -> pathlib.Path:
        return self._with_cache('pathlib_path', pathlib.Path, self._path)

    @local_file_error_wrap
    def _stat(self) -> StatResult:
        """Retrieve the stat information about the file handle."""
        p = self.pathlib_path
        try:
            s = p.stat()
            return StatResult(
                exists=True,
                is_dir=stat.S_ISDIR(s.st_mode),
                is_file=stat.S_ISREG(s.st_mode),
                st_size=s.st_size,
                st_mtime=AwareDateTime.fromtimestamp(s.st_mtime) if s.st_mtime is not None else None
            )
        except FileNotFoundError:
            return StatResult(exists=False)

    @local_file_error_wrap
    def _streaming_write(self, chunks: t.Iterable[bytes], **kwargs):
        self._halt_flag.write_all(self._path, chunks)
        self.clear_cache('stat')

    @local_file_error_wrap
    def _complete_upload(self, local_path):
        if isinstance(local_path, (str, pathlib.Path)):
            shutil.copystat(local_path, self._path)
        super()._complete_upload(local_path)

    @local_file_generator_error_wrap
    def _streaming_read(self, buffer_size: int = None):
        with open(self._path, 'rb') as h:
            yield from self._halt_flag.read_all(h, buffer_size)

    @local_file_error_wrap
    def _complete_download(self, local_path: pathlib.Path):
        shutil.copystat(self._path, local_path)
        super()._complete_download(local_path)

    def from_absolute_path(self, path: str, as_dir: bool | None = None) -> t.Self:
        return self._build_descriptor(path, as_dir)

    def _name(self) -> str:
        return self.pathlib_path.name

    @local_file_error_wrap
    def _chmod(self, mode: int):
        self.pathlib_path.chmod(mode)

    @local_file_error_wrap
    def _mkdir_and_parents(self, mode: int = 0o777, parents: bool = True):
        self.pathlib_path.mkdir(mode, parents=parents, exist_ok=True)
        self.clear_cache('stat')

    @local_file_error_wrap
    def _mkdir(self, mode: int = 0o777):
        self.pathlib_path.mkdir(mode, exist_ok=True)
        self.clear_cache('stat')

    @local_file_error_wrap
    def _remove(self):
        self.pathlib_path.unlink(True)
        self._update_stat(is_dir=None, is_file=None, exists=False, st_size=None, st_mtime=None)

    def _parent(self) -> t.Self:
        new_p = self.pathlib_path.parent
        if new_p == self._path:
            return self
        return self._build_descriptor(str(new_p), as_dir=True)

    def child(self, name: str, as_dir: bool | None = None) -> t.Self:
        return self._build_descriptor(
            str(self.pathlib_path / name),
            as_dir=as_dir
        )

    @local_file_generator_error_wrap
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        files = []
        dirs = []
        for file in self._halt_flag.iterate(self.pathlib_path.iterdir()):
            if file.is_dir():
                dirs.append(file.name)
            else:
                files.append(file.name)
        yield self._path, dirs, files
        for dir_ in dirs:
            subd = self.subdir(dir_)
            yield from subd._walk()

    @local_file_error_wrap
    def _touch(self, mode: int = 0o666):
        self.pathlib_path.touch(mode)
        self.clear_cache('stat')

    @local_file_error_wrap
    def _local_fast_move(self, destination: LocalHandle, allow_overwrite: bool = False) -> LocalHandle:
        self.pathlib_path.move(destination.pathlib_path)
        self._update_stat(is_dir=None, is_file=None, exists=False, st_size=None, st_mtime=None)
        return destination

    @local_file_error_wrap
    def _local_fast_copy(self, destination: LocalHandle, allow_overwrite: bool = False) -> LocalHandle:
        self.pathlib_path.copy(destination.pathlib_path)
        return destination

    @staticmethod
    def supports(file_path: str) -> bool:
        return '://' not in file_path or file_path.startswith('file://')
