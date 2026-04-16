"""Local file handle"""
import pathlib
import typing as t
import shutil

from medsutil.awaretime import AwareDateTime
from medsutil.storage.base import BaseStorageHandle, local_file_error_wrap, local_file_generator_error_wrap
from medsutil.storage.interface import StatResult, FeatureFlag


class LocalHandle(BaseStorageHandle):
    """Handle for a file stored on a local disk or accessible network drive.

        The underlying functionality is based on pathlib.Path with additional
        caching layers.
    """

    def __init__(self, path: str, force_is_dir: bool = None, **kwargs):
        if path.startswith("file://"):
            path = path[7:]
        if force_is_dir is None:
            if path.endswith('/'):
                force_is_dir = True
        super().__init__(
            str(pathlib.Path(path).expanduser().absolute().resolve()),
            force_is_dir,
            supports=FeatureFlag.DEFAULT | FeatureFlag.CHMOD,
            **kwargs
        )

    @property
    def pathlib_path(self) -> pathlib.Path:
        return self._with_cache('pathlib_path', pathlib.Path, self._path)

    @local_file_error_wrap
    def _stat(self) -> StatResult:
        """Retrieve the stat information about the file handle."""
        p = self.pathlib_path
        if p.exists():
            s = p.stat()
            return StatResult(
                exists=True,
                is_dir=p.is_dir(),
                is_file=p.is_file(),
                st_size=s.st_size,
                st_mtime=AwareDateTime.fromtimestamp(s.st_mtime) if s.st_mtime is not None else None
            )
        return StatResult(exists=False)

    def streaming_write(self, chunks: t.Iterable[bytes]):
        self._local_write_chunks(self._path, chunks)

    @local_file_error_wrap
    def _complete_upload(self, local_path: pathlib.Path):
        if isinstance(local_path, (str, pathlib.Path)):
            shutil.copystat(local_path, self._path)
        super()._complete_upload(local_path)

    def streaming_read(self, buffer_size: int = None):
        return self._local_read_chunks(self._path, buffer_size)

    @local_file_error_wrap
    def _complete_download(self, local_path: pathlib.Path):
        shutil.copystat(self._path, local_path)
        super()._complete_download(local_path)

    def _name(self) -> str:
        return self.pathlib_path.name

    def _chmod(self, mode: int):
        self.pathlib_path.chmod(mode)

    def _mkdir(self, mode: int = 0o777):
        self.pathlib_path.mkdir(mode)

    @local_file_error_wrap
    def _remove(self):
        self.pathlib_path.unlink(True)

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

    def _touch(self, mode: int = 0o666):
        self.pathlib_path.touch(mode)

    def _local_fast_move(self, destination: LocalHandle, allow_overwrite: bool = False) -> LocalHandle:
        self.pathlib_path.move(destination.pathlib_path)
        return destination

    def _local_fast_copy(self, destination: LocalHandle, allow_overwrite: bool = False) -> LocalHandle:
        self.pathlib_path.copy(destination.pathlib_path)
        return destination

    @staticmethod
    def supports(file_path: str) -> bool:
        return '://' not in file_path or file_path.startswith('file://')
