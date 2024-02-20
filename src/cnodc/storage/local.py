"""Local file handle"""
import pathlib
import datetime
from .base import BaseStorageHandle, local_file_error_wrap
from cnodc.util import HaltFlag
import typing as t
import shutil


class LocalHandle(BaseStorageHandle):
    """Handle for a file stored on a local disk or accessible network drive.

        The underlying functionality is based on pathlib.Path with additional
        caching layers.
    """

    def __init__(self, path: pathlib.Path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = path.expanduser().absolute()

    @local_file_error_wrap
    def stat(self, clear_cache: bool = False):
        """Retrieve the stat information about the file handle."""
        return self._with_cache('stat', self._path.stat, clear_cache=clear_cache)

    def _exists(self) -> bool:
        return self._path.exists()

    def _is_dir(self) -> bool:
        return self._path.is_dir()

    def _write_chunks(self, chunks: t.Iterable[bytes]):
        self._local_write_chunks(self._path, chunks)

    @local_file_error_wrap
    def _complete_upload(self, local_path: pathlib.Path):
        if isinstance(local_path, (str, pathlib.Path)):
            shutil.copystat(local_path, self._path)
        self._stat_cache = None

    def _read_chunks(self, buffer_size: int = None):
        return self._local_read_chunks(self._path, buffer_size)

    @local_file_error_wrap
    def _complete_download(self, local_path: pathlib.Path):
        shutil.copystat(self._path, local_path)

    def _name(self) -> str:
        return self._path.name

    def remove(self):
        self._path.unlink(True)
        self.clear_cache()

    def path(self):
        return str(self._path)

    def modified_datetime(self, clear_cache: bool = False) -> t.Optional[datetime.datetime]:
        m_time = self.stat(clear_cache).st_mtime
        if m_time is not None:
            return datetime.datetime.fromtimestamp(
                self.stat(clear_cache).st_mtime,
                datetime.timezone(datetime.timedelta(hours=0), "UTC")
            )
        return None

    def child(self, sub_path: str, as_dir: bool = False):
        return LocalHandle(self._path / sub_path, halt_flag=self._halt_flag)

    @local_file_error_wrap
    def walk(self, recursive: bool = True) -> t.Iterable[BaseStorageHandle]:
        work = [self._path]
        while work:
            d = work.pop()
            for file in HaltFlag.iterate(d.iterdir(), self._halt_flag, True):
                if file.is_dir():
                    work.append(file)
                else:
                    yield LocalHandle(file, halt_flag=self._halt_flag)

    def size(self, clear_cache: bool = False) -> int:
        return self.stat(clear_cache).st_size

    @staticmethod
    def supports(file_path: str) -> bool:
        return True

    @classmethod
    def build(cls, file_path: str, halt_flag: HaltFlag = None) -> BaseStorageHandle:
        if file_path.startswith("file://"):
            return cls(pathlib.Path(file_path[7:]), halt_flag=halt_flag)
        return cls(pathlib.Path(file_path), halt_flag=halt_flag)
