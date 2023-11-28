import pathlib
import datetime
from .base import UrlBaseHandle, StorageTier, DirFileHandle

from cnodc.util import HaltFlag
import typing as t

import shutil


class LocalHandle(DirFileHandle):

    def __init__(self, path: pathlib.Path):
        super().__init__()
        self._path = path.resolve()

    def stat(self, clear_cache: bool = False):
        return self._with_cache('stat', self._path.stat, clear_cache=clear_cache)

    def _exists(self) -> bool:
        return self._path.exists()

    def _is_dir(self) -> bool:
        return self._path.is_dir()

    def _write_chunks(self, chunks: t.Iterable[bytes], halt_flag: HaltFlag = None):
        DirFileHandle._local_write_chunks(self._path, chunks, halt_flag)

    def _complete_upload(self, local_path: pathlib.Path, halt_flag: HaltFlag = None):
        shutil.copystat(local_path, self._path)
        self._stat_cache = None

    def _read_chunks(self, buffer_size: int = None, halt_flag: HaltFlag = None):
        return DirFileHandle._local_read_chunks(self._path, buffer_size, halt_flag)

    def _complete_download(self, local_path: pathlib.Path, halt_flag: HaltFlag = None):
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

    def child(self, sub_path: str):
        return LocalHandle(self._path / sub_path)

    def walk(self, recursive: bool = True, files_only: bool = True, halt_flag: HaltFlag = None) -> t.Iterable:
        work = [self._path]
        while work:
            d = work.pop()
            for file in d.iterdir():
                if halt_flag:
                    halt_flag.check_continue(True)
                if file.is_dir():
                    work.append(file)
                else:
                    yield LocalHandle(file)

    def size(self, clear_cache: bool = False) -> int:
        return self.stat(clear_cache).st_size

    @staticmethod
    def supports(file_path: str) -> bool:
        return True

    @classmethod
    def build(cls, file_path: str) -> DirFileHandle:
        if file_path.startswith("file://"):
            return cls(pathlib.Path(file_path[7:]).resolve())
        return cls(pathlib.Path(file_path).resolve())