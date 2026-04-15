import gzip
import pathlib
import shutil
import typing as t

from medsutil.exceptions import HaltInterrupt
import medsutil.types as ct


DEFAULT_CHUNK_SIZE = 10485760
""" Default number of bytes to transfer before checking if the system has called for a shutdown. """


class DummyEvent:

    def __init__(self):
        self._set = False

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def clear(self):
        self._set = False


class HaltFlag:

    def __init__(self, event: ct.SupportsEvent):
        self.event = event

    def breakpoint(self):
        self.check_continue(True)

    def check_continue(self, raise_ex: bool = True) -> bool:
        if not self._should_continue():
            if raise_ex:
                raise HaltInterrupt()
            return False
        return True

    def _should_continue(self) -> bool:
        return not self.event.is_set()

    def iterate(self, iterable: t.Iterable, raise_ex: bool = True):
        for x in iterable:
            yield x
            if not self.check_continue(raise_ex):
                break

    def read_all(self, readable: ct.SupportsBinaryRead, chunk_size: int = None):
        chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        while (data := readable.read(chunk_size)) not in (b'', ''):
            yield data
            self.check_continue()

    def write_all(self, writable: ct.SupportsBinaryWrite, data: t.Iterable):
        for x in data:
            writable.write(x)
            self.check_continue()

    def copy_data(self, readable: ct.SupportsBinaryRead, writable: ct.SupportsBinaryWrite, chunk_size: int = None):
        chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        while (data := readable.read(chunk_size)) not in (b'', ''):
            self.check_continue()
            writable.write(data)
            self.check_continue()

    @staticmethod
    def _iterate(iterable: t.Iterable, halt_flag=None, raise_ex: bool = True):
        if halt_flag is None:
            yield from iterable
        else:
            yield from halt_flag.iterate(iterable, raise_ex)



class DummyHaltFlag(HaltFlag):

    def __init__(self):
        super().__init__(DummyEvent())


def copy_with_halt(source_handle: ct.SupportsBinaryRead,
                   destination_handle: ct.SupportsBinaryWrite,
                   chunk_size: int = None,
                   halt_flag: HaltFlag = None):
    """Copy a file with halt flag support"""
    if halt_flag is None:
        shutil.copyfileobj(source_handle, destination_handle, chunk_size or DEFAULT_CHUNK_SIZE)
    else:
        halt_flag.copy_data(source_handle, destination_handle, chunk_size)


def gzip_with_halt(source_file: pathlib.Path,
                   target_file: pathlib.Path,
                   chunk_size: int = None,
                   halt_flag: HaltFlag = None):
    """Gzip a file into the target file."""
    try:
        with open(source_file, 'rb') as src:
            with gzip.open(target_file, 'wb') as dest:
                copy_with_halt(src, dest, chunk_size, halt_flag)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex


def ungzip_with_halt(source_file: pathlib.Path,
                     target_file: pathlib.Path,
                     chunk_size: int = None,
                     halt_flag: HaltFlag = None):
    """Ungzip a file into the target file."""
    try:
        with gzip.open(source_file, 'rb') as src:
            with open(target_file, 'wb') as dest:
                copy_with_halt(src, dest, chunk_size, halt_flag)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex
