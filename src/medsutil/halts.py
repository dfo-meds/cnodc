import gzip
import os
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

    def read_all(self, readable: ct.SupportsBinaryRead, chunk_size: int = None) -> t.Iterable[t.ByteString]:
        if ct.is_binary_readable(readable):
            chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
            while (data := readable.read(chunk_size)) not in (b'', ''):
                yield data
                self.breakpoint()
        else:
            raise TypeError(f'Type [{readable.__class__.__name__}] is not supported')

    def write_all(self, writable: ct.SupportsByteStreamWriting, data: t.Iterable, remove_on_halt: bool = True):
        if ct.is_local_path(writable):
            try:
                with open(writable, 'wb') as h:
                    self.write_all(h, data, remove_on_halt)
            except HaltInterrupt:
                if remove_on_halt and os.path.exists(writable):
                    os.unlink(writable)
                raise
        elif ct.is_binary_writable(writable):
            for x in data:
                writable.write(x)
                self.breakpoint()
        elif isinstance(writable, bytearray):
            for x in data:
                writable.extend(x)
                self.breakpoint()
        else:
            raise TypeError(f'Type [{writable.__class__.__name__}] is not supported')

    def copy_data(self, readable: ct.SupportsByteStreaming, writable: ct.SupportsByteStreamWriting, chunk_size: int = None, remove_on_halt: bool = True):
        self.write_all(writable, self.read_all(readable, chunk_size), remove_on_halt)

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
