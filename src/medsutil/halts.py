"""
    Provides support for a halt flag system.

    The halt flag system is designed to work around having a clean system shutdown. The system will make a halt
    flag and pass it to any long-running processes. These processes should check regularly if the halt flag is set
    and cleanly stop their work if it is in a way that can be recovered.

    This is done with an "Event" which is basically any boolean flag that can indicate if the shutdown has been
    requested or not. It must be implemented using thread-safety or cross-process-safety where threading or
    multiprocessing has been used.

"""
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
    """ An unsafe implementation that uses a plain boolean value. Used for testing. """

    def __init__(self):
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self):
        self._set = True

    def clear(self):
        self._set = False


class HaltFlag:
    """ Represents a flag that needs to be checked in case a halt needs to be called.. """

    def __init__(self, event: ct.SupportsEvent):
        """
        :param event: A flag that can be checked.
        """
        self.event = event

    def breakpoint(self):
        """ Checks if we should continue.
            :raises HaltInterrupt: if the system is shutting down.
        """
        self.check_continue(True)

    def check_continue(self, raise_ex: bool = True) -> bool:
        """ Checks if we should continue.
            :raises HaltInterrupt: If raise_ex is True and the system is shutting down.
        """
        if not self._should_continue():
            if raise_ex:
                raise HaltInterrupt()
            return False
        return True

    def _should_continue(self) -> bool:
        """ Internal check if we should continue. """
        return not self.event.is_set()

    def iterate[X](self, iterable: t.Iterable[X], raise_ex: bool = True) -> t.Generator[X, None, None]:
        """ Iterates through the given iterable, calling `check_continue(raise_ex)` after each item.
            :raises HaltInterrupt: If the system is shutting down and raise_ex is true.
        """
        for x in iterable:
            yield x
            if not self.check_continue(raise_ex):
                break

    def read_all(self,
                 readable: ct.SupportsBinaryRead,
                 chunk_size: int = None) -> t.Generator[t.ByteString, None, None]:
        """ Reads all data from a file, calling `breakpoint()` after each item has been yielded.
            :raises HaltInterrupt: If the system is shutting down.
            :raises TypeError: If `readable` is not supported
        """
        if ct.is_binary_readable(readable):
            chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
            while (data := readable.read(chunk_size)) not in (b'', ''):
                yield data
                self.breakpoint()
        else:
            raise TypeError(f'Type [{readable.__class__.__name__}] is not supported')

    def write_all(self, writable: ct.SupportsBinaryWrite, data: t.Iterable, remove_on_halt: bool = True):
        """ Writes all data to a file, calling `breakpoint()` after each chunk has been written.
            If remove_on_halt is true, the file is removed if the process is interrupted.
            :raises HaltInterrupt: If the system is shutting down.
            :raises TypeError: If writable is not writable.
        """
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

    def copy_data(self,
                  readable: ct.SupportsBinaryRead,
                  writable: ct.SupportsBinaryWrite,
                  chunk_size: int = None,
                  remove_on_halt: bool = True):
        """ Copies all data from one file to another, checking after every read and write.
            :raises HaltInterrupt: If the system is shutting down.
            :raises TypeError: If readable is not readable or writable is not writable.
        """
        self.write_all(writable, self.read_all(readable, chunk_size), remove_on_halt)

    @staticmethod
    def _iterate[X](iterable: t.Iterable[X], halt_flag=None, raise_ex: bool = True) -> t.Generator[X, None, None]:
        """ Iterates through all items in the given iterable and checks the halt flag between each item (if one is
            provided).
        """
        if halt_flag is None:
            yield from iterable
        else:
            yield from halt_flag.iterate(iterable, raise_ex)



class DummyHaltFlag(HaltFlag):
    """ A halt flag that uses a dummy event. """

    def __init__(self):
        super().__init__(DummyEvent())


def copy_with_halt(source_handle: ct.SupportsBinaryRead,
                   destination_handle: ct.SupportsBinaryWrite,
                   chunk_size: int = None,
                   halt_flag: HaltFlag = None):
    """Copy a file with halt flag support. If no halt flag is specified, shutil.copyfileobj is used. """
    if halt_flag is None:
        shutil.copyfileobj(source_handle, destination_handle, chunk_size or DEFAULT_CHUNK_SIZE)
    else:
        halt_flag.copy_data(source_handle, destination_handle, chunk_size)


def gzip_with_halt(source_file: pathlib.Path,
                   target_file: pathlib.Path,
                   chunk_size: int = None,
                   halt_flag: HaltFlag = None):
    """Gzip a file into the target file with halt flag checking. ."""
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
    """Ungzip a file into the target file with halt flag checking."""
    try:
        with gzip.open(source_file, 'rb') as src:
            with open(target_file, 'wb') as dest:
                copy_with_halt(src, dest, chunk_size, halt_flag)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex
