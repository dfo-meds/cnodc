import shutil
import typing as t

from cnodc.util.exceptions import HaltInterrupt
from cnodc.util.constants import DEFAULT_CHUNK_SIZE
from cnodc.util.protocols import EventProtocol, Readable, Writable


class HaltFlag:

    def __init__(self, event: EventProtocol):
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

    def read_all(self, readable: Readable, chunk_size: int = None, raise_ex: bool = False):
        chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        while (data := readable.read(chunk_size)) not in (b'', ''):
            yield data
            if not self.check_continue(raise_ex):
                break

    def copy_data(self, readable: Readable, writable: Writable, chunk_size: int = None, raise_ex: bool = True):
        chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        while (data := readable.read(chunk_size)) not in (b'', ''):
            writable.write(data)
            if not self.check_continue(raise_ex):
                break

    @staticmethod
    def _iterate(iterable: t.Iterable, halt_flag=None, raise_ex: bool = True):
        if halt_flag is None:
            yield from iterable
        else:
            yield from halt_flag.iterate(iterable, raise_ex)


class DummyEvent:

    def __init__(self):
        self._set = False

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def clear(self):
        self._set = False


class DummyHaltFlag(HaltFlag):

    def __init__(self):
        super().__init__(DummyEvent())
