import abc
import typing as t


class EventProtocol(t.Protocol):

    def is_set(self) -> bool: pass  # pragma: no cover
    def clear(self): pass  # pragma: no cover
    def set(self): pass  # pragma: no cover


@t.runtime_checkable
class Readable(t.Protocol):

    @abc.abstractmethod
    def read(self, chunk_size: int) -> bytes:
        pass  # pragma: no cover


@t.runtime_checkable
class Writable(t.Protocol):

    @abc.abstractmethod
    def write(self, b: bytes):
        pass  # pragma: no cover
