from .exceptions import CNODCError, ConfigError
import typing as t
import importlib
import abc
import time


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]


class HaltInterrupt(KeyboardInterrupt):
    pass


class HaltFlag(t.Protocol):

    def breakpoint(self):
        self.check_continue(True)

    def check_continue(self, raise_ex: bool = True) -> bool:
        if not self._should_continue():
            if raise_ex:
                raise HaltInterrupt()
            return False
        return True

    def _should_continue(self) -> bool:
        raise NotImplementedError()

    def sleep(self, time_seconds: float):
        time.sleep(time_seconds)

    @staticmethod
    def iterate(iterable: t.Iterable, halt_flag=None, raise_ex: bool = True):
        if halt_flag is None:
            yield from iterable
        else:
            for x in iterable:
                if not halt_flag.check_continue(raise_ex):
                    break
                yield x


class DynamicObjectLoadError(CNODCError):
    pass


@t.runtime_checkable
class Readable(t.Protocol):

    @abc.abstractmethod
    def read(self, chunk_size: int) -> bytes:
        pass


@t.runtime_checkable
class Writable(t.Protocol):

    @abc.abstractmethod
    def write(self, b: bytes):
        pass


def dynamic_object(cls_name):
    if "." not in cls_name:
        raise DynamicObjectLoadError(f"cls_name should be in format package.class [actual {cls_name}]", "DOBJ", 1000)
    package_dot_pos = cls_name.rfind(".")
    package = cls_name[0:package_dot_pos]
    specific_cls_name = cls_name[package_dot_pos + 1:]
    try:
        mod = importlib.import_module(package)
        return getattr(mod, specific_cls_name)
    except ModuleNotFoundError as ex:
        raise DynamicObjectLoadError(f"Package or module [{package}] not found", "DOBJ", 1001) from ex
    except AttributeError as ex:
        raise DynamicObjectLoadError(f"Class [{specific_cls_name}] not found in [{package}]", "DOBJ", 1002) from ex
