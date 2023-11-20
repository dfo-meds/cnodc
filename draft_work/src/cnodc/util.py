import math
import typing as t
import importlib
from .exc import CNODCError
import abc


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]


class HaltInterrupt(KeyboardInterrupt):
    pass


class HaltFlag(t.Protocol):

    def check(self, raise_ex: bool = True) -> bool:
        raise NotImplementedError()

    def sleep(self, time_seconds: float):
        raise NotImplementedError()


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
