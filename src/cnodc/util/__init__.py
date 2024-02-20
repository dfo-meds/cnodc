import datetime
import gzip
import math
import os
import pathlib
import shutil
import struct

from .exceptions import CNODCError, ConfigError
import typing as t
import importlib
import abc
import time


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]


def vlq_encode(number: int) -> bytearray:
    result = bytearray()
    while number >= 0b10000000:
        bits = number & 0b01111111
        number >>= 7
        result.append(bits | 0b10000000)
    result.append(number)
    return result


def vlq_decode(bytes_: bytes) -> tuple[int, int]:
    total = 0
    shift = 0
    pos = 0
    while pos < len(bytes_):
        total += (bytes_[pos] & 0b01111111) << shift
        shift += 7
        if not bytes_[pos] & 0b10000000:
            break
        pos += 1
    return total, pos + 1


def clean_for_json(data):
    if isinstance(data, dict):
        return {
            x: clean_for_json(data[x]) for x in data
        }
    elif isinstance(data, (set, list, tuple)):
        return [clean_for_json(x) for x in data]
    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    else:
        return data


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
        raise DynamicObjectLoadError(f"Object [{specific_cls_name}] not found in [{package}]", "DOBJ", 1002) from ex


def is_close(a, sigma_a, b, sigma_b, rel_tol, abs_tol):
    if a > b:
        a_lower_bound = a - sigma_a
        b_upper_bound = b + sigma_b
        return a_lower_bound < b_upper_bound or math.isclose(a_lower_bound, b_upper_bound, rel_tol=rel_tol, abs_tol=abs_tol)


# NB:
# Using shutil.copyfileobj() is fairly fast but doesn't have a halt flag. Therefore, a very big file
# (e.g. tbs) may cause significant issues during halting. The below methods allow a halt_flag to be
# passed which will halt the copy process and remove the target file. The chunk size was based on testing:
# 2.5 MiB per read translates to about 0.5 seconds between reads. Thus, splitting the
# file into roughly this size of chunks should allow the script to break within 0.5 seconds still.
# Overall performance is similar to using shutil.copyfileobj().

def haltable_ungzip(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=2621440, halt_flag: HaltFlag = None):
    """Ungzip a file into the target file."""
    try:
        with gzip.open(source_file, 'rb') as src:
            with open(target_file, 'wb') as dest:
                if halt_flag is None:
                    shutil.copyfileobj(src, dest)
                else:
                    halt_flag.check_continue(True)
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        halt_flag.check_continue(True)
                        dest.write(src_bytes)
                        halt_flag.check_continue(True)
                        src_bytes = src.read(chunk_size)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex


def haltable_gzip(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=2621440, halt_flag: HaltFlag = None):
    """Gzip a file into the target file."""
    try:
        with open(source_file, 'rb') as src:
            with gzip.open(target_file, 'wb') as dest:
                if halt_flag is None:
                    shutil.copyfileobj(src, dest)
                else:
                    halt_flag.check_continue(True)
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        halt_flag.check_continue(True)
                        dest.write(src_bytes)
                        halt_flag.check_continue(True)
                        src_bytes = src.read(chunk_size)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex
