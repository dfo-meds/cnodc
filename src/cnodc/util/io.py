import gzip
import pathlib
import shutil
import typing as t
from typing import TextIO

from cnodc.util.exceptions import HaltInterrupt
from cnodc.util.protocols import Readable, Writable
from cnodc.util.halts import HaltFlag
from cnodc.util.constants import DEFAULT_CHUNK_SIZE


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


def copy_with_halt(source_handle: t.Union[Readable, TextIO], destination_handle: Writable, chunk_size: t.Optional[int] = None, halt_flag: t.Optional[HaltFlag] = None):
    """Copy a file with halt flag support"""
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    if not halt_flag:
        shutil.copyfileobj(source_handle, destination_handle, chunk_size)
    else:
        halt_flag.copy_data(source_handle, destination_handle, chunk_size)


def gzip_with_halt(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=None, halt_flag: HaltFlag = None):
    """Gzip a file into the target file."""
    try:
        with open(source_file, 'rb') as src:
            with gzip.open(target_file, 'wb') as dest:
                copy_with_halt(src, dest, chunk_size, halt_flag)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex


def ungzip_with_halt(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=2621440, halt_flag: HaltFlag = None):
    """Ungzip a file into the target file."""
    try:
        with gzip.open(source_file, 'rb') as src:
            with open(target_file, 'wb') as dest:
                copy_with_halt(src, dest, chunk_size, halt_flag)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex
