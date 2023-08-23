import logging
import typing as t
import abc
import pathlib
import os
import itertools
import cnodc.ocproc2 as ocproc2
from cnodc.ocproc2 import DataRecord

TranscodingResult = t.Union[
    ocproc2.RecordSet,
    ocproc2.DataRecord,
    t.Iterable[ocproc2.RecordSet],
    t.Iterable[ocproc2.DataRecord]
]

TranscodingSingleResult = t.Union[
    ocproc2.RecordSet,
    ocproc2.DataRecord
]

TranscodingMultipleResult = t.Union[
    t.Iterable[ocproc2.RecordSet],
    t.Iterable[ocproc2.DataRecord]
]


def ocproc2_from_dict(d: dict):
    if isinstance(d, dict):
        if 'C' in d or 'P' in d or 'S' in d or 'M' in d or ('type' in d and d['type'] == 'DataRecord'):
            return ocproc2.DataRecord().from_mapping(d)
        elif 'R' in d or 'E' in d or ('type' in d and d['type'] == 'RecordSet'):
            return ocproc2.RecordSet().from_mapping(d)
        else:
            raise ValueError(f"Cannot detect object type from dictionary")
    elif isinstance(d, list):
        return ocproc2.RecordSet().from_mapping(d)
    raise ValueError(f"Input is not a type")


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


class BufferedBinaryReader:

    def __init__(self, data: t.Iterable[t.Iterable[int]]):
        self._data = data
        self._buffer = bytearray()
        self._buffer_length = 0
        self._iter = None
        self._complete = False
        self._start_offset = 0

    def _load_more(self) -> bool:
        if self._complete:
            return False
        if self._iter is None:
            self._iter = iter(self._data)
        nxt = next(self._iter, None)
        if nxt is None:
            self._complete = True
            return False
        else:
            self._buffer.extend(nxt)
            self._buffer_length = len(self._buffer)
            return True

    def _load_rest(self):
        if self._iter is None:
            self._iter = iter(self._data)
        nxt = next(self._iter, None)
        while nxt is not None:
            self._buffer.extend(nxt)
            nxt = next(self._iter, None)
        self._buffer_length = len(self._buffer)
        self._complete = True

    def _true_current_len(self):
        return self._buffer_length + self._start_offset

    def is_at_end(self):
        if self._buffer_length > 0:
            return False
        if self._complete:
            return True
        while self._buffer_length == 0:
            if not self._load_more():
                break
        return self._buffer_length == 0

    def current_offset(self):
        return self._start_offset

    def consume_by_lines(self):
        while not self.is_at_end():
            yield self.consume_until([b"\n", b"\r"])
            self.skip_bytes(b"\r")
            if self.peek(1) == b"\n":
                self.discard_buffer(self._start_offset + 1)

    def consume(self, length: int, start_at: int = 0) -> bytearray:
        self._make_index_valid(length + start_at + self._start_offset)
        res = self._buffer[start_at:start_at+length]
        self.discard_buffer(start_at + length + self._start_offset)
        return res

    def consume_until(self, bytes_: t.Union[list, bytes], include_target: bool = False) -> bytearray:
        res, target_offset = self.find_first([bytes_]) if isinstance(bytes_, bytes) else self.find_first(bytes_)
        if target_offset is None:
            return self.consume(len(self._buffer))
        else:
            return self.consume(target_offset + (len(res) if include_target else 0))

    def discard_buffer(self, up_to_byte: int):
        up_to_byte -= self._start_offset
        if up_to_byte < self._buffer_length:
            self._buffer = self._buffer[up_to_byte:]
            self._buffer_length = len(self._buffer)
            self._start_offset += up_to_byte
        else:
            self._buffer = bytearray()
            self._start_offset += self._buffer_length
            self._buffer_length = 0

    def skip_bytes(self, v: bytes):
        idx = 0
        while True:
            if not self._make_index_valid(idx + self._start_offset):
                if idx > 0:
                    self.discard_buffer(idx + self._start_offset)
                break
            if self._buffer[idx] in v:
                idx += 1
                continue
            else:
                if idx > 0:
                    self.discard_buffer(idx + self._start_offset)
                break

    def peek(self, l: int):
        self._make_index_valid(l + self._start_offset)
        return self._buffer[0:l]

    def lstrip(self, v: bytes):
        idx = 0
        while True:
            if not self._make_index_valid(idx + self._start_offset):
                return BufferedBinaryReader([])
            if self._buffer[idx] in v:
                idx += 1
                continue
            else:
                if idx == 0:
                    return self
                return BufferedBinaryReader(itertools.chain([self._buffer[idx:]], self._data))

    def subset(self, start_index: t.Optional[int] = None, stop_index: t.Optional[int] = None, step: t.Optional[int] = None):
        if stop_index is not None and stop_index < (self._buffer_length + self._start_offset):
            if start_index is None:
                start_index = self._start_offset
            if step is None:
                step = 1
            subset = self._buffer[start_index - self._start_offset:stop_index - self._start_offset:step]
            return BufferedBinaryReader((subset,))
        return BufferedBinaryReader((self.iterate(start_index, stop_index, step),))

    def _make_index_valid(self, idx: int) -> bool:
        if idx < 0:
            raise ValueError(f"Invalid index [{idx}], negative indicies not allowed")
        if idx < self._start_offset:
            raise ValueError(f"Invalid index [{idx}], buffered content has been discarded")
        while idx >= self._true_current_len():
            if not self._load_more():
                return False
        return True

    def __iter__(self) -> t.Iterator[int]:
        return self.iterate(0, None, 1)

    def iterate(self, start_index: t.Optional[int] = None, stop_index: t.Optional[int] = None, step: t.Optional[int] = None) -> t.Iterator[int]:
        if start_index is None:
            start_index = self._start_offset
        if step is None:
            step = 1
        idx = start_index
        while True:
            if not self._make_index_valid(idx):
                break
            while ((idx - self._start_offset) < self._buffer_length) and (stop_index is None or idx < stop_index):
                yield self._buffer[idx - self._start_offset]
                idx += step
                continue
            if stop_index is not None and idx >= stop_index:
                break

    def __getitem__(self, idx: t.Union[slice, int]) -> t.Union[t.Iterator[int], int]:
        if isinstance(idx, slice):
            return self.iterate(idx.start, idx.stop, idx.step)
        else:
            if not self._make_index_valid(idx):
                raise KeyError(idx)
            return self._buffer[idx]

    def find(self, v: bytes, start_idx: int = None) -> t.Optional[int]:
        _, idx = self.find_first([v], start_idx)
        return idx

    def read_all(self) -> bytearray:
        """Read the rest as one bytearray"""
        self._load_rest()
        return self._buffer

    def read_all_in_chunks(self) -> t.Iterable[bytes]:
        """Read the rest, but in chunks """
        if self._buffer:
            yield self._buffer
            self._buffer = []
            self._buffer_length = 0
        if not self._complete:
            if self._iter is None:
                self._iter = iter(self._data)
            for item in self._iter:
                yield item
            self._complete = True

    def find_first(self, options: t.Iterable[bytes], start_idx: int = None) -> t.Tuple[t.Optional[bytes], t.Optional[int]]:
        if start_idx is None:
            start_idx = self._start_offset
        else:
            start_idx += self._start_offset
        options = set((o, len(o)) for o in options if o is not None)
        opt_max_length = max(o[1] for o in options)
        opt_min_length = min(o[1] for o in options)
        while True:
            if not self._make_index_valid(start_idx + opt_min_length):
                return None, None
            opt = self._check_any_at_position(options, start_idx, opt_max_length)
            if opt is not None:
                return opt, start_idx - self._start_offset
            start_idx += 1

    def _check_any_at_position(self, options: set, idx: int, opt_max_len: int):
        self._make_index_valid(idx + opt_max_len - 1)
        _true_idx = idx - self._start_offset
        for opt, opt_len in options:
            if _true_idx + opt_len >= self._buffer_length:
                continue
            # Otherwise we can do the same check but with a range
            if all(opt[i] == self._buffer[_true_idx+i] for i in range(0, opt_len)):
                return opt
            else:
                continue
        # No options matched
        return None


class CodecLogger:

    def exception(self, message):
        self.log(logging.ERROR, message)

    def critical(self, message):
        self.log(logging.CRITICAL, message)

    def error(self, message):
        self.log(logging.ERROR, message)

    def warning(self, message):
        self.log(logging.WARNING, message)

    def info(self, message):
        self.log(logging.INFO, message)

    def debug(self, message):
        self.log(logging.DEBUG, message)

    @abc.abstractmethod
    def log(self, level, message):
        raise NotImplementedError()


class CodecBasicLogger(CodecLogger):

    def __init__(self):
        self._log = logging.getLogger("osdt.codec")

    def log(self, level, message):
        self._log.log(level, message)


class CodecStoreLogger(CodecLogger):

    def __init__(self, min_level=logging.NOTSET):
        self.log_store = {}
        self.min_level = min_level

    def log(self, level, message):
        if level >= self.min_level:
            if level not in self.log_store:
                self.log_store[level] = []
            self.log_store[level].append(message)

    def to_list(self):
        messages = []
        for level in self.log_store:
            level_name = logging.getLevelName(level)
            messages.extend(f"[{level_name}] {msg}" for msg in self.log_store[level])


class DecodedMessage:

    def __init__(self,
                 message_idx: int,
                 binary_content: t.Union[bytes, bytearray],
                 logger: CodecLogger,
                 records: TranscodingResult = None):
        self.message_idx = message_idx
        self.binary_content = binary_content
        self.logger = logger
        self.records = records

    def iterate_records(self) -> tuple[int, DataRecord]:
        if self.records:
            for idx, r in enumerate(self.records):
                yield idx, r


class CodecProtocol(t.Protocol):

    @abc.abstractmethod
    def set_logger(self, logger: CodecLogger):
        raise NotImplementedError()

    @abc.abstractmethod
    def description(self):
        raise NotImplementedError()

    def dump(self,
             record_set: TranscodingResult,
             output_file: t.Union[Writable, str, os.PathLike],
             **kwargs):
        if isinstance(output_file, Writable):
            self._write_to_writable(output_file, record_set, kwargs)
        else:
            with open(output_file, "wb") as fp:
                self._write_to_writable(fp, record_set, kwargs)

    def _write_to_writable(self, fp: Writable, record_set: TranscodingResult, kwargs):
        for _bytes in self.encode(record_set, **kwargs):
            fp.write(_bytes)

    def load(self,
             input_file: t.Union[Readable, bytes, bytearray, str, os.PathLike, t.Iterable],
             chunk_size: int = 16384,
             **kwargs) -> TranscodingResult:
        if isinstance(input_file, Readable):
            return self.decode(CodecProtocol.read_in_chunks(input_file, chunk_size), **kwargs)
        elif isinstance(input_file, (bytes, bytearray)):
            return self.decode((input_file,), **kwargs)
        elif isinstance(input_file, (str, os.PathLike)):
            return self.decode(CodecProtocol.read_from_file(input_file, chunk_size), **kwargs)
        else:
            return self.decode(input_file, **kwargs)

    @staticmethod
    def read_from_file(file_path, chunk_size: int = 16384) -> t.Iterable[bytes]:
        with open(file_path, "rb") as fp:
            yield from CodecProtocol.read_in_chunks(fp, chunk_size)

    @staticmethod
    def read_in_chunks(has_read: Readable, chunk_size: int = 16384) -> t.Iterable[bytes]:
        piece = has_read.read(chunk_size)
        while piece:
            yield piece
            piece = has_read.read(chunk_size)

    def load_messages(
             self,
             input_file: t.Union[Readable, bytes, bytearray, str, os.PathLike, t.Iterable],
             chunk_size: int = 16384,
             **kwargs) -> t.Iterable[DecodedMessage]:
        if isinstance(input_file, Readable):
            return self.decode_messages(CodecProtocol.read_in_chunks(input_file, chunk_size), **kwargs)
        elif isinstance(input_file, (bytes, bytearray)):
            return self.decode_messages((input_file,), **kwargs)
        elif isinstance(input_file, (str, os.PathLike)):
            return self.decode_messages(CodecProtocol.read_from_file(input_file, chunk_size), **kwargs)
        else:
            return self.decode_messages(input_file, **kwargs)

    @abc.abstractmethod
    def encode(self, records: TranscodingResult, **kwargs) -> t.Iterable[bytes]:
        raise NotImplementedError()

    def decode(self, data: t.Iterable[bytes], **kwargs) -> TranscodingResult:
        for dm in self.decode_messages(data, **kwargs):
            if dm.records:
                yield from dm.records

    @abc.abstractmethod
    def decode_messages(self, data: t.Iterable[bytes], **kwargs) -> t.Iterable[DecodedMessage]:
        raise NotImplementedError()

    @abc.abstractmethod
    def check_compatibility(self, file_path: pathlib.Path) -> bool:
        raise NotImplementedError()


class BaseCodec(CodecProtocol):

    def __init__(self, desc: str = "", ext: t.Optional[str] = None):
        self._desc = desc
        self._ext = ext
        self.logger = CodecLogger()

    def set_logger(self, logger: CodecLogger):
        self.logger = logger

    def description(self):
        return self._desc

    def check_compatibility(self, file_path: pathlib.Path) -> bool:
        if self._ext is not None:
            return file_path.name.lower().endswith(self._ext.lower())
