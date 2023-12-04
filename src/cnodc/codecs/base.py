import typing as t
from cnodc.ocproc2 import DataRecord
import os

from cnodc.util import CNODCError
from cnodc.util import HaltFlag, HaltInterrupt

ByteIterable: t.Iterable[t.Union[bytes, bytearray]]


class ByteSequenceReader:

    def __init__(self, raw_bytes: ByteIterable, halt_flag: HaltFlag = None):
        self._halt_flag = halt_flag
        self._raw_data = raw_bytes
        self._iterator = None
        self._buffer = bytearray()
        self._buffer_length = 0
        self._offset = 0
        self._complete = False

    def check_continue(self):
        if self._halt_flag:
            self._halt_flag.check_continue(True)

    def offset(self):
        return self._offset

    def _read_next(self) -> bool:
        if self._complete:
            return False
        if self._iterator is None:
            self._iterator = iter(self._raw_data)
        next_item = next(self._iterator, None)
        if next_item is None:
            self._complete = True
            return False
        self._buffer.extend(next_item)
        self._buffer_length = len(self._buffer)
        return True

    def _read_rest(self):
        if self._complete:
            return
        if self._iterator is None:
            self._iterator = iter(self._raw_data)
        try:
            next_item = next(self._iterator, None)
            while next_item is not None:
                self._buffer.extend(next_item)
                self.check_continue()
                next_item = next(self._iterator, None)
            self._complete = True
        finally:
            self._buffer_length = len(self._buffer)

    def at_eof(self):
        if self._buffer_length > 0:
            return False
        if self._complete:
            return True
        while self._buffer_length == 0:
            if not self._read_next():
                break
        return self._buffer_length == 0

    def _read_at_least(self, actual_index: int, halt_flag: HaltFlag = None) -> bool:
        while not(self.at_eof() or actual_index < self._buffer_length):
            self.check_continue()
            self._read_next()
        return actual_index < self._buffer_length

    def discard_leading(self, length: int):
        if length < self._buffer_length:
            self._offset += self._buffer_length
            self._buffer_length = 0
            self._buffer = bytearray()
        else:
            self._offset += length
            self._buffer = self._buffer[length:]
            self._buffer_length = len(self._buffer)

    def consume_lines(self, exclude_line_endings: bool = True) -> t.Iterable[bytearray]:
        while not self.at_eof():
            self.check_continue()
            res = self.consume_until([b"\n", b"\r"])
            yield res.rstrip(b"\r\n") if exclude_line_endings else res
            self.lstrip(b"\n", 1)

    def consume_all(self) -> bytearray:
        self._read_rest()
        res = self._buffer
        self._buffer = bytearray()
        self._buffer_length = 0
        return res

    def consume_until(self, matches: t.Union[list, bytes], include_target: bool = False) -> bytearray:
        matching, actual_offset = self.find_first_match([matches] if isinstance(matches, bytes) else matches)
        if actual_offset is None:
            return self.consume(self._buffer_length)
        else:
            return self.consume(actual_offset + (len(matching) if include_target else 0))

    def consume(self, length: int) -> bytearray:
        self._read_at_least(length)
        res = self._buffer[0:length]
        self.discard_leading(length)
        return res

    def lstrip(self, bytes_: bytes, max_strip: int = None):
        idx = 0
        while max_strip is None or max_strip > 0:
            if not self._read_at_least(idx):
                break
            if self._buffer[idx] not in bytes_:
                break
            idx += 1
            if max_strip is not None:
                max_strip -= 1
        if idx > 0:
            self.discard_leading(idx)

    def find_first_match(self, matches: list[bytes]) -> tuple[t.Optional[bytes], t.Optional[int]]:
        options = set()
        min_length = None
        max_length = None
        for o in matches:
            o_len = len(o)
            if min_length is None or o_len < min_length:
                min_length = o_len
            if max_length is None or o_len > max_length:
                max_length = o_len
            options.add((o, o_len))
        curr_idx = 0
        while True:
            self.check_continue()
            if not self._read_at_least(curr_idx + min_length):
                return None, None
            opt = self._check_any_at(options, curr_idx, max_length)
            if opt is not None:
                return opt, curr_idx
            curr_idx += 1

    def _check_any_at(self, options: set[tuple[bytes, int]], start_idx: int, max_length: int):
        self._read_at_least(start_idx + max_length)
        for opt, opt_len in options:
            if start_idx + opt_len >= self._buffer_length:
                continue
            if all(opt[i] == self._buffer[start_idx + i] for i in range(0, opt_len)):
                return opt
        return None

    def __getitem__(self, user_index: t.Union[slice, int]) -> t.Union[t.Iterator[int], int]:
        if isinstance(user_index, slice):
            raise NotImplementedError()
        else:
            if not self._read_at_least(user_index):
                raise KeyError(user_index)
            return self._buffer[user_index]


class Readable(t.Protocol):

    def read(self, chunk_size: t.Optional[int] = None) -> bytes:
        raise NotImplementedError()


class Writable(t.Protocol):

    def write(self, data: bytes):
        raise NotImplementedError()


class DecodeResult:

    def __init__(self):
        self.records: t.Optional[list[DataRecord]] = None
        self.success: bool = False
        self.from_exception: t.Optional[Exception] = None

    @staticmethod
    def from_exception(ex: Exception):
        dr = DecodeResult()
        dr.from_exception = ex
        dr.success = False
        return dr

    @staticmethod
    def from_record_list(records: list[DataRecord]):
        dr = DecodeResult()
        dr.success = True
        dr.records = records
        return dr


class EncodeResult(t.Protocol):

    def __init__(self):
        self.data_stream: t.Union[bytearray, bytes, None] = None
        self.success: bool = False
        self.from_exception: t.Optional[Exception] = None

    @staticmethod
    def from_exception(ex: Exception):
        er = EncodeResult()
        er.from_exception = ex
        er.success = False
        return er

    @staticmethod
    def from_bytes(bytes_: t.Union[bytes, bytearray]):
        er = EncodeResult()
        er.data_stream = bytes_
        er.success = True
        return er


class BaseCodec:

    def __init__(self, halt_flag: HaltFlag = None):
        self.is_encoder = False
        self.is_decoder = False
        self._halt_flag = halt_flag

    def dump(self,
             output_file: t.Union[Writable, str, os.PathLike],
             record_set: t.Iterable[DataRecord],
             **kwargs):
        if hasattr(output_file, 'write'):
            self.write_in_chunks(output_file, self.encode_messages(record_set, **kwargs))
        else:
            with open(output_file, "wb") as h:
                self.write_in_chunks(h, self.encode_messages(record_set, **kwargs))

    def load(self,
             file: t.Union[Readable, bytes, bytearray, str, os.PathLike, ByteIterable],
             chunk_size: int = 16384,
             **kwargs):
        if hasattr(file, 'read'):
            return self.decode_messages(self.read_in_chunks(file, chunk_size), **kwargs)
        elif isinstance(file, (bytes, bytearray)):
            return self.decode_messages((file,), **kwargs)
        elif isinstance(file, (str, os.PathLike)):
            with open(file, "rb") as h:
                return self.decode_messages(self.read_in_chunks(h, chunk_size), **kwargs)
        else:
            return self.decode_messages(file, **kwargs)

    def encode_messages(self,
                        data: t.Iterable[DataRecord],
                        **kwargs) -> ByteIterable:
        fail_on_error = bool(kwargs.pop('fail_on_error')) if 'fail_on_error' in kwargs else False
        on_first = True
        yield from self.encode_start(**kwargs)
        for record in HaltFlag.iterate(data, self._halt_flag, True):
            result = self.encode(record)
            if result.success:
                if not on_first:
                    yield from self.encode_separator(**kwargs)
                yield self.encode(record).data_stream
                on_first = False
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error encoding data from file", "CODECS", 1000) from result.from_exception
                else:
                    raise CNODCError(f"Error encoding data from file", "CODECS", 1001)
        yield from self.encode_end(**kwargs)

    def encode_start(self, **kwargs) -> ByteIterable:
        yield b''

    def encode(self,
               record: DataRecord,
               **kwargs) -> EncodeResult:
        raise NotImplementedError()

    def encode_separator(self, **kwargs) -> ByteIterable:
        yield b''

    def encode_end(self, **kwargs) -> ByteIterable:
        yield b''

    def as_byte_sequence(self, bytes_: ByteIterable) -> ByteSequenceReader:
        return ByteSequenceReader(bytes_, self._halt_flag)

    def decode_messages(self,
                        data: ByteIterable,
                        **kwargs) -> t.Iterable[DataRecord]:
        fail_on_error = bool(kwargs.pop('fail_on_error')) if 'fail_on_error' in kwargs else False
        for result in HaltFlag.iterate(self.decode(data), self._halt_flag, True):
            if result.success:
                yield from result.records
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1002) from result.from_exception
                else:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1003)

    def decode(self,
               data: ByteIterable,
               **kwargs) -> t.Iterable[DecodeResult]:
        raise NotImplementedError()

    def write_in_chunks(self, file_handle: Writable, output: ByteIterable):
        for bytes_ in HaltFlag.iterate(output, self._halt_flag, True):
            file_handle.write(bytes_)

    def read_in_chunks(self, file_handle: Readable, chunk_size: int) -> ByteIterable:
        chunk = file_handle.read(chunk_size)
        while chunk != b'':
            if self._halt_flag:
                self._halt_flag.check_continue(True)
            yield chunk
            chunk = file_handle.read(chunk_size)
