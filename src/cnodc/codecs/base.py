import pathlib
import typing as t

import zrlog

from cnodc.ocproc2 import DataRecord
import os

from cnodc.util import CNODCError
from cnodc.util import HaltFlag, HaltInterrupt

ByteIterable = t.Iterable[t.Union[bytes, bytearray]]

# TODO: Investigate using mmap here?
class ByteSequenceReader:

    def __init__(self, raw_bytes: ByteIterable, halt_flag: HaltFlag = None):
        self._halt_flag = halt_flag
        self._raw_data = raw_bytes
        self._iterator = None
        self._buffer = bytearray()
        self._buffer_length = 0
        self._offset = 0
        self._complete = False

    def iterate_rest(self) -> t.Iterable[t.Union[bytearray, bytes]]:
        if self._buffer:
            yield self._buffer
            self._offset += self._buffer_length
            self._buffer = bytearray()
            self._buffer_length = 0
        if not self._complete:
            if self._iterator is None:
                self._iterator = iter(self._raw_data)
            next_item = next(self._iterator, None)
            while next_item is not None:
                self._offset += len(next_item)
                yield next_item
                next_item = next(self._iterator, None)
            self._complete = True

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

    def _read_at_least(self, actual_index: int) -> bool:
        while not(self.at_eof() or actual_index < self._buffer_length):
            self.check_continue()
            if not self._read_next():
                break
        return actual_index < self._buffer_length

    def _discard_leading(self, length: int):
        if length >= self._buffer_length:
            self._offset += self._buffer_length
            self._buffer_length = 0
            self._buffer = bytearray()
        else:
            self._offset += length
            self._buffer = self._buffer[length:]
            self._buffer_length = len(self._buffer)

    def peek(self, length: int) -> bytes:
        self._read_at_least(length)
        return self._buffer[0:length]

    def peek_line(self, exclude_line_endings: bool = True) -> bytes:
        while not (b"\n" in self._buffer or b"\r" in self._buffer):
            self.check_continue()
            if not self._read_next():
                break
        next_n = self._buffer.find(b"\n")
        next_r = self._buffer.find(b"\r")
        index = None
        if next_n is not None and next_r is not None:
            index = min(next_n, next_r)
        elif next_n is not None:
            index = next_n
        elif next_r is not None:
            index = next_r
        if index is None:
            return self._buffer
        if not exclude_line_endings:
            index += 1
        return self._buffer[:index]

    def consume_line(self, exclude_line_endings: bool = True) -> bytes:
        res = self.consume_until([b"\n", b"\r"], include_target=True)
        if res[-1] == 13 and self[0] == b"\n":
            self._discard_leading(1)
            if not exclude_line_endings:
                res += b"\n"
        return res.rstrip(b"\r\n") if exclude_line_endings else res

    def consume_lines(self, exclude_line_endings: bool = True) -> t.Iterable[bytearray]:
        while not self.at_eof():
            self.check_continue()
            res = self.consume_until([b"\n", b"\r"], include_target=True)
            if res[-1] == 13 and self[0] == b"\n":
                self._discard_leading(1)
                if not exclude_line_endings:
                    res += b"\n"
            yield res.rstrip(b"\r\n") if exclude_line_endings else res

    def consume_all(self) -> bytearray:
        self._read_rest()
        res = self._buffer
        self._buffer = bytearray()
        self._offset += self._buffer_length
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
        self._discard_leading(length)
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
            self._discard_leading(idx)

    def find_first_match(self, matches: list[bytes]) -> tuple[t.Optional[bytes], t.Optional[int]]:
        options = set()
        min_length = None
        max_length = None
        for o in matches:
            o_len = len(o)
            if o_len > 0:
                if min_length is None or o_len < min_length:
                    min_length = o_len
                if max_length is None or o_len > max_length:
                    max_length = o_len
                options.add((o, o_len))
        if not options:
            return None, None
        curr_idx = 0
        checker = self._check_any_at
        if max_length == 1:
            checker = self._fast_check_any_at
            options = [x[0] for x, l in options]
        while True:
            self.check_continue()
            if not self._read_at_least(curr_idx + min_length):
                return None, None
            opt = checker(options, curr_idx, max_length)
            if opt is not None:
                return opt, curr_idx
            curr_idx += 1

    def _fast_check_any_at(self, options: list[int], start_idx: int, max_length: int) -> t.Optional[bytes]:
        first = self._buffer[start_idx]
        return first.to_bytes(1, 'little') if first in options else None

    def _check_any_at(self, options: set[tuple[bytes, int]], start_idx: int, max_length: int) -> t.Optional[bytes]:
        self._read_at_least(start_idx + max_length)
        for opt, opt_len in options:
            if start_idx + opt_len >= self._buffer_length:
                continue
            if all(opt[i] == self._buffer[start_idx + i] for i in range(0, opt_len)):
                return opt
        return None

    def __getitem__(self, user_index: t.Union[slice, int]) -> t.Union[t.Iterator[bytes], bytes]:
        if isinstance(user_index, slice):
            raise NotImplementedError()
        else:
            if not self._read_at_least(user_index):
                raise KeyError(user_index)
            return self._buffer[user_index].to_bytes(1, 'little')


class Readable(t.Protocol):

    def read(self, chunk_size: t.Optional[int] = None) -> bytes:
        raise NotImplementedError()


class Writable(t.Protocol):

    def write(self, data: bytes):
        raise NotImplementedError()


class DecodeResult:

    def __init__(self,
                 records: t.Optional[list[DataRecord]] = None,
                 exc: t.Optional[Exception] = None,
                 message_idx: int = 0,
                 original: t.Union[bytes, bytearray, None] = None,
                 skipped: bool = False):
        self.records: t.Optional[list[DataRecord]] = records
        self.success = exc is None and records is not None
        self.from_exception: t.Optional[Exception] = exc
        self.message_idx: int = message_idx
        self.original: t.Union[bytes, bytearray, None] = original
        self.skipped = skipped


class EncodeResult:

    def __init__(self,
                 data_stream: t.Optional[ByteIterable] = None,
                 exc: t.Optional[Exception] = None,
                 original: t.Optional[DataRecord] = None):
        self.data_stream: t.Optional[ByteIterable] = data_stream
        self.from_exception: t.Optional[Exception] = exc
        self.original: t.Optional[DataRecord] = original
        self.success: bool = self.from_exception is None and self.data_stream is not None


class BaseCodec:

    def __init__(self, log_name: str, is_encoder: bool = False, is_decoder: bool = False, halt_flag: HaltFlag = None):
        self.is_encoder = is_encoder
        self.is_decoder = is_decoder
        self._halt_flag = halt_flag
        self.log = zrlog.get_logger(log_name)

    def dump(self,
             output_file: t.Union[Writable, str, os.PathLike],
             record_set: t.Iterable[DataRecord],
             **kwargs):
        if hasattr(output_file, 'write'):
            self._write_in_chunks(output_file, self.encode_records(record_set, **kwargs))
        else:
            with open(output_file, "wb") as h:
                self._write_in_chunks(h, self.encode_records(record_set, **kwargs))

    def load_all(self,
                 file: t.Union[Readable, bytes, bytearray, str, os.PathLike, ByteIterable],
                 chunk_size: int = 16384,
                 **kwargs) -> t.Iterable[DataRecord]:
        if hasattr(file, 'read'):
            yield from self.decode_messages(self._read_in_chunks(file, chunk_size), **kwargs)
        elif isinstance(file, (bytes, bytearray)):
            yield from self.decode_messages(BaseCodec._yield_bytes(file), **kwargs)
        elif isinstance(file, (str, os.PathLike, pathlib.Path)):
            with open(file, "rb") as h:
                yield from self.decode_messages(self._read_in_chunks(h, chunk_size), **kwargs)
        else:
            yield from self.decode_messages(file, **kwargs)

    @staticmethod
    def _yield_bytes(b: t.Union[bytes, bytearray]):
        yield b

    def encode_records(self,
                       data: t.Iterable[DataRecord],
                       **kwargs) -> ByteIterable:
        fail_on_error = bool(kwargs.pop('fail_on_error')) if 'fail_on_error' in kwargs else False
        on_first = True
        yield from self._encode_start(**kwargs)
        for record_idx, record in enumerate(HaltFlag.iterate(data, self._halt_flag, True)):
            result = self._encode(record)
            if result.success:
                if not on_first:
                    yield from self._encode_separator(**kwargs)
                yield from self._encode(record).data_stream
                on_first = False
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error encoding data from file, record [{record_idx}]", "CODECS", 1000) from result.from_exception
                else:
                    raise CNODCError(f"Error encoding data from file, record [{record_idx}]", "CODECS", 1001)
        yield from self._encode_end(**kwargs)

    def _encode_start(self, **kwargs) -> ByteIterable:
        yield b''

    def _encode_record(self,
                       record: DataRecord,
                       **kwargs) -> EncodeResult:
        try:
            return self._encode(record, **kwargs)
        except Exception as ex:
            return EncodeResult(
                original=record,
                exc=ex
            )

    def _encode(self,
                record: DataRecord,
                **kwargs) -> EncodeResult:
        raise NotImplementedError()

    def _encode_separator(self, **kwargs) -> ByteIterable:
        yield b''

    def _encode_end(self, **kwargs) -> ByteIterable:
        yield b''

    def _as_byte_sequence(self, bytes_: ByteIterable) -> ByteSequenceReader:
        return ByteSequenceReader(bytes_, self._halt_flag)

    def decode_messages(self,
                        data: ByteIterable,
                        **kwargs) -> t.Iterable[DataRecord]:
        fail_on_error = bool(kwargs.pop('fail_on_error')) if 'fail_on_error' in kwargs else False
        for result in HaltFlag.iterate(self.decode_to_results(data, **kwargs), self._halt_flag, True):
            if result.success:
                yield from result.records
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1002) from result.from_exception
                else:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1003)
            else:
                if result.from_exception:
                    self.log.error(f"Error decoding data from file: {result.from_exception.__class__.__name__}: {str(result.from_exception)}", "CODECS", 1004)
                else:
                    self.log.error(f"Unknown error decoding data from file", "CODECS", 1005)

    def decode_to_results(self, data: ByteIterable, include_skipped: bool = True, **kwargs) -> t.Iterable[DecodeResult]:
        idx = 0
        for result in self._decode(data, **kwargs):
            result.message_idx = idx
            idx += 1
            if include_skipped or not result.skipped:
                yield result

    def _decode(self,
                data: ByteIterable,
                **kwargs) -> t.Iterable[DecodeResult]:
        raise NotImplementedError()

    def _write_in_chunks(self, file_handle: Writable, output: ByteIterable):
        for bytes_ in HaltFlag.iterate(output, self._halt_flag, True):
            file_handle.write(bytes_)

    def _read_in_chunks(self, file_handle: Readable, chunk_size: int = 16384) -> ByteIterable:
        chunk = file_handle.read(chunk_size)
        while chunk != b'':
            if self._halt_flag:
                self._halt_flag.check_continue(True)
            yield chunk
            chunk = file_handle.read(chunk_size)
