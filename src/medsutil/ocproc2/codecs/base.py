import pathlib
import typing as t
import os

import zrlog

from medsutil.ocproc2 import ParentRecord
from pipeman.exceptions import CNODCError, NotSupportedError
from medsutil.byteseq import ByteSequenceReader
from medsutil.halts import DummyHaltFlag, HaltFlag
import medsutil.types as ct


class DecodeResult:

    @t.overload
    def __init__(self, records: list[ParentRecord], message_idx: int = 0, original: t.ByteString | None = None, skipped: bool = False, single_message: bool = False): ...

    @t.overload
    def __init__(self, exc: Exception, message_idx: int = 0, original: t.ByteString | None = None, skipped: bool = False, single_message: bool = False): ...

    def __init__(self,
                 records: t.Optional[list[ParentRecord]] = None,
                 exc: t.Optional[Exception] = None,
                 message_idx: int = 0,
                 original: t.ByteString | None = None,
                 skipped: bool = False,
                 single_message: bool = False):
        self.records: t.Optional[list[ParentRecord]] = records
        self.success = exc is None and records is not None
        self.from_exception: t.Optional[Exception] = exc
        self.single_message: bool = single_message
        self.message_idx: int = message_idx
        self.original: t.ByteString | None = original
        self.skipped = skipped

    def __repr__(self):  # pragma: no coverage (debugging only)
        return f'<DecodeResult;{self.success};{len(self.records) if self.records else ''};{type(self.from_exception).__name__ if self.from_exception else ''}'


class EncodeResult:

    @t.overload
    def __init__(self, data_stream: ct.ByteStrings, original: ParentRecord | None = None): ...

    @t.overload
    def __init__(self, exc: Exception, original: ParentRecord | None = None): ...

    def __init__(self,
                 data_stream: ct.ByteStrings | None = None,
                 exc: Exception | None = None,
                 original: ParentRecord | None = None):
        self.data_stream: ct.ByteStrings | None = data_stream
        self.from_exception: Exception | None = exc
        self.original: ParentRecord | None = original
        self.success: bool = self.from_exception is None and self.data_stream is not None

    def __repr__(self):  # pragma: no coverage (debugging only)
        return f'<EncodeResult;{self.success};{'' if not self.from_exception else type(self.from_exception).__name__}'


class BaseCodec:

    def __init__(self,
                 log_name: str,
                 is_encoder: bool = False,
                 is_decoder: bool = False,
                 force_single_mode: bool = False,
                 halt_flag: HaltFlag = None,
                 default_options: dict = None):
        self.is_encoder = is_encoder
        self.is_decoder = is_decoder
        self.force_single_mode = force_single_mode
        self._halt_flag = halt_flag or DummyHaltFlag()
        self.log = zrlog.get_logger(log_name)
        self._defaults = default_options or {}

    def _as_byte_sequence(self, bytes_: ct.ByteStrings) -> ByteSequenceReader:
        return ByteSequenceReader(bytes_, self._halt_flag)

    def _process_options(self, options: dict): ...

    def dump(self,
             output_file: ct.SupportsBinaryWrite | ct.PathLike,
             record_set: t.Iterable[ParentRecord],
             **kwargs):
        if ct.is_binary_writable(output_file):
            self._write_in_chunks(output_file, self.encode_records(record_set, **kwargs))
        else:
            with open(output_file, "wb") as h:
                self._write_in_chunks(h, self.encode_records(record_set, **kwargs))

    def encode_records(self,
                       data: t.Iterable[ParentRecord],
                       **kwargs) -> ct.ByteStrings:
        options = {x: self._defaults[x] for x in self._defaults}
        options.update(kwargs)
        self._process_options(options)
        return self._encode_records(data, options)

    def _encode_records(self, data: t.Iterable[ParentRecord], options: dict):
        on_first = True
        st = self._encode_start(options)
        sep = self._encode_separator(options)
        en = self._encode_end(options)
        if st:
            yield st
        for record_data in self._encode_record_wrapper(data, options):
            if sep and not on_first:
                yield sep
            yield from self._encode_record_data_for_file(record_data, options)
            on_first = False
        if en:
            yield en

    def _encode_record_data_for_file(self, record_data: ct.ByteStrings, options: dict) -> ct.ByteStrings:
        yield from record_data

    def _encode_record_wrapper(self, data: t.Iterable[ParentRecord], options: dict) -> t.Iterable[ct.ByteStrings]:
        fail_on_error = bool(options.pop('fail_on_error', False))
        for record_idx, record in enumerate(self._halt_flag.iterate(data)):
            result = self._encode_record(record, options)
            if result.success:
                if result.data_stream is not None:
                    yield result.data_stream
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error encoding data from file, record [{record_idx}]", "CODECS", 1000) from result.from_exception
                else:
                    raise CNODCError(f"Error encoding data from file, record [{record_idx}]", "CODECS", 1001)
            elif result.from_exception:
                self.log.error(
                    f"Error encoding data from file, record [{record_idx}]",
                    exc_info=(type(result.from_exception), result.from_exception, result.from_exception.__traceback__)
                )
            else:
                self.log.error(f"Error encoding data from file, record [{record_idx}]")

    def _encode_record(self,
                       record: ParentRecord,
                       options: dict) -> EncodeResult:
        try:
            return EncodeResult(
                original=record,
                data_stream=self._encode_single_record(record, options)
            )
        except Exception as ex:
            return EncodeResult(
                original=record,
                exc=ex
            )

    def _encode_single_record(self, record: ParentRecord, options: dict) -> ct.ByteStrings: raise NotSupportedError

    def _encode_start(self, options: dict) -> t.ByteString | None: ...

    def _encode_separator(self, options: dict) -> t.ByteString | None: ...

    def _encode_end(self, options: dict) -> t.ByteString | None: ...

    def load(self,
             file: t.Union[ct.SupportsBinaryRead, t.ByteString, ct.ByteStrings, ct.PathLike],
             chunk_size: int = None,
             **kwargs) -> t.Iterable[ParentRecord]:
        if ct.is_binary_readable(file):
            yield from self.decode_messages(
                self._read_in_chunks(file, chunk_size),
                **kwargs
            )
        elif isinstance(file, (bytes, bytearray, memoryview)):
            yield from self.decode_messages(
                BaseCodec._yield_bytes(file),
                **kwargs
            )
        elif isinstance(file, (str, os.PathLike, pathlib.Path)):
            with open(file, "rb") as h:
                yield from self.decode_messages(
                    self._read_in_chunks(h, chunk_size),
                    **kwargs
                )
        else:
            yield from self.decode_messages(file, **kwargs)

    def decode_messages(self,
                        data: ct.ByteStrings,
                        **kwargs) -> t.Generator[ParentRecord]:
        fail_on_error = bool(kwargs.pop('fail_on_error')) if 'fail_on_error' in kwargs else False
        for result in self._halt_flag.iterate(self.buffered_decode_messages(data, **kwargs)):
            if result.success:
                yield from result.records
            elif fail_on_error:
                if result.from_exception:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1002) from result.from_exception
                else:
                    raise CNODCError(f"Error decoding data from file", "CODECS", 1003)
            elif result.from_exception:
                self.log.error(f"Error decoding data from file",
                               exc_info=(type(result.from_exception), result.from_exception, result.from_exception.__traceback__))
            else:
                self.log.error(f"Error decoding data from file")

    def buffered_decode_messages(self, data: ct.ByteStrings, **kwargs) -> t.Iterable[DecodeResult]:
        options = {x: self._defaults[x] for x in self._defaults}
        options.update(kwargs)
        self._process_options(options)
        if self.force_single_mode:
            yield from self._decode_messages(data, options)
        else:
            decoded_records = self._decode_messages(data, options)
            first = next(decoded_records, None)
            second = next(decoded_records, None)
            if first is None:
                pass
            elif second is None:
                first.single_message = True
                yield first
            else:
                yield first
                yield second
                yield from decoded_records

    def _decode_messages(self,
                         data: ct.ByteStrings,
                         options: dict) -> t.Generator[DecodeResult]:
        if self.force_single_mode:
            result = self._decode_message(b''.join(data), options)
            result.message_idx = 0
            yield result
        else:
            idx = 0
            for message_data in self._parse_into_messages(data, options):
                result = self._decode_message(message_data, options)
                result.message_idx = idx
                idx += 1
                yield result

    def _decode_message(self, record_data: t.ByteString, options: dict):
        try:
            return DecodeResult(
                records=[x for x in self._decode_single_message(record_data, options)],
                original=record_data
            )
        except Exception as ex:
            return DecodeResult(
                exc=ex,
                original=record_data
            )

    def _parse_into_messages(self, data: ct.ByteStrings, options: dict) -> ct.ByteStrings:
        yield b''.join(data)

    def _decode_single_message(self, data: t.ByteString, options: dict) -> t.Iterable[ParentRecord]: raise NotSupportedError

    def _write_in_chunks(self, file_handle: ct.SupportsBinaryWrite, output: ct.ByteStrings):
        for bytes_ in self._halt_flag.iterate(output):
            file_handle.write(bytes_)

    def _read_in_chunks(self, file_handle: ct.SupportsBinaryRead, chunk_size: int = None) -> ct.ByteStrings:
        yield from self._halt_flag.read_all(file_handle, chunk_size)

    @classmethod
    def check_file_type(cls, file_path: str) -> bool:
        if hasattr(cls, 'FILE_EXTENSION'):
            return file_path.endswith(cls.FILE_EXTENSION)
        return False

    @staticmethod
    def _yield_bytes(b: t.ByteString) -> t.Iterable[t.ByteString]:
        yield b
