import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from cnodc.ocproc2 import DataRecord
from ..util import CNODCError


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.json", is_encoder=True, is_decoder=True, **kwargs)

    def encode_start(self, **kwargs) -> ByteIterable:
        yield b'['

    def _encode(self,
                record: DataRecord,
                **kwargs) -> EncodeResult:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        try:
            return EncodeResult(
                data_stream=[json.dumps(record.to_mapping()).encode(encoding)],
                original=record
            )
        except Exception as ex:
            return EncodeResult(
                exc=ex,
                original=record
            )

    def encode_separator(self, **kwargs) -> ByteIterable:
        yield b','

    def encode_end(self, **kwargs) -> ByteIterable:
        yield b']'

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        stream = self.as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if stream.at_eof():
            return []
        if stream[0] == 91:
            yield from self._decode_streaming_records(stream, encoding)
        else:
            yield self._decode_single_message(stream, encoding)

    def _decode_single_message(self, stream: ByteSequenceReader, encoding: str) -> DecodeResult:
        return self._decode_message(stream.consume_all(), encoding)

    def _decode_streaming_records(self, stream: ByteSequenceReader, encoding: str) -> t.Iterable[DecodeResult]:
        # Skip the initial byte, its a square bracket
        depth = 0
        buffer = bytearray()
        last_offset = None
        while not stream.at_eof():
            if last_offset is not None and last_offset == stream.offset():
                raise CNODCError(f"Stream reading error detected, infinite loop", "OCPROC2JSON", 1000)
            last_offset = stream.offset()
            buffer.extend(stream.consume_until([b"[", b"]", b"{", b"}"], True))
            end_c = buffer[-1]
            if end_c in (b"[", b"{"):
                depth += 1
            elif end_c == b"]":
                depth -= 1
                if depth == 0:
                    break
            elif end_c == b"}":
                depth -= 1
                if depth == 1:
                    # First character is either a comma or a square bracket that isn't closed, so finish it
                    yield self._decode_message(buffer[1:], encoding)
                    buffer = bytearray()
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        # TODO: handle this better
        if not stream.at_eof():
            print("warning, end of file not reached")

    def _decode_message(self, stream: t.Union[bytes, bytearray], encoding: str):
        try:
            data = stream.decode(encoding)
            dr = DataRecord()
            dr.from_mapping(json.loads(data))
            return DecodeResult(
                records=[dr],
                original=stream
            )
        except Exception as ex:
            return DecodeResult(
                exc=ex,
                original=stream
            )
