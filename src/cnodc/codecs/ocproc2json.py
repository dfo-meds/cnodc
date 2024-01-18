import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from cnodc.ocproc2 import DataRecord
from ..util import CNODCError


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.json',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.json", is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, **kwargs) -> ByteIterable:
        yield b'['

    def _encode(self,
                record: DataRecord,
                **kwargs) -> t.Iterable[bytes]:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        yield json.dumps(BaseCodec.record_to_map(record)).encode(encoding)

    def _encode_separator(self, **kwargs) -> ByteIterable:
        yield b','

    def _encode_end(self, **kwargs) -> ByteIterable:
        yield b']'

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        stream = self._as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if stream.at_eof():
            return []
        elif stream[0] == b'[':
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
            if end_c in (91, 123):
                depth += 1
            elif end_c == 93:
                depth -= 1
                if depth == 0:
                    break
            elif end_c == 125:
                depth -= 1
                if depth == 1:
                    # First character is either a comma or a square bracket that isn't closed, so finish it
                    yield self._decode_message(buffer[1:], encoding)
                    buffer = bytearray()
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        # TODO: handle this better
        if not stream.at_eof():
            print("warning, end of file not reached")
        if buffer == b'':
            print("warning, missing trailing bracket")
        elif buffer != b']':
            print("warning, buffer not empty")
            print(buffer)

    def _decode_message(self, stream: t.Union[bytes, bytearray], encoding: str):
        try:
            data = stream.decode(encoding)
            return DecodeResult(
                records=[BaseCodec.map_to_record(json.loads(data))],
                original=stream
            )
        except Exception as ex:
            return DecodeResult(
                exc=ex,
                original=stream
            )
