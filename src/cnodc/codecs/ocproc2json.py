import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from ..util import HaltInterrupt
from cnodc.ocproc2 import DataRecord


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"

    def encode_start(self, **kwargs) -> ByteIterable:
        yield b'['

    def encode(self,
               record: DataRecord,
               **kwargs) -> EncodeResult:
        try:
            data = json.dumps(record.to_mapping()).encode('utf-8')
            return EncodeResult.from_bytes(data)
        except (KeyboardInterrupt, HaltInterrupt) as ex:
            raise ex
        except Exception as ex:
            return EncodeResult.from_exception(ex)

    def encode_separator(self, **kwargs) -> ByteIterable:
        yield b','

    def encode_end(self, **kwargs) -> ByteIterable:
        yield b']'

    def decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        stream = self.as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if stream.at_eof():
            return []
        if stream[0] == 91:
            yield from self._decode_streaming_records(stream)
        else:
            yield self._decode_single_message(stream)

    def _decode_single_message(self, stream: ByteSequenceReader) -> DecodeResult:
        return self._decode_message(stream.consume_all())

    def _decode_streaming_records(self, stream: ByteSequenceReader) -> t.Iterable[DecodeResult]:
        # Skip the initial byte, its a square bracket
        depth = 0
        buffer = bytearray()
        while not stream.at_eof():
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
                    yield self._decode_message(buffer[1:])
                    buffer = bytearray()
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        # TODO: handle this better
        if not stream.at_eof():
            print("warning, end of file not reached")

    def _decode_message(self, stream: t.Union[bytes, bytearray]):
        try:
            data = stream.decode('utf-8')
            return DecodeResult.from_record_list(self._build_data_records(data))
        except (HaltInterrupt, KeyboardInterrupt) as ex:
            raise ex
        except Exception as ex:
            return DecodeResult.from_exception(ex)

    def _build_data_records(self, data: str) -> list[DataRecord]:
        dr = DataRecord()
        dr.from_mapping(json.loads(data))
        return [dr]
