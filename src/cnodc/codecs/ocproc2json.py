try:
    import orjson
    json_dumps = orjson.dumps
    json_loads = orjson.loads
    json_name = 'orjson'
except ModuleNotFoundError:
    import json
    json_name = 'json'

    def json_dumps(o):
        return json.dumps(o).encode('utf-8')

    def json_loads(s):
        return json.loads(s.decode('utf-8'))

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

import cnodc.ocproc2 as ocproc2
from ..util import CNODCError


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.json',)

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.codecs.json", support_single=True, is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'['

    def encode_single_record(self, record: ocproc2.ParentRecord, encoding='utf-8', **kwargs) -> t.Union[bytes, bytearray]:
        return json_dumps(BaseCodec.record_to_map(record))

    def _encode_separator(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b','

    def _encode_end(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b']'

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        stream = self._as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if stream.at_eof():
            return []
        elif stream[0] == b'[':
            yield from self._decode_streaming_records(stream, encoding)
        else:
            yield self.decode_single_record(stream.consume_all(), encoding)

    def _decode_streaming_records(self, stream: ByteSequenceReader, encoding: str) -> t.Iterable[DecodeResult]:
        # Skip the initial byte, its a square bracket
        depth = 0
        buffer = bytearray()
        for chunk in stream.split_and_iterate([b"[", b"]", b"{", b"}"], True):
            buffer.extend(chunk)
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
                    yield self.decode_single_record(buffer[1:], encoding=encoding)
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

    def _decode_single_record(self, stream: t.Union[bytes, bytearray], encoding: str = 'utf-8', *args, **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        return BaseCodec.map_to_record(json_loads(stream))
