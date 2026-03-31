import typing as t

from cnodc.ocproc2.codecs.base import BaseCodec, ByteIterable, ByteSequenceReader
import cnodc.ocproc2 as ocproc2
import cnodc.util.json as json


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ".json"

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.codecs.json", support_single=True, is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'['

    def encode_single_record(self, record: ocproc2.ParentRecord, encoding='utf-8', **kwargs) -> ByteIterable:
        yield json.dump_bytes(BaseCodec.record_to_map(record))

    def _encode_separator(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b','

    def _encode_end(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b']'

    def parse_into_record_bytes(self, data: ByteIterable, **kwargs) -> ByteIterable:
        stream = self._as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if stream.at_eof():
            return []
        elif stream[0] == b'[':
            yield from self._decode_streaming_records(stream)
        else:
            yield stream.consume_all()

    def _decode_streaming_records(self, stream: ByteSequenceReader) -> ByteIterable:
        # Skip the initial byte, its a square bracket
        depth = 0
        buffer = bytearray()
        for chunk in stream.split_and_iterate([b"[", b"]", b"{", b"}"], True):
            buffer.extend(chunk)
            end_c = buffer[-1]
            if end_c in (91, 123):  # [ or {
                depth += 1
            elif end_c == 93:  # ]
                depth -= 1
                if depth == 0:
                    break
            elif end_c == 125: # }
                depth -= 1
                if depth == 1:
                    buffer = buffer.strip()
                    # First character is either a comma or a square bracket that isn't closed, so finish it
                    yield buffer[1:]
                    buffer = bytearray()
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        buffer = buffer.strip()
        # TODO: handle this better
        if not stream.at_eof():
            self.log.warning(f"More data detected")
        if buffer == b'':
            self.log.warning(f"Missing trailing bracket")
        elif buffer != b']':  # pragma: no coverage (fallback for malformed JSON)
            self.log.warning(f"More data detected")

    def decode_single_record(self, stream: t.Union[bytes, bytearray], *args, **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        return BaseCodec.map_to_record(json.load_dict(stream))

