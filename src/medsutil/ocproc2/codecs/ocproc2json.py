import typing as t

from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs.base import BaseCodec
from medsutil.byteseq import ByteSequenceReader
from medsutil.ocproc2.util import ParentExport
from medsutil.types import ByteStrings
import medsutil.ocproc2 as ocproc2
import medsutil.json as json


class OCProc2JsonCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ".json"

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.codecs.json", is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, options: dict) -> t.Union[None, bytes, bytearray]:
        return b'['

    def _encode_single_record(self, record: ocproc2.ParentRecord, options: dict) -> ByteStrings:
        yield json.dumpb(record.to_mapping())

    def _encode_separator(self, options: dict) -> t.Union[None, bytes, bytearray]:
        return b','

    def _encode_end(self, options: dict) -> t.Union[None, bytes, bytearray]:
        return b']'

    def _parse_into_messages(self, data: ByteStrings, options: dict) -> ByteStrings:
        stream = self._as_byte_sequence(data)
        stream.lstrip(OCProc2JsonCodec.JSON_WHITESPACE)
        if not stream.at_eof():
            if stream[0] == b'[':
                yield from self._decode_streaming_records(stream)
            else:
                yield stream.consume_all()

    def _decode_streaming_records(self, stream: ByteSequenceReader) -> ByteStrings:
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

    def _decode_single_message(self, stream: t.ByteString, options: dict) -> t.Iterable[ocproc2.ParentRecord]:
        if isinstance(stream, memoryview):
            yield ParentRecord.build_from_mapping(t.cast(ParentExport, t.cast(object, json.load_dict(bytes(stream)))))
        else:
            yield ParentRecord.build_from_mapping(t.cast(ParentExport, t.cast(object, json.load_dict(stream))))

