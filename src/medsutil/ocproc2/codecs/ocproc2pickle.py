from .base import BaseCodec
from medsutil.types import ByteStrings
import typing as t

import medsutil.ocproc2 as ocproc2
from medsutil.vlq import vlq_encode
import pickle


class OCProc2PickleCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.pickle',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.pickle", is_encoder=True, is_decoder=True, **kwargs)

    def _encode_single_record(self, record: ocproc2.ParentRecord, options: dict) -> ByteStrings:
        yield pickle.dumps(record.to_mapping())

    def _encode_record_data_for_file(self, record_data: ByteStrings, options: dict):
        ba = bytearray()
        for bytes_ in record_data:
            ba.extend(bytes_)
        yield vlq_encode(len(ba))
        yield ba

    def _parse_into_messages(self, data: ByteStrings, options: dict) -> ByteStrings:
        stream = self._as_byte_sequence(data)
        while not stream.at_eof():
            record_length = stream.consume_vlq_int()
            yield stream.consume(record_length)

    def _decode_single_message(self, data: t.Union[bytes, bytearray], options: dict) -> t.Iterable[ocproc2.ParentRecord]:
        yield ocproc2.ParentRecord.build_from_mapping(
            pickle.loads(data)
        )
