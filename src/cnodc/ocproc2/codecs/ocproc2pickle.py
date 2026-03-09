import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

import cnodc.ocproc2 as ocproc2
from cnodc.util import CNODCError, vlq_encode
import pickle


class OCProc2PickleCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.pickle',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.pickle", is_encoder=True, is_decoder=True, support_single=True, **kwargs)

    def encode_single_record(self, record: ocproc2.ParentRecord, **kwargs) -> ByteIterable:
        yield pickle.dumps(record.to_mapping())

    def _encode_record_data_for_file(self, record_data: ByteIterable):
        ba = bytearray()
        for bytes_ in record_data:
            ba.extend(bytes_)
        yield vlq_encode(len(ba))
        yield ba

    def parse_into_record_bytes(self, data: ByteIterable, **kwargs) -> ByteIterable:
        stream = self._as_byte_sequence(data)
        while not stream.at_eof():
            record_length = stream.consume_vlq_int()
            yield stream.consume(record_length)

    def decode_single_record(self, data: t.Union[bytes, bytearray], **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        return BaseCodec.map_to_record(pickle.loads(data))
