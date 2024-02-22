import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

import cnodc.ocproc2 as ocproc2
from ..util import CNODCError, vlq_encode
import pickle


class OCProc2PickleCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.pickle',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.pickle", is_encoder=True, is_decoder=True, support_single=True, **kwargs)

    def encode_single_record_for_decode(self, record: ocproc2.ParentRecord, **kwargs) -> t.Union[bytes, bytearray]:
        data = self.encode_single_record(record, **kwargs)
        yield vlq_encode(len(data))
        yield data

    def encode_single_record(self, record: ocproc2.ParentRecord, **kwargs) -> t.Union[bytes, bytearray]:
        return pickle.dumps(record.to_mapping())

    def _decode(self,
                data: ByteIterable,
                **kwargs) -> t.Iterable[DecodeResult]:
        stream = self._as_byte_sequence(data)
        while not stream.at_eof():
            record_length = 0
            shift = 0
            read_more = True
            while read_more:
                i = stream.consume(1)[0]
                if i > 127:
                    i -= 128
                else:
                    read_more = False
                record_length |= i << shift
                shift += 7
            yield self.decode_single_record(stream.consume(record_length), **kwargs)

    def _decode_single_record(self, data: t.Union[bytes, bytearray], **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        return BaseCodec.map_to_record(pickle.loads(data))
