import json

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from cnodc.ocproc2 import DataRecord
from ..util import CNODCError
import pickle


class OCProc2PickleCodec(BaseCodec):

    JSON_WHITESPACE = b" \r\n\t"
    FILE_EXTENSION = ('.pickle',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.pickle", is_encoder=True, is_decoder=True, **kwargs)

    def _encode(self,
                record: DataRecord,
                **kwargs) -> t.Iterable[bytes]:
        data = pickle.dumps(record.to_mapping())
        data_len = len(data)
        if data_len > 0:
            print(data_len)
            buf = bytearray()
            while True:
                chunk = data_len & 0x7f
                data_len >>= 7
                if data_len:
                    buf.append(chunk + 128)
                else:
                    buf.append(chunk)
                    break
            yield buf
            yield data
        else:
            print('error nothing to pickle')

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
            content = stream.consume(record_length)
            try:
                yield DecodeResult(
                    records=[BaseCodec.map_to_record(pickle.loads(content))],
                    original=content
                )
            except Exception as ex:
                yield DecodeResult(
                    original=content,
                    exc=ex
                )
