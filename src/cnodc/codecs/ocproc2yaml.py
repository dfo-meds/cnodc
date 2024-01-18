import json

import yaml

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from ..util import HaltInterrupt, CNODCError
from cnodc.ocproc2 import DataRecord


class OCProc2YamlCodec(BaseCodec):

    LINE_BREAKS = [b'\n', b'\r', b'\x85', b'\xE2\x80\xA8', b'\xE2\x80\xA9']
    DOCUMENT_BREAKS = [b'...', b'---']
    FILE_EXTENSION = ('yaml',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.yaml", is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, **kwargs) -> ByteIterable:
        yield b'%YAML 1.1\n'

    def _encode(self,
                record: DataRecord,
                **kwargs) -> EncodeResult:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        yield '---\n'.encode(encoding)
        yield yaml.safe_dump(record.to_mapping()).encode(encoding)
        yield '\n...\n'.encode(encoding)

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        encoding = kwargs.pop('encoding') if 'encoding' in kwargs else 'utf-8'
        doc_breaks = []
        for line_break in OCProc2YamlCodec.LINE_BREAKS:
            for doc_break in OCProc2YamlCodec.DOCUMENT_BREAKS:
                ba = bytearray()
                ba.extend(line_break)
                ba.extend(doc_break)
                doc_breaks.append(bytes(ba))
        stream = self._as_byte_sequence(data)
        last_offset = None
        while not stream.at_eof():
            data = b''
            if last_offset is not None and last_offset == stream.offset():
                raise CNODCError(f"Stream decoding error, infinite loop detected", "OCPROC2YAML", 1000)
            try:
                last_offset = stream.offset()
                data = stream.consume_until(doc_breaks, True)
                doc = yaml.safe_load(data.decode(encoding))
                if doc:
                    dr = DataRecord()
                    dr.from_mapping(doc)
                    yield DecodeResult(
                        records=[dr],
                        original=data
                    )
            except Exception as ex:
                yield DecodeResult(
                    exc=ex,
                    original=data
                )
