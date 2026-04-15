from medsutil.ocproc2.codecs.base import BaseCodec
from medsutil.types import ByteStrings
import typing as t
import medsutil.ocproc2 as ocproc2
import yaml


LINE_BREAKS = [b'\n', b'\r', b'\x85', b'\xE2\x80\xA8', b'\xE2\x80\xA9']
DOCUMENT_BREAKS = [b'...', b'---']


class OCProc2YamlCodec(BaseCodec):

    FILE_EXTENSION = ('yaml', 'yml')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.yaml", is_encoder=True, is_decoder=True, **kwargs)

    def _encode_start(self, options: dict) -> t.ByteString:
        return b'%YAML 1.1\n---\n'

    def _encode_single_record(self, record: ocproc2.ParentRecord, options) -> ByteStrings:
        yield yaml.dump(record.to_mapping()).encode(options.get('encoding', 'utf-8'))

    def _encode_separator(self, options: dict) -> t.ByteString:
        return b'\n...\n---\n'

    def _encode_end(self, options: dict) -> t.ByteString:
        return b'\n...\n'

    def _parse_into_messages(self, data: t.Iterable[t.ByteString], options: dict) -> t.Iterable[t.ByteString]:
        stream = self._as_byte_sequence(data)
        data: bytearray = bytearray()
        for line in stream.split_and_iterate(LINE_BREAKS, include_target=False):
            line = line.rstrip(b"\r\n\t ")
            if line in DOCUMENT_BREAKS:
                if data:
                    yield data
                    data = bytearray()
            elif line.startswith(b'%YAML'):
                continue
            elif line:
                data.extend(line + b"\n")
        if data:  # pragma: no coverage (fallback, not usually happening)
            yield data

    def _decode_single_message(self, data: t.Union[bytes, bytearray], options) -> t.Iterable[ocproc2.ParentRecord]:
        data_str = data.decode(options.get('encoding', 'utf-8'))
        yield ocproc2.ParentRecord.build_from_mapping(
            yaml.safe_load(data_str)
        )
