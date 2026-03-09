import yaml
from cnodc.ocproc2.codecs.base import BaseCodec, ByteIterable
import typing as t
import cnodc.ocproc2 as ocproc2

try:
    from yaml import CSafeLoader as Loader, Dumper as Dumper
except: # pragma: no coverage (fall back for older Python)
    from yaml import SafeLoader as Loader, Dumper as Dumper


LINE_BREAKS = [b'\n', b'\r', b'\x85', b'\xE2\x80\xA8', b'\xE2\x80\xA9']
DOCUMENT_BREAKS = [b'...', b'---']


class OCProc2YamlCodec(BaseCodec):

    FILE_EXTENSION = ('yaml', 'yml')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.yaml", is_encoder=True, is_decoder=True, support_single=True, **kwargs)

    def _encode_start(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'%YAML 1.1\n---\n'

    def encode_single_record(self, record: ocproc2.ParentRecord, encoding='utf-8', **kwargs) -> ByteIterable:
        yield yaml.dump(BaseCodec.record_to_map(record), Dumper=Dumper).encode(encoding or 'utf-8')

    def _encode_separator(self, encoding='utf-8', **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'\n...\n---\n'

    def _encode_end(self, encoding='utf-8', **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'\n...\n'

    def parse_into_record_bytes(self, data: ByteIterable, **kwargs) -> ByteIterable:
        stream = self._as_byte_sequence(data)
        data = bytearray()
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

    def decode_single_record(self, data: t.Union[bytes, bytearray], encoding='utf-8', **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        data_str = data.decode(encoding or 'utf-8')
        doc = yaml.load(data_str, Loader=Loader)
        return BaseCodec.map_to_record(doc)
