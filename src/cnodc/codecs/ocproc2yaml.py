import yaml
from cnodc.codecs.base import BaseCodec, ByteIterable, DecodeResult
import typing as t
from cnodc.util import CNODCError
import cnodc.ocproc2 as ocproc2

try:
    from yaml import CSafeLoader as Loader, Dumper as Dumper
except:
    from yaml import SafeLoader as Loader, Dumper as Dumper


# TODO: there is an issue somewhere in the YAML codec that is leading to invalid
# outputs or issues with reading.

def _yaml_dump_dict(obj: dict, prefix: str = '') -> str:
    s = ''
    for x in obj.keys():
        s += f'{prefix}{x}: '
        if isinstance(obj[x], dict):
            s += '\n' + _yaml_dump_dict(obj[x], prefix + '  ')
        elif isinstance(obj[x], list) or isinstance(obj[x], tuple) or isinstance(obj[x], set):
            s += '\n' + _yaml_dump_list(obj[x], prefix + '  ')
        else:
            s += _yaml_dump_value(obj[x])
        if s[-1] != "\n":
            s += "\n"
    return s


def _yaml_dump_list(obj: t.Iterable, prefix: str = '') -> str:
    s = ''
    for item in obj:
        s += f'{prefix}- '
        if isinstance(item, dict):
            s += '\n' + _yaml_dump_dict(item, prefix + '  ')
        elif isinstance(item, list) or isinstance(item, tuple) or isinstance(item, set):
            s += '\n' + _yaml_dump_list(item, prefix + '  ')
        else:
            s += _yaml_dump_value(item)
        if s[-1] != "\n":
            s += "\n"
    return s


def _yaml_dump_value(obj) -> str:
    if obj is None:
        return '~'
    elif isinstance(obj, bool):
        return 'true' if obj else 'false'
    elif isinstance(obj, float) or isinstance(obj, int):
        return str(obj)
    val = str(obj).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('"', '\\u0022')
    return f"\"{val}\""


class OCProc2YamlCodec(BaseCodec):

    LINE_BREAKS = [b'\n', b'\r', b'\x85', b'\xE2\x80\xA8', b'\xE2\x80\xA9']
    DOCUMENT_BREAKS = [b'...', b'---']
    FILE_EXTENSION = ('yaml',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.yaml", is_encoder=True, is_decoder=True, support_single=True, **kwargs)

    def _encode_start(self, **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'%YAML 1.1\n---\n'

    def encode_single_record(self, record: ocproc2.ParentRecord, encoding='utf-8', **kwargs) -> t.Union[bytes, bytearray]:
        return yaml.dump(BaseCodec.record_to_map(record), Dumper=Dumper).encode(encoding or 'utf-8')

    def _encode_separator(self, encoding='utf-8', **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'\n...\n---\n'

    def _encode_end(self, encoding='utf-8', **kwargs) -> t.Union[None, bytes, bytearray]:
        return b'\n...\n'

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
                # TODO: if the ... message ending is omitted, then the decoding breaks
                # we can fix this by checking if the document ends with
                # ---(line breaks) and strip the extra characters.
                doc = yaml.safe_load(data.decode(encoding))
                if doc:
                    yield DecodeResult(
                        records=[BaseCodec.map_to_record(doc)],
                        original=data
                    )
            except Exception as ex:
                yield DecodeResult(
                    exc=ex,
                    original=data
                )

    def _decode_single_record(self, data: t.Union[bytes, bytearray], encoding='utf-8', **kwargs) -> t.Optional[ocproc2.ParentRecord]:
        doc = yaml.load(data.decode(encoding or 'utf-8'), Loader=Loader)
        if doc:
            return BaseCodec.map_to_record(doc)
        return None
