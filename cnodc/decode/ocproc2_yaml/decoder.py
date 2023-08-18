from cnodc.decode.common import BufferedBinaryReader, BaseCodec, TranscodingResult, ocproc2_from_dict, DecodedMessage
from cnodc.ocproc2 import RecordSet, DataRecord
import yaml
import typing as t

# Use lib yaml for faster processing, if available
try:
    from yaml import CSafeLoader as SafeLoader, CDumper as Dumper
except ImportError:
    from yaml import SafeLoader, Dumper


class OCProc2YamlCodec(BaseCodec):

    def __init__(self):
        super().__init__("Uncompressed YAML format for OCPROC2", ".yaml")
        self._encoding = 'utf-8'
        self._line_breaks = [b'\n', b'\r', b'\x85', b'\xE2\x80\xA8', b'\xE2\x80\xA9']
        self._document_breaks = [b'...', b'---']
        self._document_break_checks = []
        for line_break in self._line_breaks:
            for document_break in self._document_breaks:
                ba = bytearray()
                ba.extend(line_break)
                ba.extend(document_break)
                self._document_break_checks.append(bytes(ba))

    def encode(self, records: TranscodingResult, compact: bool = True, **kwargs) -> t.Iterable[bytes]:
        # A single recordset or data record is just dumped
        yield '%YAML 1.1\n'.encode(self._encoding)
        if isinstance(records, (RecordSet, DataRecord)):
            yield "---\n".encode(self._encoding)
            yield yaml.safe_dump(records.to_mapping(compact=compact)).encode(self._encoding)
            yield "\n...\n".encode(self._encoding)
        # Multiple record sets or data records
        else:
            for record in records:
                yield "---\n".encode(self._encoding)
                yield yaml.safe_dump(record.to_mapping(compact=compact)).encode(self._encoding)
                yield "\n...\n".encode(self._encoding)

    def decode_messages(self, data: t.Iterable[bytes], replace_logger_cls: t.Type = None, **kwargs) -> t.Iterable[DecodedMessage]:
        buffered_data = BufferedBinaryReader(data)
        message_idx = 0
        while not buffered_data.is_at_end():
            stream = buffered_data.consume_until(self._document_break_checks, True)
            data = stream.decode('utf-8')
            doc = yaml.load(data, SafeLoader)
            if doc:
                if replace_logger_cls:
                    self.logger = replace_logger_cls()
                yield DecodedMessage(message_idx, stream, self.logger, ocproc2_from_dict(doc))



