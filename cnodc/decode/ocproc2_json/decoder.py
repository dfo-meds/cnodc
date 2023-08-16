from cnodc.decode.common import BufferedBinaryReader, BaseCodec, TranscodingResult, ocproc2_from_dict
from cnodc.ocproc2 import RecordSet, DataRecord
import json
import typing as t


class OCProc2JsonCodec(BaseCodec):

    def __init__(self):
        super().__init__("Uncompressed JSON format for OCPROC2", ".json")
        self._encoding = 'utf-8'
        self._json_whitespace = b"\r\t\n "

    def encode(self, records: TranscodingResult, compact: bool = True, **kwargs) -> t.Iterable[bytes]:
        yield '['.encode(self._encoding)
        # A single recordset or data record is just dumped
        if isinstance(records, (RecordSet, DataRecord)):
            yield json.dumps(records.to_mapping(compact=compact)).encode(self._encoding)
        # Multiple record sets or data records
        else:
            first = True
            for record in records:
                if first:
                    first = False
                else:
                    yield ',\n'.encode(self._encoding)
                yield json.dumps(record.to_mapping(compact=compact)).encode(self._encoding)
        yield ']'.encode(self._encoding)

    def decode(self, data: t.Iterable[bytes], **kwargs) -> t.Iterable[DataRecord]:
        buffered_data = BufferedBinaryReader(data)
        buffered_data.lstrip(self._json_whitespace)
        if buffered_data[0] == 91:
            # [ character indicates we have a list of DataRecords, lets stream it
            yield from self._decode_streaming_data_records(buffered_data)
        elif buffered_data[0] == 123:
            # { character means we likely have a single RecordSet or DataRecord, lets just parse it
            yield self._decode_data_record(buffered_data.read_all())
        else:
            raise ValueError(f"Invalid start character: [{buffered_data[0]}]")

    def _decode_streaming_data_records(self, buffered_data: BufferedBinaryReader) -> t.Iterable[DataRecord]:
        start_idx = 0
        depth = 0
        for idx, _byte in enumerate(buffered_data):
            if depth == 1 and _byte in (44, 93):
                # start_idx will either be a [ or a , character, we don't need it
                subset = buffered_data.subset(start_idx + 1, idx)
                yield self._decode_data_record(subset.read_all())
                start_idx = idx
                buffered_data.discard_buffer(idx)
            if _byte in (91, 123):
                depth += 1
            elif _byte in (93, 125):
                depth -= 1

    def _decode_data_record(self, binary_data: t.Union[bytes, bytearray]) -> t.Iterable[DataRecord]:
        data = json.loads(binary_data.decode(self._encoding))
        return ocproc2_from_dict(data)
