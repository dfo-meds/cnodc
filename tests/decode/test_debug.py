from cnodc.ocproc2 import ParentRecord
from cnodc.ocproc2.codecs import OCProc2DebugCodec
from cnodc.ocproc2.codecs.base import BaseCodec, EncodeResult, ByteData, DecodeResult, ByteIterable
from cnodc.util import CNODCError
from decode.helpers import CodecTestCase


class BadCodec(BaseCodec):

    def __init__(self):
        super().__init__('test', True, True)

    def encode_single_record(self, **kwargs):
        raise ValueError

    def decode_single_record(self, data: ByteData, **kwargs) -> ParentRecord:
        raise ValueError

    def parse_into_record_bytes(self, data: ByteIterable, **kwargs) -> ByteIterable:
        yield from data

class BadCodec2(BaseCodec):

    def __init__(self):
        super().__init__('test', True, True)

    def _encode_record(self, record: ParentRecord, **kwargs) -> EncodeResult:
        return EncodeResult(original=None)

    def _decode_record(self, record_data, **kwargs):
        return DecodeResult(records=None)

    def parse_into_record_bytes(self, data: ByteIterable, **kwargs) -> ByteIterable:
        yield from data




class TestBoringCodecCommonStuff(CodecTestCase):

    def test_encode_error(self):
        codec = BadCodec()
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.encode_records(self._build_standard_records(), fail_on_error=True)]

    def test_encode_error_no_fail(self):
        codec = BadCodec()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.encode_records(self._build_standard_records(), fail_on_error=False)]

    def test_encode_error_no_fail_none(self):
        codec = BadCodec2()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.encode_records(self._build_standard_records(), fail_on_error=False)]

    def test_encode_error_none_result(self):
        codec = BadCodec2()
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.encode_records(self._build_standard_records(), fail_on_error=True)]

    def test_decode_error(self):
        codec = BadCodec()
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_records([b"12345"], fail_on_error=True)]

    def test_decode_error_no_fail(self):
        codec = BadCodec()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.decode_records([b"12345"], fail_on_error=False)]

    def test_decode_error_None_no_fail(self):
        codec = BadCodec2()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.decode_records([b"12345"], fail_on_error=False)]

    def test_decode_error_none_result(self):
        codec = BadCodec2()
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_records([b"12345"], fail_on_error=True)]

class TestDebugFormat(CodecTestCase):

    def test_file_type(self):
        self.assertFalse(OCProc2DebugCodec.check_file_type('test.yaml'))
        self.assertFalse(OCProc2DebugCodec.check_file_type('test.yml'))
        self.assertFalse(OCProc2DebugCodec.check_file_type('test.json'))

    def test_record_dump(self):
        file = self.temp_dir / 'test.txt'
        records = self._build_standard_records()
        codec = OCProc2DebugCodec()
        codec.dump(file, records)
        self.assertTrue(file.exists())

    def test_record_dump_handle(self):
        file = self.temp_dir / 'test.txt'
        records = self._build_standard_records()
        codec = OCProc2DebugCodec()
        with open(file, 'wb') as h:
            codec.dump(h, records)
        self.assertTrue(file.exists())
