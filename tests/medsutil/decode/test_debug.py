from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs import OCProc2DebugCodec
from medsutil.ocproc2.codecs.base import BaseCodec, EncodeResult, DecodeResult
from medsutil import types as ct
from medsutil.exceptions import CodedError
from tests.helpers.decode_base import CodecTestCase
import typing as t


class BadCodec(BaseCodec):

    def __init__(self):
        super().__init__('test', True, True)

    def _encode_single_record(self, record: ParentRecord, options: dict) -> ct.ByteStrings:
        raise ValueError

    def _decode_single_message(self, data: t.ByteString, options: dict) -> ParentRecord:
        raise ValueError

class BadCodec2(BaseCodec):

    def __init__(self):
        super().__init__('test', True, True)

    def _encode_record(self, record: ParentRecord, options) -> EncodeResult:
        return EncodeResult(data_stream=None, original=None)

    def _decode_record(self, record_data, options: dict):
        return DecodeResult(records=None)





class TestBoringCodecCommonStuff(CodecTestCase):

    def test_encode_error(self):
        codec = BadCodec()
        with self.assertRaises(CodedError):
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
        with self.assertRaises(CodedError):
            _ = [x for x in codec.encode_records(self._build_standard_records(), fail_on_error=True)]

    def test_decode_error(self):
        codec = BadCodec()
        with self.assertRaises(CodedError):
            _ = [x for x in codec.decode_messages([b"12345"], fail_on_error=True)]

    def test_decode_error_no_fail(self):
        codec = BadCodec()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.decode_messages([b"12345"], fail_on_error=False)]

    def test_decode_error_None_no_fail(self):
        codec = BadCodec2()
        with self.assertLogs("test", level="ERROR"):
            _ = [x for x in codec.decode_messages([b"12345"], fail_on_error=False)]

    def test_decode_error_none_result(self):
        codec = BadCodec2()
        with self.assertRaises(CodedError):
            _ = [x for x in codec.decode_messages([b"12345"], fail_on_error=True)]

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
