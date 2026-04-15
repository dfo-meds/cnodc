from medsutil.ocproc2.codecs import OCProc2JsonCodec
from tests.helpers.decode_base import CodecTestCase


class TestOCProc2JsonFormat(CodecTestCase):

    def test_basic(self):
        codec = OCProc2JsonCodec()
        byte_iterable = codec.encode_records(self._build_standard_records())
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_load_all_from_file_handle(self):
        file = self.temp_dir / 'file.txt'
        codec = OCProc2JsonCodec()
        codec.dump(file, self._build_standard_records())
        with open(file, 'rb') as h:
            records = [x for x in codec.load(h)]
        self._verify_standard_records(records)

    def test_load_all_from_file_handle_no_mmap(self):
        file = self.temp_dir / 'file.txt'
        codec = OCProc2JsonCodec()
        codec.dump(file, self._build_standard_records())
        records = [x for x in codec.load(file, use_mmap=False)]
        self._verify_standard_records(records)

    def test_load_all_from_bytes(self):
        codec = OCProc2JsonCodec()
        data = bytearray()
        for bytes_ in codec.encode_records(self._build_standard_records()):
            data.extend(bytes_)
        self._verify_standard_records([x for x in codec.load(data)])

    def test_load_all_direct(self):
        codec = OCProc2JsonCodec()
        self._verify_standard_records([x for x in codec.load(codec.encode_records(self._build_standard_records()))])

    def test_decode_nothing(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  ', b'\r', b'\n', b'\t', b'    ']
        records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(0, len(records))

    def test_decode_one_record(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  {"_metadata": {"Foo": "Bar"}}']
        records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(1, len(records))
        self.assertEqual("Bar", records[0].metadata["Foo"].value)

    def test_decode_many_records(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  [{"_metadata": {"Foo": "Bar"}},{"_metadata": {"Foo": "Bar2"}}]']
        records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(2, len(records))
        self.assertEqual("Bar", records[0].metadata["Foo"].value)
        self.assertEqual("Bar2", records[1].metadata["Foo"].value)

    def test_decode_many_records_whitespace(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  [  {\t"_metadata"\r\n: \t\r{"Foo":\n    "Bar"}} \n  , \n   \t  {\t"_metadata"  :     \t {"Foo": \t    "Bar2"}   }   ]     ']
        records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(2, len(records))
        self.assertEqual("Bar", records[0].metadata["Foo"].value)
        self.assertEqual("Bar2", records[1].metadata["Foo"].value)

    def test_decode_bad_json(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  [{"_metadata": {"Foo": "Bar"}},{"_metadata": {"Foo": "Bar2"}}],[{"what?": "hello"}]']
        with self.assertLogs("cnodc.codecs.json", "WARNING"):
            records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(2, len(records))
        self.assertEqual("Bar", records[0].metadata["Foo"].value)
        self.assertEqual("Bar2", records[1].metadata["Foo"].value)

    def test_decode_bad_json_no_bracket(self):
        codec = OCProc2JsonCodec()
        byte_iterable = [b'  [{"_metadata": {"Foo": "Bar"}},{"_metadata": {"Foo": "Bar2"}}']
        with self.assertLogs("cnodc.codecs.json", "WARNING"):
            records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(2, len(records))
        self.assertEqual("Bar", records[0].metadata["Foo"].value)
        self.assertEqual("Bar2", records[1].metadata["Foo"].value)

    def test_json_extensions(self):
        self.assertTrue(OCProc2JsonCodec.check_file_type('test.json'))
        self.assertFalse(OCProc2JsonCodec.check_file_type('test.yaml'))

