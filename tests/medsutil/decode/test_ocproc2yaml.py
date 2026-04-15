from medsutil.ocproc2.codecs import OCProc2YamlCodec
from tests.helpers.decode_base import CodecTestCase


class TestOCProc2YamlFormat(CodecTestCase):

    def test_file_type(self):
        self.assertTrue(OCProc2YamlCodec.check_file_type('test.yaml'))
        self.assertTrue(OCProc2YamlCodec.check_file_type('test.yml'))
        self.assertFalse(OCProc2YamlCodec.check_file_type('test.json'))

    def test_encode_decode(self):
        codec = OCProc2YamlCodec()
        self._verify_standard_records(
            [x for x in codec.load(codec.encode_records(self._build_standard_records()))]
        )