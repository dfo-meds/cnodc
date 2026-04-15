from medsutil.ocproc2.codecs import OCProc2PickleCodec
from tests.helpers.decode_base import CodecTestCase


class TestOCProc2PickleFormat(CodecTestCase):

    def test_encode_decode(self):
        codec = OCProc2PickleCodec()
        self._verify_standard_records(
            [x for x in codec.load(codec.encode_records(self._build_standard_records()))]
        )
