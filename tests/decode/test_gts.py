import unittest as ut

from cnodc.ocproc2 import ParentRecord
from cnodc.ocproc2.codecs import GtsCodec
from cnodc.ocproc2.codecs.wmo.ascii import AsciiDecoder


class DummySubcodec(AsciiDecoder):

    def __init__(self):
        super().__init__()
        self.messages = 0

    def decode_message(self, header: str, ascii_message: str):
        self.messages += 1
        yield ParentRecord()


class TestGtsCodec(ut.TestCase):

    def test_dummy_decode_with_garbage(self):
        data = b'\x04 BAD HEADER\n\x04IOSC01 RJTD 170700\nAAAA message=\nIOSC01 RJTD 170100 AAA\n\nAAAA message2='
        codec = GtsCodec()
        codec._sub_codecs[b'AAAA'] = DummySubcodec()
        records = [x for x in codec.decode_records([data])]
        self.assertEqual(2, len(records))
        self.assertEqual(2, codec._sub_codecs[b'AAAA'].messages)

    def test_gts_header(self):
        good_headers = [
            'IOSC01 RJTD 170700',
            'IOSC01 RJTD 170700 AAA',
            ' IOSC01 RJTD 170700',
            '\rIOSC01 RJTD 170700',
            '\nIOSC01 RJTD 170700',
            '\x00IOSC01 RJTD 170700',
            '\x03IOSC01 RJTD 170700',
            '\x04IOSC01 RJTD 170700',
        ]
        bad_headers = [
            '',
            'foobar'
            '\tIOSC01 RJTD 170700',
            'IOSC01 RJTD  170700',
            'IOSC01RJTD  170700',
            'IOSC01  RJTD170700',
            'aAAA01 AAAA 000000',
            'AaAA01 AAAA 000000',
            'AAaA01 AAAA 000000',
            'AAAa01 AAAA 000000',
            'AAAA01 aAAA 000000',
            'AAAA01 AaAA 000000',
            'AAAA01 AAaA 000000',
            'AAAA01 AAAa 000000',
            'AAAAB0 AAAA 000000',
            'AAAA0B AAAA 000000',
            'AAAA00 AAAA A00000',
            'AAAA00 AAAA 0A0000',
            'AAAA00 AAAA 00A000',
            'AAAA00 AAAA 000A00',
            'AAAA00 AAAA 0000A0',
            'AAAA00 AAAA 00000A',
            'AAAA00 AAAA 000000AAAA',
            'AAAA00 AAAA 000000 aAA',
            'AAAA00 AAAA 000000 AaA',
            'AAAA00 AAAA 000000 AAa',
        ]
        codec = GtsCodec()
        for x in good_headers:
            with self.subTest(good_header=x):
                self.assertTrue(codec._is_gts_header(x))
        for x in bad_headers:
            with self.subTest(bad_header=x):
                self.assertFalse(codec._is_gts_header(x))