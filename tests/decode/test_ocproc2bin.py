from json import JSONDecodeError

from cnodc.ocproc2.codecs.ocproc2bin import OCProc2BinCodec
from cnodc.util import CNODCError
from decode.helpers import CodecTestCase


class TestOCProc2BinaryFormat(CodecTestCase):

    def test_basic_encode_decode(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records())
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_lzma(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMA")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_lzma_crc4(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMACRC4")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_lzma_crc8(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMACRC8")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_lzma_crc8_custom_level(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMA2CRC4")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_zlib(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="ZLIB")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_zlib_custom_level(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="ZLIB8")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bz2(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="BZ2")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bz2_custom_level(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), compression="BZ26")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_rs32(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), correction="RS32")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_lzma_level(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMA10")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_lzma_level_letter(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="LZMAA")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_zlib_level(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="ZLIB10")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_zlib_level_letter(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="ZLIBA")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_bz2_level(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="BZ210")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_bz2_level_letter(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="BZ2A")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_compression(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), compression="GZIP")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_correction(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), correction="BZ2")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_json_format(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), codec="JSON")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_yaml_format(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), codec="YAML")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_pickle_format(self):
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records(self._build_standard_records(), codec="PICKLE")
        self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_codec(self):
        codec = OCProc2BinCodec()
        with self.assertRaises(CNODCError):
            byte_iterable = codec.encode_records(self._build_standard_records(), codec="STUFF")
            self._verify_standard_records([x for x in codec.decode_messages(byte_iterable)])

    def test_bad_decode_content_too_short(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x00']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_header_too_short(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x05', b'\x00', b'A', b'B', b'C']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_header_not_ascii(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x05', b'\x00', b'A', b'B', b'C', b'D', b'\x81']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_no_commas_in_header(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x05', b'\x00', b'A', b'B', b'C', b'D', b' ']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_one_comma_in_header(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x05', b'\x00', b'A', b',', b'C', b'D', b' ']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_three_comma_in_header(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x05', b'\x00', b'A', b',', b',', b',', b',']
        with self.assertRaises(CNODCError):
            _ = [x for x in codec.decode_messages(byte_iterable)]

    def test_bad_decode_json_gibberish(self):
        codec = OCProc2BinCodec()
        byte_iterable = [b'\x06', b'\x00', b'JSON', b',', b',', b'nonsense']
        with self.assertLogs('cnodc.codecs.json', level='ERROR'):
            decode = [x for x in codec.decode_to_results(byte_iterable)]
        self.assertEqual(1, len(decode))
        self.assertFalse(decode[0].success)
        self.assertIsInstance(decode[0].from_exception, JSONDecodeError)










