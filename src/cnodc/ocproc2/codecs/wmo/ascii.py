from cnodc.ocproc2.codecs.gts import GtsSubDecoder
from cnodc.ocproc2.codecs.base import DecodeResult, ByteSequenceReader
from cnodc.util.exceptions import NotSupportedError


class AsciiDecoder(GtsSubDecoder):

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool) -> DecodeResult:
        body = reader.consume_until(b'=')
        original_data = header.encode('ascii') + b"\n" + body
        if skip_decode:
            return DecodeResult(skipped=True, original=original_data)
        try:
            return DecodeResult(
                records=self.decode_message(header, body.decode('ascii')),
                original=original_data
            )
        except Exception as ex:
            return DecodeResult(exc=ex, original=original_data)

    def decode_message(self, header: str, ascii_message: str):
        raise NotSupportedError


class DriftingBuoyZZYY(AsciiDecoder):
    pass

class TrackObNNXX(AsciiDecoder):
    pass

class BathyJJVV(AsciiDecoder):
    pass

class TesacKKYY(AsciiDecoder):
    pass