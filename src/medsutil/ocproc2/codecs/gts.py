from medsutil.ocproc2.codecs.base import BaseCodec, DecodeResult
from medsutil.byteseq import ByteSequenceReader
from medsutil.types import ByteStrings
import typing as t


class GtsSubDecoder:

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool) -> DecodeResult:
        raise NotImplementedError  # pragma: no coverage (default)


class GtsCodec(BaseCodec):

    WHITESPACE = bytes([13, 10, 32, 3, 4, 0])

    FILE_EXTENSION = ('.bufr',)

    def __init__(self, *args, **kwargs):
        from medsutil.ocproc2.codecs.wmo.bufr import Bufr4Decoder
        from medsutil.ocproc2.codecs.wmo.ascii import DriftingBuoyZZYY,TrackObNNXX, BathyJJVV, TesacKKYY
        super().__init__(log_name="cnodc.codecs.gts", is_decoder=True, *args, **kwargs)
        self._sub_codecs: dict[bytes, GtsSubDecoder] = {
            b'BUFR': Bufr4Decoder(),
            b'ZZYY': DriftingBuoyZZYY(),
            b'NNXX': TrackObNNXX(),
            b'JJVV': BathyJJVV(),
            b'KKYY': TesacKKYY()
        }
        self._skip_ascii = []

    def _decode_messages(self, data: ByteStrings, options: dict) -> t.Iterable[DecodeResult]:
        reader = ByteSequenceReader(data)
        header = ''
        reader.lstrip(GtsCodec.WHITESPACE)
        current_idx = 0
        s = options.get('skip_to_message_idx', None)
        skip_to = int(s) if s is not None else None
        while not reader.at_eof():
            test_line = reader.peek_line(True).decode('ascii', 'replace')
            if self._is_gts_header(test_line):
                header = reader.consume_line(True).decode('ascii')
            reader.lstrip(GtsCodec.WHITESPACE)
            x = self._attempt_decode_next_gts_message(reader, header, skip_to is not None and (skip_to > current_idx))
            if x is not None:
                yield x
            current_idx += 1
            reader.lstrip(GtsCodec.WHITESPACE)

    def _attempt_decode_next_gts_message(self, reader: ByteSequenceReader, header: str, skip_decode: bool = False) -> t.Optional[DecodeResult]:
        message_type = reader.peek(5)
        for key in self._sub_codecs:
            if message_type.startswith(key):
                return self._sub_codecs[key].decode_from_bytes(reader, header, skip_decode)
        discard_line = reader.consume_line(True)
        self.log.debug(f"Discarding line {discard_line.decode('ascii', 'replace')}, unrecognized start sequence")
        return None

    def _is_gts_header(self, s: str) -> bool:
        s = s.strip(GtsCodec.WHITESPACE.decode('ascii'))
        # Format is one of:
        # AAAA## AAAA ######
        # or
        # AAAA## AAAA ###### AAA
        # White space may be on either side
        len_s = len(s)
        if len_s not in (18, 22):
            return False
        if s[6] != ' ':
            return False
        if s[11] != ' ':
            return False
        if not s[0:4].isupper():
            return False
        if not s[7:11].isupper():
            return False
        if not s[4:6].isdigit():
            return False
        if not s[12:18].isdigit():
            return False
        if len_s == 22:
            if s[18] != ' ':
                return False
            if not s[19:].isupper():
                return False
        return True
