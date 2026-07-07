from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs.base import BaseCodec, DecodeResult
from medsutil.byteseq import ByteSequenceReader
import medsutil.types as ct
import typing as t


class GtsSubDecoder:

    def get_message_type(self, reader: ByteSequenceReader, header: str) -> t.Hashable:
        raise NotImplementedError  # pragma: no coverage (default)

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool, received_date: AwareDateTime | None = None) -> DecodeResult:
        raise NotImplementedError  # pragma: no coverage (default)

    def supports_multiple_records(self) -> bool:
        raise NotImplementedError

    def encode_from_records(self, record: t.Iterable[ParentRecord], **kwargs) -> t.Iterable[bytes | bytearray]:
        raise NotImplementedError

    def encode_from_record(self, record: ParentRecord, **kwargs) -> t.Iterable[bytes | bytearray]:
        raise NotImplementedError

class GtsCodec(BaseCodec):

    WHITESPACE = bytes([13, 10, 32, 3, 4, 0])

    FILE_EXTENSION = ('.bufr',)

    def __init__(self, *args, **kwargs):
        from medsutil.ocproc2.codecs.wmo.bufr import Bufr4Decoder
        from medsutil.ocproc2.codecs.wmo.ascii import BuoyZZYY,TrackObNNXX, BathyJJVV, TesacKKYY, WaveObMMXX
        super().__init__(log_name="cnodc.codecs.gts", is_decoder=True, *args, **kwargs)
        self._sub_codecs: dict[bytes, GtsSubDecoder] = {
            b'BUFR': Bufr4Decoder(),
            b'ZZYY': BuoyZZYY(),
            b'JJVV': BathyJJVV(),
            b'KKYY': TesacKKYY()
            #b'NNXX': TrackObNNXX(),
            #b'MMXX': WaveObMMXX(),
        }
        self._skip_ascii = []

    def _decode_messages(self, data: ct.ByteStrings, options: dict) -> t.Iterable[DecodeResult]:
        reader = ByteSequenceReader(data)
        s = options.get('skip_to_message_idx', None)
        rdate = options.get("received_date", None)
        skip_to = int(s) if s is not None else None
        for current_idx, header in self._find_headers(reader):
            x = self._attempt_decode_next_gts_message(reader, header, skip_to is not None and (skip_to > current_idx), received_date=rdate)
            if x is not None:
                yield x

    def _find_headers(self, reader: ByteSequenceReader) -> t.Iterable[tuple[int, str]]:
        current_idx = 0
        header = ''
        reader.lstrip(GtsCodec.WHITESPACE)
        while not reader.at_eof():
            test_line = reader.peek_line(True).decode('ascii', 'replace')
            if self._is_gts_header(test_line):
                header = reader.consume_line(True).decode('ascii')
            reader.lstrip(GtsCodec.WHITESPACE)
            yield current_idx, header
            current_idx += 1
            reader.lstrip(GtsCodec.WHITESPACE)

    def report_message_structures(self, data: bytes) -> dict[t.Hashable, int]:
        reader = ByteSequenceReader([data])
        type_info: dict[t.Hashable, int] = {}
        for _, header in self._find_headers(reader):
            msg_type_info = self._attempt_decode_next_gts_message_type(reader, header)
            if msg_type_info not in type_info:
                type_info[msg_type_info] = 0
            type_info[msg_type_info] += 1
        return type_info


    def _attempt_decode_next_gts_message_type(self, reader: ByteSequenceReader, header: str) -> t.Hashable:
        message_type = reader.peek(5)
        for key in self._sub_codecs:
            if message_type.startswith(key):
                return self._sub_codecs[key].get_message_type(reader, header)
        discard_line = reader.consume_line(True)
        # Exclude some common issues
        if len(discard_line) > 4 and discard_line[0:4] == b'****':
            ...
        else:
            self.log.debug(f"Discarding line {discard_line.decode('ascii', 'replace')}, unrecognized start sequence")
            print(discard_line.decode('ascii'))
        return None

    def _attempt_decode_next_gts_message(self, reader: ByteSequenceReader, header: str, skip_decode: bool = False, received_date: AwareDateTime | None = None) -> t.Optional[DecodeResult]:
        message_type = reader.peek(5)
        for key in self._sub_codecs:
            if message_type.startswith(key):
                return self._sub_codecs[key].decode_from_bytes(reader, header, skip_decode, received_date)
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
