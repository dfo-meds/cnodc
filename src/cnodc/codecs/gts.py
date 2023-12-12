from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader
import typing as t


class GtsSubDecoder:

    def decode_message(self, header: str, input_) -> DecodeResult:
        raise NotImplementedError()


class GtsCodec(BaseCodec):

    WHITESPACE = bytes([13, 10, 32, 3, 4, 0])

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.codecs.gts", is_decoder=True, *args, **kwargs)
        self._sub_codecs: dict[str, GtsSubDecoder] = {
            'BUFR4': Bufr4Decoder(),
            'ZZYY': DriftingBuoyZZYY(),
            'NNXX': TrackObNNXX(),
            'JJVV': BathyJJVV(),
            'KKYY': TesacKKYY()
        }
        self._skip_ascii = []

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        reader = ByteSequenceReader(data)
        header = ''
        reader.lstrip(GtsCodec.WHITESPACE)
        while not reader.at_eof():
            test_line = reader.peek_line(True).decode('ascii')
            if self._is_gts_header(test_line):
                header = reader.consume_line(True).decode('ascii')
            reader.lstrip(GtsCodec.WHITESPACE)
            yield self._attempt_decode_next_gts_message(reader, header)
            reader.lstrip(GtsCodec.WHITESPACE)

    def _attempt_decode_next_gts_message(self, reader: ByteSequenceReader, header: str) -> DecodeResult:
        message_type = reader.peek(5)
        if message_type.startswith(b'BUFR'):
            return self._decode_bufr(reader, header)
        elif message_type == b'KKYY ':
            return self._decode_basic_ascii('KKYY', reader, header)
        elif message_type == b'JJVV ':
            return self._decode_basic_ascii('JJVV', reader, header)
        elif message_type == b'NNXX ':
            return self._decode_basic_ascii('NNXX', reader, header)
        elif message_type == b'ZZYY ':
            return self._decode_basic_ascii('ZZYY', reader, header)
        else:
            discard_line = reader.consume_line(True)
            self.log.debug(f"Discarding line {discard_line.decode('ascii', 'replace')}, unrecognized start sequence")

    def _decode_bufr(self, reader: ByteSequenceReader, header: str) -> DecodeResult:
        reader.consume(4)
        message_length = int.from_bytes(reader.consume(3), 'big')
        bufr_version = int(reader.consume(1)[0])
        content = bytearray()
        content.extend(b'BUFR')
        content.extend(message_length.to_bytes(3, 'big'))
        content.extend(bufr_version.to_bytes(1, 'big'))
        content.extend(reader.consume(message_length - 8))
        if bufr_version == 4:
            return self._sub_codecs['BUFR4'].decode_message(header, content)
        else:
            return DecodeResult(
                exc=Exception(f'Invalid BUFR version [{bufr_version}]'),
                original=content
            )

    def _decode_basic_ascii(self, message_type: str, reader: ByteSequenceReader, header: str) -> DecodeResult:
        body = reader.consume_until(b'=')
        return self._sub_codecs[message_type].decode_message(header, body)

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
        if s[6] != '':
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


class Bufr4Decoder(GtsSubDecoder):

    def __init__(self):
        pass

    def decode_message(self, header: str, bufr_message: bytearray) -> DecodeResult:
        original = header.encode('ascii') + b"\n" + bufr_message
        return DecodeResult(exc=Exception("not supported yet"), original=original)


class DriftingBuoyZZYY(GtsSubDecoder):

    def __init__(self):
        pass

    def decode_message(self, header: str, ascii_message: bytearray) -> DecodeResult:
        original = header.encode('ascii') + b'\n' + ascii_message
        return DecodeResult(exc=Exception("not supported yet"), original=original)


class TrackObNNXX(GtsSubDecoder):

    def __init__(self):
        pass

    def decode_message(self, header: str, ascii_message: bytearray) -> DecodeResult:
        original = header.encode('ascii') + b'\n' + ascii_message
        return DecodeResult(exc=Exception("not supported yet"), original=original)


class BathyJJVV(GtsSubDecoder):

    def __init__(self):
        pass

    def decode_message(self, header: str, ascii_message: bytearray) -> DecodeResult:
        original = header.encode('ascii') + b'\n' + ascii_message
        return DecodeResult(exc=Exception("not supported yet"), original=original)


class TesacKKYY(GtsSubDecoder):

    def __init__(self):
        pass

    def decode_message(self, header: str, ascii_message: bytearray) -> DecodeResult:
        original = header.encode('ascii') + b'\n' + ascii_message
        return DecodeResult(exc=Exception("not supported yet"), original=original)