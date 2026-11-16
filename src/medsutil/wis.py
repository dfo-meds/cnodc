import enum
import typing as t

from medsutil.byteseq import ByteSequenceReader


class WISMessageType(enum.Enum):
    BUFR = 1
    GRIB = 2
    TAC = 3
    UNKNOWN = 0


class WISMessage:

    def __init__(self,
                 message_type: WISMessageType,
                 content: bytes,
                 wmo_type: str,
                 processing_center: str,
                 reporting_time: str,
                 optional_group: str | None = None):
        self.message_type = message_type
        self.content = content
        self.wmo_type = wmo_type
        self.processing_center = processing_center
        self.reporting_time = reporting_time
        self.optional_group = optional_group

    def basename(self) -> str:
        if self.optional_group:
            return "_".join((
                self.wmo_type,
                self.processing_center,
                self.reporting_time,
                self.optional_group
            ))
        else:
            return "_".join((
                self.wmo_type,
                self.processing_center,
                self.reporting_time
            ))

    def original_header(self) -> bytearray:
        ba = bytearray()
        ba.extend(self.wmo_type.encode('ascii'))
        ba.extend(b' ')
        ba.extend(self.processing_center.encode('ascii'))
        ba.extend(b' ')
        ba.extend(self.reporting_time.encode('ascii'))
        if self.optional_group:
            ba.extend(b' ')
            ba.extend(self.optional_group.encode('ascii'))
        return ba


class WISMessageParser:

    WHITESPACE = bytes([13, 10, 32, 3, 4, 0])

    def __init__(self, reader: ByteSequenceReader):
        self.reader = reader
        self._invalid_header: str | None = None
        self._invalid_buffer: list[bytes] = []

    def split_messages(self) -> t.Generator[WISMessage, None, None]:
        for header in self._find_headers():
            message = self._extract_message(header)
            if message is not None:
                yield message
        last_invalid = self._build_invalid_message()
        if last_invalid is not None:
            yield last_invalid

    def _extract_message(self, header: str) -> WISMessage | None:
        msg_type = self.reader.peek(4)
        if msg_type == b'BUFR':
            return self._extract_bufr_message(header)
        elif msg_type == b'GRIB':
            return self._extract_grib_message(header)
        elif msg_type[0] == msg_type[1] and msg_type[2] == msg_type[3]:
            return self._extract_tac_message(header)
        else:
            return self._extract_invalid_message(header)

    def _extract_bufr_message(self, header: str):
        data = self.reader.peek(7)
        length = int.from_bytes(data[4:], "big")
        return WISMessage(
            WISMessageType.BUFR,
            self.reader.consume(length),
            **self._extract_from_header(header)
        )

    def _extract_grib_message(self, header: str):
        data = self.reader.peek(16)
        length = int.from_bytes(data[8:], "big")
        return WISMessage(
            WISMessageType.GRIB,
            self.reader.consume(length),
            **self._extract_from_header(header)
        )

    def _extract_tac_message(self, header: str):
        body = self.reader.consume_until(b"=", include_target=True)
        return WISMessage(
            WISMessageType.TAC,
            body,
            **self._extract_from_header(header)
        )

    def _build_invalid_message(self):
        return_message = None
        if self._invalid_header is not None and self._invalid_buffer:
            return_message = WISMessage(
                WISMessageType.UNKNOWN,
                b''.join(self._invalid_buffer),
                **self._extract_from_header(self._invalid_header)
            )
            self._invalid_buffer.clear()
        return return_message

    def _extract_invalid_message(self, header: str):
        return_message = None
        if self._invalid_header and header != self._invalid_header:
            return_message = self._build_invalid_message()
        self._invalid_header = header
        self._invalid_buffer.append(self.reader.consume_line(False))
        return return_message

    def _extract_from_header(self, header: str) -> dict[str, t.Any]:
        pieces = header.split(" ")
        header_info = {
            "wmo_type": pieces[0],
            "processing_center": pieces[1],
            "reporting_time": pieces[2]
        }
        if len(pieces) > 3:
            header_info["optional_group"] = pieces[3]
        return header_info


    def _find_headers(self) -> t.Iterable[str]:
        header = ""
        self.reader.lstrip(self.WHITESPACE)
        while not self.reader.at_eof():
            test_line = self.reader.peek_line(True).decode("ascii", "replace")
            if self.is_gts_header(test_line):
                header = self.reader.consume_line(True).decode("ascii")
                self.reader.lstrip(self.WHITESPACE)
            yield header
            self.reader.lstrip(self.WHITESPACE)

    @staticmethod
    def is_gts_header(s: str) -> bool:
        s = s.strip(WISMessageParser.WHITESPACE.decode("ascii"))
        # Format is one of:
        # AAAA## AAAA ######
        # or
        # AAAA## AAAA ###### AAA
        # White space may be on either side
        len_s = len(s)
        if len_s not in (18, 22):
            return False
        if s[6] != " ":
            return False
        if s[11] != " ":
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
            if s[18] != " ":
                return False
            if not s[19:].isupper():
                return False
        return True
