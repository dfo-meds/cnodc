from osdt.common import CodecProtocol, BaseCodec
from osdt.cds import DataRecord, TranscodingResult, RecordSet
import typing as t
import pathlib
from osdt.common import BufferedBinaryReader


class CodeFormHandler:

    def __init__(self, form_code: str):
        self._form_code = form_code + " "

    def can_handle_content(self, line_1: str) -> bool:
        return line_1.startswith(self._form_code)

    def handle_content(self, header: str, lines: list[str], start_index: int) -> tuple[int, t.Iterable[DataRecord]]:
        raise NotImplementedError()


class GTSAsciiStreamCodec(BaseCodec):

    def __init__(self):
        super().__init__("GTS Message Stream", ".gts")
        self._message_whitespace = bytes([13, 10, 32, 3, 4])
        self._registry: dict[str, CodeFormHandler] = {
            "fm18xii": FM18XII_BUOY(),
            "fm62vii": FM62VIII_TRACKOB(),
            "fm63xi": FM63XI_BATHY(),
            "fm64x": FM64X_TESAC()
        }

    def register_code_form(self, form_name: str, code_form: CodeFormHandler):
        self._registry[form_name] = code_form

    def encode(self, records: TranscodingResult, **kwargs) -> t.Iterable[bytes]:
        pass

    def decode(self, bytes_: t.Iterable[bytes], **kwargs) -> TranscodingResult:
        reader = BufferedBinaryReader(bytes_)
        header = None
        content = []
        for line in reader.consume_by_lines():
            line = line.strip(self._message_whitespace)._decode('ascii')
            if line == "":
                continue
            if self._is_gts_header(line):
                if content:
                    yield from self._decode_message(header, content)
                    content = []
                header = line
            else:
                content.append(line)
        if content:
            yield from self._decode_message(header, content)

    def _is_gts_header(self, line: str):
        line_len = len(line)
        if line_len == 18 or line_len == 22:
            if line[6] != " ":
                return False
            if line[11] != " ":
                return False
            if line_len == 22:
                if line[18] != " ":
                    return False
            if not line[4:6].isdigit():
                return False
            if not line[12:18].isdigit():
                return False
            hr = int(line[14:16])
            if hr > 23:
                return False
            mn = int(line[16:18])
            if mn > 59:
                return False
            dom = int(line[12:14])
            if dom > 31:
                return False
            return True
        return False

    def _decode_message(self, header: str, content: list[str]):
        idx = 0
        l_content = len(content)
        while idx < l_content:
            for rname in self._registry:
                if self._registry[rname].can_handle_content(content[idx]):
                    idx, records = self._registry[rname].handle_content(header, content, idx)
                    yield from records
                    break
            else:
                print(f"No way to handle line {content[idx]}, skipping")
                idx += 1


class FM13XIV_SHIP(CodeFormHandler):

    def __init__(self):
        super().__init__("BBXX")

    def handle_content(self, header: str, lines: list[str], start_idx: int) -> tuple[int, t.Iterable[DataRecord]]:
        for i in range(start_idx, len(lines)):
            if lines[i].endswith("="):
                return i + 1, []


class FM18XII_BUOY(CodeFormHandler):

    def __init__(self):
        super().__init__("ZZYY")

    def handle_content(self, header: str, lines: list[str], start_idx: int) -> tuple[int, t.Iterable[DataRecord]]:
        return start_idx + 1, []


class FM62VIII_TRACKOB(CodeFormHandler):

    def __init__(self):
        super().__init__("NNXX")

    def handle_content(self, header: str, lines: list[str], start_idx: int) -> tuple[int, t.Iterable[DataRecord]]:
        return start_idx + 1, []


class FM63XI_BATHY(CodeFormHandler):

    def __init__(self):
        super().__init__("JJVV")

    def handle_content(self, header: str, lines: list[str], start_idx: int) -> tuple[int, t.Iterable[DataRecord]]:
        for i in range(start_idx, len(lines)):
            if lines[i].endswith("="):
                return i + 1, []


class FM64X_TESAC(CodeFormHandler):

    def __init__(self):
        super().__init__("KKYY")

    def handle_content(self, header: str, lines: list[str], start_idx: int) -> tuple[int, t.Iterable[DataRecord]]:
        for i in range(start_idx, len(lines)):
            if lines[i].endswith("="):
                return i + 1, []


