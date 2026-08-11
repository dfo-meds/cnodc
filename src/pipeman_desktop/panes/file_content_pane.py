import pathlib
from unittest import case

from gcapp import i18n
from pipeman_desktop.panes.base_pane import BasePane
import tkinter as tk
import tkinter.ttk as ttk
import typing as t

from pipeman_desktop.state import ApplicationState, DisplayChange


class FileContentCodec(t.Protocol):
    def decode(self, b: bytes) -> str:
        ...

    def encode(self, b: str) -> bytes:
        ...

class HexConverter:

    def decode(self, b: bytes) -> str:
        return b.hex(" ")

    def encode(self, b: str) -> bytes:
        return bytes.fromhex(b.replace(" ", "").replace("\n", "").replace("\r", "").replace("\t", ""))


class AsciiConverter:

    CONTROL_DISPLAY_START = 9216
    CONTROL_DELETE = 9249

    def _ascii_decode(self, c: int) -> str:
        # unicode display codes for control characters (except newline, which we will use)
        if 0 <= c <= 31 and c != 10:
            return chr(self.CONTROL_DISPLAY_START + c)
        if c == 127:
            return "\u2421"

        # normal ascii
        if 32 <= c <= 126:
            return chr(c)

        # unknown
        return "\uFFFD"

    def _ascii_encode(self, c: int) -> int | None:
        if self.CONTROL_DISPLAY_START <= c <= (self.CONTROL_DISPLAY_START + 32):
            return c - self.CONTROL_DISPLAY_START
        if self.CONTROL_DELETE == c:
            return 127
        if 32 <= c <= 126:
            return c
        return None

    def decode(self, b: bytes) -> str:
        return "".join(self._ascii_decode(x) for x in b)

    def encode(self, b: str) -> bytes:
        raise NotImplementedError


class Utf8Converter:

    def decode(self, b: bytes) -> str:
        return b.decode("utf-8", errors="replace")

    def encode(self, b: str) -> bytes:
        return b.encode("utf-8", errors="ignore")


class FileContentPane(BasePane):

    OUTPUT_MODES: dict[str, FileContentCodec] = {
        "output.hex": HexConverter,
        "output.ascii": AsciiConverter,
        "output.utf8": Utf8Converter,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._frame: ttk.Frame | None = None
        self._output_mode: ttk.Combobox | None = None
        self._output_view: tk.Text | None = None
        self._options_map: dict[str, FileContentCodec] = {}
        self._options_text_var = tk.StringVar()
        self._options_text_var.trace("w", self._load_content)
        self._current_file_path: str | None = None
        self._current_output_mode: FileContentCodec | None = None

    def on_init(self):
        self._frame = ttk.Frame(self.app.decode_qc_mode)
        self._frame.grid(row=0, column=0, sticky=tk.NSEW)
        self._frame.grid_columnconfigure(0, weight=1)
        self._frame.grid_rowconfigure(0, weight=1)
        self._frame.grid_rowconfigure(1, weight=0)
        self._output_view = tk.Text(self._frame, wrap="word", state="disabled")
        self._output_view.grid(row=0, column=0, sticky=tk.NSEW)
        self._output_mode = ttk.Combobox(self._frame, values=[], textvariable=self._options_text_var)
        self._output_mode.grid(row=1, column=0)
        self._update_output_mode_options()

    def _update_output_mode_options(self):
        current = self._options_text_var.get()
        if current in self._options_map:
            current_func = self._options_map[current]
        else:
            current_func = None
        self._options_map = {
            i18n.tr(x): y
            for x, y in self.OUTPUT_MODES.items()
        }
        values = sorted(self._options_map.keys())
        if self._output_mode is not None:
            self._output_mode.config(values=values)
        for x, y in self._options_map.items():
            if y is current_func:
                self._options_text_var.set(x)
                break
        else:
            self._options_text_var.set(values[0])

    def _load_content(self):
        if self._output_view is not None:
            if self.app.state.qc_mode == "decode" and self.app.state.qc_file_path:
                output_mode = self._options_map[self._options_text_var.get()]
                if self.app.state.qc_file_path != self._current_file_path or self._current_output_mode != output_mode:
                    self._update_text(
                        self.app.state.qc_file_path,
                        output_mode
                    )
                    self._current_output_mode = output_mode
                    self._current_file_path = self.app.state.qc_file_path
            else:
                self._current_file_path = None
                self._clear_text()

    def _update_text(self, file_path: pathlib.Path, output_mode: FileContentCodec):
        if self._output_view is not None:
            with open(file_path, "rb") as h:
                self._clear_text()
                self._output_view.insert(tk.END, output_mode.decode(h.read()))

    def _clear_text(self):
        if self._output_view is not None:
            self._output_view.delete("1.0", tk.END)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.LANGUAGE:
            self._update_output_mode_options()
        if change_type & DisplayChange.BATCH_STATE:
            self._load_content()
