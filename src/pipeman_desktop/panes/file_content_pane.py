import pathlib
from unittest import case

from gcapp import i18n
from pipeman_desktop.panes.base_pane import BasePane
import tkinter as tk
import tkinter.ttk as ttk
import typing as t

from pipeman_desktop.state import ApplicationState, DisplayChange

CONTROL_DISPLAY_START = 9216

def _ascii_display(c: int) -> str:
    # unicode display codes for control characters (except newline, which we will use)
    if 0 <= c <= 31 and c != 10:
        return chr(CONTROL_DISPLAY_START + c)
    if c == 127:
        return "\u2421"

    # normal ascii
    if 32 <= c <= 126:
        return chr(c)

    # unknown
    return "\uFFFD"

def convert_to_hex(b: bytes) -> str:
    return b.hex(" ")

def convert_to_ascii(b: bytes) -> str:
    return "".join(_ascii_display(x) for x in b)

def convert_to_utf8(b: bytes) -> str:
    return b.decode("utf-8", errors="replace")


class FileContentPane(BasePane):

    OUTPUT_MODES: dict[str, t.Callable[[bytes], str]] = {
        "output.hex": convert_to_hex,
        "output.ascii": convert_to_ascii,
        "output.utf8": convert_to_utf8,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._frame: ttk.Frame | None = None
        self._output_mode: ttk.Combobox | None = None
        self._output_view: tk.Text | None = None
        self._options_map: dict[str, t.Callable[[bytes], str]] = {}
        self._options_text_var = tk.StringVar()
        self._options_text_var.trace("w", self._load_content)
        self._current_file_path: str | None = None
        self._current_output_mode: t.Callable[[bytes], str] | None = None

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
                self._output_view.configure()

    def _update_text(self, file_path: pathlib.Path, output_mode: t.Callable[[bytes], str]):
        with open(file_path, "rb") as h:
            self._output_view.delete("1.0", tk.END)
            self._output_view.insert(tk.END, output_mode(h.read()))

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.LANGUAGE:
            self._update_output_mode_options()
        if change_type & DisplayChange.BATCH_STATE:
            self._load_content()
