import gcapp.i18n.base as i18n
import typing as t
import tkinter as tk


class Tooltip:

    def __init__(self,
                 target,
                 text: t.Union[str, dict[str, str]]):
        target.bind('<Enter>', self._on_enter)
        target.bind('<Leave>', self._on_leave)
        self._text = text
        self._target = target
        self._window = None
        self._frame = None
        self._label = None

    def _on_enter(self, e):
        txt = self._get_text()
        if self._window is None and txt:
            self._window = tk.Toplevel(self._target)
            self._window.wm_overrideredirect(True)
            x = self._target.winfo_rootx() + 2
            y = self._target.winfo_rooty() + self._target.winfo_height() + 2
            self._window.wm_geometry(f"+{x}+{y}")
            self._frame = tk.Frame(self._window, borderwidth=1, relief="solid")
            self._frame.pack(ipadx=1, ipady=1)
            self._label = tk.Label(self._frame, text=txt)
            self._label.pack()
        else:
            self._on_leave(None)

    def _get_text(self):
        if isinstance(self._text, str):
            return i18n.tr(self._text)
        else:
            return str(i18n.MLString(self._text))

    def _on_leave(self, e):
        if self._window is not None:
            tw = self._window
            self._window = None
            tw.destroy()
