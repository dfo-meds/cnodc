import cnodc.desktop.translations as i18n
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

    def _on_enter(self, e):
        if self._window is None:
            self._window = tk.Toplevel(self._target)
            self._window.wm_overrideredirect(True)
            x = self._target.winfo_rootx() + 2
            y = self._target.winfo_rooty() + self._target.winfo_height() + 2
            self._window.wm_geometry(f"+{x}+{y}")
            l = tk.Label(self._window, text=self._get_text())
            l.pack()

    def _get_text(self):
        if isinstance(self._text, str):
            return i18n.get_text(self._text)
        else:
            return i18n.get_text_from_dict(self._text)

    def _on_leave(self, e):
        if self._window is not None:
            tw = self._window
            self._window = None
            tw.destroy()
