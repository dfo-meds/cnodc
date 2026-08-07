import gcapp.i18n.base as i18n
import typing as t
import tkinter as tk


class Popup:

    def __init__(self):
        self._window = None

    def show(self, master, x: int, y: int):
        if self._window is None:
            self._window = tk.Toplevel(master)
            self._window.wm_overrideredirect(True)
            self._window.wm_geometry(f"+{x}+{y}")
            self.body(self._window)

    def body(self, window):
        ...

    def hide(self):
        if self._window is not None:
            tw = self._window
            self._window = None
            tw.destroy()


class Tooltip(Popup):

    def __init__(self,
                 target,
                 text: t.Union[str, dict[str, str]]):
        super().__init__()
        target.bind('<Enter>', self._on_enter)
        target.bind('<Leave>', self._on_leave)
        self._text = text
        self._target = target
        self._window = None
        self._frame = None
        self._label = None

    def _on_enter(self, e):
        txt = self._get_text()
        if txt:
            self.show(self._target, self._target.winfo_rootx() + 2, self._target.winfo_rooty() + self._target.winfo_height() + 2)
        else:
            self.hide()

    def body(self, window):
        self._frame = tk.Frame(window, borderwidth=1, relief="solid")
        self._frame.pack(ipadx=1, ipady=1)
        self._label = tk.Label(self._frame, text=self._get_text())
        self._label.pack()

    def _on_leave(self, e):
        self.hide()

    def _get_text(self):
        if isinstance(self._text, str):
            return i18n.tr(self._text)
        else:
            return str(i18n.MLString(self._text))
