import tkinter as tk
from cnodc.desktop.gui.base_pane import BasePane
import tkinter.ttk as ttk


class ButtonPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: list[ttk.Button] = []

    def on_init(self):
        self._buttons.append(ttk.Button(self.app.top_bar, text="S", width=2, command=self._save, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="N", width=2, command=self._save_and_next, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="D", width=2, command=self._discard, state=tk.DISABLED))
        for idx, button in enumerate(self._buttons):
            button.grid(row=0, column=idx, ipadx=2, ipady=2)

    def on_open_qc_batch(self, batch_type: str):
        for button in self._buttons:
            button.configure(state=tk.NORMAL)

    def on_close_qc_batch(self, batch_type: str, load_next: bool):
        for button in self._buttons:
            button.configure(state=tk.DISABLED)

    def _save(self):
        self.app.close_current_batch(False)

    def _save_and_next(self):
        self.app.close_current_batch(True)

    def _discard(self):
        pass




