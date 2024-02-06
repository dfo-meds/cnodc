import functools
import tkinter as tk
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation
import tkinter.ttk as ttk
import enum


class ButtonPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: list[ttk.Button] = []

    def on_init(self):
        self._buttons.append(ttk.Button(self.app.top_bar, text="S", width=2, command=self._save, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="SC", width=2, command=self._save_and_close, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="SN", width=2, command=self._save_and_next, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="SR", width=2, command=self._save_and_release, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="SF", width=2, command=self._save_and_fail, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="C", width=2, command=self._then_close, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="R", width=2, command=self._then_release, state=tk.DISABLED))
        self._buttons.append(ttk.Button(self.app.top_bar, text="F", width=2, command=self._then_fail, state=tk.DISABLED))
        for idx, button in enumerate(self._buttons):
            button.grid(row=0, column=idx, ipadx=2, ipady=2)

    def after_open_batch(self, batch_type: str):
        self.enable_all()

    def before_close_batch(self, op: QCBatchCloseOperation, batch_type: str, load_next: bool, ex=None):
        self.disable_all()

    def disable_all(self):
        for button in self._buttons:
            button.configure(state=tk.DISABLED)

    def enable_all(self):
        for button in self._buttons:
            button.configure(state=tk.NORMAL)

    def _save(self, after_save=None):
        self.disable_all()
        self.app.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.save_work',
            on_success=after_save if after_save is not None else self._on_save_success,
            on_error=self._on_save_error
        )

    def _on_save_success(self, res: bool):
        if not res:
            print('shouldnt happen')
        self.enable_all()

    def _on_save_error(self, ex):
        self.app.show_user_exception(ex)
        self.enable_all()

    def _save_and_close(self):
        self.disable_all()
        self._save(self._then_close)

    def _save_and_next(self):
        self.disable_all()
        self._save(functools.partial(self._then_close, load_next=True))

    def _save_and_release(self):
        self.disable_all()
        self._save(self._then_release)

    def _save_and_fail(self):
        self.disable_all()
        self._save(self._then_fail)

    def _then_close(self, res: bool, load_next: bool = False):
        if res:
            self.app.close_current_batch(QCBatchCloseOperation.COMPLETE, load_next)
        else:
            print('shouldnt happen')

    def _then_release(self, res: bool = True):
        if res:
            self.app.close_current_batch(QCBatchCloseOperation.RELEASE)
        else:
            print('shouldnt happen')

    def _then_fail(self, res: bool = True):
        if res:
            self.app.close_current_batch(QCBatchCloseOperation.FAIL)
        else:
            print('shouldnt happen')



