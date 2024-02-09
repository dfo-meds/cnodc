import functools
import tkinter as tk
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange
import tkinter.ttk as ttk
import enum


class ButtonPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: dict[str, ttk.Button] = {}

    def on_init(self):
        self._buttons['save'] = ttk.Button(self.app.top_bar, text="Save", command=self.app.save_changes, state=tk.DISABLED)
        self._buttons['load_next'] = ttk.Button(self.app.top_bar, text="Submit and Load", command=functools.partial(self._then_complete, load_next=True), state=tk.DISABLED)
        self._buttons['complete'] = ttk.Button(self.app.top_bar, text="Submit", command=self._then_complete, state=tk.DISABLED)
        self._buttons['release'] = ttk.Button(self.app.top_bar, text="Release", command=self._then_release, state=tk.DISABLED)
        self._buttons['fail'] = ttk.Button(self.app.top_bar, text="Report Error", command=self._then_fail, state=tk.DISABLED)
        self._buttons['escalate'] = ttk.Button(self.app.top_bar, text='Escalate', command=self._then_escalate, state=tk.DISABLED)
        self._buttons['descalate'] = ttk.Button(self.app.top_bar, text='De-escalate', command=self._then_descalate, state=tk.DISABLED)
        for idx, button in enumerate(self._buttons.keys()):
            self._buttons[button].grid(row=0, column=idx, ipadx=2, ipady=2)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.OP_ONGOING | DisplayChange.BATCH):
            self.set_button_state('save', app_state.is_batch_action_available('apply_working'))
            self.set_button_state('load_next', app_state.is_batch_action_available('complete'))
            self.set_button_state('complete', app_state.is_batch_action_available('complete'))
            self.set_button_state('release', app_state.is_batch_action_available('release'))
            self.set_button_state('fail', app_state.is_batch_action_available('fail'))
            self.set_button_state('escalate', app_state.is_batch_action_available('escalate'))
            self.set_button_state('descalate', app_state.is_batch_action_available('descalate'))

    def set_button_state(self, key: str, is_enabled: bool):
        self._buttons[key].configure(state=(tk.NORMAL if is_enabled else tk.DISABLED))

    def _then_complete(self, res: bool = True, load_next: bool = False):
        self.app.close_current_batch(QCBatchCloseOperation.COMPLETE, load_next)

    def _then_release(self, res: bool = True):
        self.app.close_current_batch(QCBatchCloseOperation.RELEASE)

    def _then_fail(self, res: bool = True):
        self.app.close_current_batch(QCBatchCloseOperation.FAIL)

    def _then_escalate(self, res: bool = True):
        self.app.close_current_batch(QCBatchCloseOperation.ESCALATE)

    def _then_descalate(self, res: bool = True):
        self.app.close_current_batch(QCBatchCloseOperation.DESCALATE)
