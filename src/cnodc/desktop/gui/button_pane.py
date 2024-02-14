import functools
import tkinter as tk
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange
import tkinter.ttk as ttk
import enum
import typing as t
import cnodc.desktop.translations as i18n
from cnodc.desktop.gui.choice_dialog import ask_choice


class ButtonPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: dict[str, ttk.Button] = {}
        self._button_frame: t.Optional[ttk.Frame] = None

    def on_init(self):
        button_frame = ttk.Frame(self.app.top_bar)
        button_frame.grid(row=0, column=0)
        # TODO: translate button text
        self._buttons['load_new'] = ttk.Button(button_frame, text="Load", command=self._next_item, state=tk.DISABLED)
        self._buttons['save'] = ttk.Button(button_frame, text="Save", command=self.app.save_changes, state=tk.DISABLED)
        self._buttons['load_next'] = ttk.Button(button_frame, text="Submit and Load", command=functools.partial(self._then_complete, load_next=True), state=tk.DISABLED)
        self._buttons['complete'] = ttk.Button(button_frame, text="Submit", command=self._then_complete, state=tk.DISABLED)
        self._buttons['release'] = ttk.Button(button_frame, text="Release", command=self._then_release, state=tk.DISABLED)
        self._buttons['fail'] = ttk.Button(button_frame, text="Report Error", command=self._then_fail, state=tk.DISABLED)
        self._buttons['escalate'] = ttk.Button(button_frame, text='Escalate', command=self._then_escalate, state=tk.DISABLED)
        self._buttons['descalate'] = ttk.Button(button_frame, text='De-escalate', command=self._then_descalate, state=tk.DISABLED)
        idx = 0
        for button in self._buttons.keys():
            self._buttons[button].grid(row=0, column=idx, ipadx=2, ipady=2)
            idx += 1
        self._label = ttk.Label(self.app.top_bar, text="", font=('', 18, 'bold'))
        self._label.grid(row=0, column=idx, ipadx=2, ipady=2, sticky='e')

    def _next_item(self):
        choice = ask_choice(self.app.root, self.app.app_state.service_choices())
        if choice is not None:
            self.app.open_qc_batch(choice)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.USER:
            self.set_button_state('load_new', app_state.can_open_new_queue_item())
        if change_type & DisplayChange.ACTION:
            self.set_button_state('save', app_state.is_batch_action_available('apply_working') and app_state.has_unsaved_changes)
        if change_type & (DisplayChange.OP_ONGOING | DisplayChange.BATCH):
            self.set_button_state('load_new', app_state.can_open_new_queue_item())
            self.set_button_state('save', app_state.is_batch_action_available('apply_working') and app_state.has_unsaved_changes)
            self.set_button_state('load_next', app_state.is_batch_action_available('complete'))
            self.set_button_state('complete', app_state.is_batch_action_available('complete'))
            self.set_button_state('release', app_state.is_batch_action_available('release'))
            self.set_button_state('fail', app_state.is_batch_action_available('fail'))
            self.set_button_state('escalate', app_state.is_batch_action_available('escalate'))
            self.set_button_state('descalate', app_state.is_batch_action_available('descalate'))
        if change_type & DisplayChange.RECORD:
            if app_state.record is not None:
                if app_state.record.metadata.has_value('WMOID'):
                    self._label.configure(text=f'WMO ID: {app_state.record.metadata.best_value("WMOID")}')
                else:
                    self._label.configure(text=app_state.record_uuid)

    def on_language_change(self, language: str):
        # TODO: button labels
        pass

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
