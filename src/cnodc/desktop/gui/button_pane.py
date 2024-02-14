import functools
import itertools
import tkinter as tk
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange
import tkinter.ttk as ttk
import enum
import typing as t
import cnodc.desktop.translations as i18n
from cnodc.desktop.gui.choice_dialog import ask_choice
import PIL.Image as Image
import PIL.ImageTk as ImageTk
import pathlib
import numpy as np

from cnodc.desktop.gui.tooltip import Tooltip


class ButtonPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: dict[str, ttk.Button] = {}
        self._button_frame: t.Optional[ttk.Frame] = None
        base_path = pathlib.Path(__file__).absolute().parent.parent / 'resources'
        self._images = {
            'load_new': self._build_button_image(base_path / 'load.png'),
            'save': self._build_button_image(base_path / 'save.png'),
            'release': self._build_button_image(base_path / 'release.png'),
            'report': self._build_button_image(base_path / 'report.png'),
            'submit': self._build_button_image(base_path / 'submit.png'),
            'load_next': self._build_button_image(base_path / 'submit_next.png'),
            'escalate': self._build_button_image(base_path / 'calate.png'),
            'descalate': self._build_button_image(base_path / 'calate.png', True),
        }
        self.tts = []

    def _build_button_image(self, path: pathlib.Path, rotate: bool = False, size: int = 30):
        image = Image.open(str(path)).convert('RGBA').resize((size, size))
        if rotate:
            image = image.rotate(180)
        data = np.array(image)
        red, green, blue, alpha = data.T
        alpha2 = np.reshape(alpha.T, (size * size))
        raw_values = []
        raw_values_disabled = []
        for x in range(0, len(alpha2)):
            raw_values.extend([0, 0, 0, min(alpha2[x] * 2, 255)])
            raw_values_disabled.extend([0, 0, 0, min(int(alpha2[x] / 2), 255)])
        image = np.array(raw_values, dtype='u1')
        image = np.resize(image, (size, size, 4))
        image_dis = np.array(raw_values_disabled, dtype='u1')
        image_dis = np.resize(image_dis, (size, size, 4))
        return ImageTk.PhotoImage(Image.fromarray(image)), ImageTk.PhotoImage(Image.fromarray(image_dis))

    def on_init(self):
        button_frame = ttk.Frame(self.app.top_bar)
        button_frame.grid(row=0, column=0)
        # TODO: translate button text
        self._buttons['load_new'] = ttk.Button(
            button_frame,
            #text="Load",
            image=[
                self._images['load_new'][0],
                'disabled',
                self._images['load_new'][1],
            ],
            command=self._next_item,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['load_new'], 'tooltip.load_new'))
        self._buttons['save'] = ttk.Button(
            button_frame,
            #text="Save",
            image=[
                self._images['save'][0],
                'disabled',
                self._images['save'][1]
            ],
            command=self.app.save_changes,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['save'], 'tooltip.save'))
        self.app.root.bind('<Control-s>', self._save_event)
        self._buttons['load_next'] = ttk.Button(
            button_frame,
            #text="Submit and Load",
            image=[
                self._images['load_next'][0],
                'disabled',
                self._images['load_next'][1]
            ],
            command=functools.partial(self._then_complete, load_next=True),
            state=tk.DISABLED
        )
        self.app.root.bind('<Control-n>', functools.partial(self._then_complete, load_next=True))
        self.tts.append(Tooltip(self._buttons['load_next'], 'tooltip.load_next'))
        self._buttons['complete'] = ttk.Button(
            button_frame,
            #text="Submit",
            image=[
                self._images['submit'][0],
                'disabled',
                self._images['submit'][1]
            ],
            command=self._then_complete,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['complete'], 'tooltip.complete'))
        self._buttons['release'] = ttk.Button(
            button_frame,
            #text="Release",
            image=[
                self._images['release'][0],
                'disabled',
                self._images['release'][1]
            ],
            command=self._then_release,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['release'], 'tooltip.release'))
        self._buttons['fail'] = ttk.Button(
            button_frame,
            #text="Report Error",
            image=[
                self._images['report'][0],
                'disabled',
                self._images['report'][1]
            ],
            command=self._then_fail,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['fail'], 'tooltip.fail'))
        self._buttons['escalate'] = ttk.Button(
            button_frame,
            #text='Escalate',
            image=[
                self._images['escalate'][0],
                'disabled',
                self._images['escalate'][1]
            ],
            command=self._then_escalate,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['escalate'], 'tooltip.escalate'))
        self._buttons['descalate'] = ttk.Button(
            button_frame,
            #text='De-escalate',
            image=[
                self._images['descalate'][0],
                'disabled',
                self._images['descalate'][1]
            ],
            command=self._then_descalate,
            state=tk.DISABLED
        )
        self.tts.append(Tooltip(self._buttons['descalate'], 'tooltip.descalate'))
        idx = 0
        for button in self._buttons.keys():
            self._buttons[button].grid(row=0, column=idx, ipadx=0, ipady=0, padx=0, pady=0)
            idx += 1
        self._label = ttk.Label(self.app.top_bar, text="", font=('', 18, 'bold'))
        self._label.grid(row=0, column=idx, ipadx=2, ipady=2, sticky='e')

    def _save_event(self, e):
        self.app.save_changes()

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
        if self.app.app_state.is_batch_action_available('complete'):
            self.app.close_current_batch(QCBatchCloseOperation.COMPLETE, load_next)

    def _then_release(self, res: bool = True):
        if self.app.app_state.is_batch_action_available('release'):
            self.app.close_current_batch(QCBatchCloseOperation.RELEASE)

    def _then_fail(self, res: bool = True):
        if self.app.app_state.is_batch_action_available('fail'):
            self.app.close_current_batch(QCBatchCloseOperation.FAIL)

    def _then_escalate(self, res: bool = True):
        if self.app.app_state.is_batch_action_available('escalate'):
            self.app.close_current_batch(QCBatchCloseOperation.ESCALATE)

    def _then_descalate(self, res: bool = True):
        if self.app.app_state.is_batch_action_available('descalate'):
            self.app.close_current_batch(QCBatchCloseOperation.DESCALATE)

