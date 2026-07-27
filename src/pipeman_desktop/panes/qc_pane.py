import functools
import tkinter as tk
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.util import ReviewResult
from pipeman_desktop.state import DisplayChange, ApplicationState
import tkinter.ttk as ttk
import typing as t
from pipeman_desktop.components.choice_dialog import ask_choice
from pipeman_desktop.components.tooltip import Tooltip
import PIL.Image as Image
import PIL.ImageTk as ImageTk
import pathlib
import numpy as np




class QCPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buttons: dict[str, ttk.Button] = {}
        self._button_close_state: dict[str, ReviewResult] = {}
        self._button_frame: t.Optional[ttk.Frame] = None
        self._base_path = pathlib.Path(__file__).absolute().parent.parent / 'resources'
        self._images = {}
        self._last_choice = None
        self._load_next = tk.IntVar()
        self._checkbox: ttk.Checkbutton | None = None
        self._label: ttk.Label | None = None
        self.tts = []

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.USER:
            self.set_button_state('load_new', app_state.can_open_qc_batch())
        if change_type & DisplayChange.ACTION:
            self.set_button_state('save', app_state.can_save_changes())
        if change_type & (DisplayChange.SAVING | DisplayChange.BATCH_STATE):
            self.set_button_state('load_new', app_state.can_open_qc_batch())
            self.set_button_state('save', app_state.can_save_changes())
            for bn, close_op in self._button_close_state.items():
                self.set_button_state(bn, app_state.can_close_current_batch(close_op))
        if change_type & DisplayChange.RECORD:
            if app_state.record is not None:
                if app_state.record.metadata.has_value('WMOID'):
                    self._label.configure(text=f'WMO ID: {app_state.record.metadata.best("WMOID")}')
                else:
                    self._label.configure(text=app_state.record_uuid or '')

    def set_button_state(self, key: str, is_enabled: bool):
        self._buttons[key].configure(state=(tk.NORMAL if is_enabled else tk.DISABLED))

    def on_language_change(self):
        # TODO: button labels
        pass

    def on_init(self):
        button_frame = ttk.Frame(self.app.top_bar)
        button_frame.grid(row=0, column=0)

        self._build_button(button_frame, "load_new", "load.png", self.next_item)

        self._build_button(button_frame, "save", "save.png", self.save)
        self.app.root.bind('<Control-s>', self.save)

        self._build_button(button_frame, "release", "release.png", self.release_item)

        self._build_button(button_frame, "report", "report.png", self.fail_item)

        self._build_button(button_frame, "submit", "submit.png", self.complete_item)
        self.app.root.bind('<Control-n>', functools.partial(self.complete_item))

        self._build_button(button_frame, "recheck", "recheck.png", self.recheck_item)

        self._build_button(button_frame, "escalate", "calate.png", self.escalate_item)

        self._build_button(button_frame, "descalate", "calate.png", self.descalate_item, rotate=True)

        self._checkbox = ttk.Checkbutton(self.app.top_bar, variable=self._load_next)
        self._checkbox.grid(row=0, column=len(self._buttons), ipadx=2, ipady=2, sticky='w')
        self.tts.append(Tooltip(self._checkbox, f'tooltip_toggle_autoload'))

        self._label = ttk.Label(self.app.top_bar, text="", font=('', 18, 'bold'))
        self._label.grid(row=0, column=len(self._buttons) + 1, ipadx=2, ipady=2, sticky='e')

        self.fetch_queue_ready_count()

    def _build_button(self,
                      button_frame: ttk.Frame,
                      button_name: str,
                      file_name: str,
                      command: t.Callable,
                      close_state: ReviewResult | None = None,
                      rotate: bool = False):
        self._images[button_name] = self._build_button_image(
            self._base_path / file_name,
            self.app.top_bar.master,
            rotate
        )
        self._buttons[button_name] = ttk.Button(
            button_frame,
            image=[self._images[button_name][0], 'disabled', self._images[button_name][1]],
            command=command,
            state=tk.DISABLED
        )
        if close_state is not None:
            self._button_close_state[button_name] = close_state
        self._buttons[button_name].grid(row=0, column=len(self._buttons) - 1, ipadx=0, ipady=0, padx=0, pady=0)
        self.tts.append(Tooltip(self._buttons[button_name], f'tooltip_{button_name}'))

    def _build_button_image(self, path: pathlib.Path, master, rotate: bool = False, size: int = 30):
        image = Image.open(str(path)).convert('RGBA').resize((size, size))
        if rotate:
            image = image.rotate(180)
        data = np.array(image)
        red, green, blue, alpha = data.T
        alpha2 = np.reshape(alpha.T, (size * size))
        raw_values = []
        raw_values_disabled = []
        for x in range(0, len(alpha2)):
            raw_values.extend([0, 0, 0, min(int(alpha2[x]) * 2, 255)])
            raw_values_disabled.extend([0, 0, 0, min(int(alpha2[x] / 2), 255)])
        image = np.array(raw_values, dtype='u1')
        image = np.resize(image, (size, size, 4))
        image_dis = np.array(raw_values_disabled, dtype='u1')
        image_dis = np.resize(image_dis, (size, size, 4))
        return ImageTk.PhotoImage(Image.fromarray(image), master=master), ImageTk.PhotoImage(Image.fromarray(image_dis), master=master)

    def fetch_queue_ready_count(self):
        if self.app.state.has_access("desktop.queue_items_ready"):
            self.app.dispatcher.submit_job(
                "pipeman_desktop.client.api_client.get_queue_report",
                on_success=self._fetch_queue_ready_count,
                on_error=self.app.show_user_exception
            )
        else:
            self.app.after(5000, self.fetch_queue_ready_count)

    def _fetch_queue_ready_count(self, result: list[tuple[str, str | None, int, int]]):
        self.app.state.update_queue_ready_count(result)
        self.app.after(5000, self.fetch_queue_ready_count)

    def save(self, e=None):
        self.app.state.save_changes()

    def next_item(self, e=None):
        if self._last_choice is not None:
            self.app.state.open_qc_batch(self._last_choice, on_no_item=self._next_item)
        else:
            self._next_item()

    def _next_item(self):
        choice = ask_choice(self.app.root, self.app.state.batch_queue_choices())
        if choice is not None:
            self.app.state.open_qc_batch(choice)

    def recheck_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.RECHECK, self.next_item if load_next else None)

    def complete_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.CONTINUE, self.next_item if load_next else None)

    def release_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.RELEASE, self.next_item if load_next else None)

    def fail_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.ERROR, self.next_item if load_next else None)

    def escalate_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.ESCALATE, self.next_item if load_next else None)

    def descalate_item(self, e=None, load_next: bool | None = None):
        if load_next is None:
            load_next = self._load_next.get() > 0
        self.app.state.close_current_batch(ReviewResult.DESCALATE, self.next_item if load_next else None)

