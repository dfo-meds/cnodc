import pathlib
import queue
import time
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk
import traceback

import zrlog
from gcapp.system import System
from medsutil.savedata import SaveData
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.panes.action_pane import ActionPane
from pipeman_desktop.panes.qc_pane import QCPane
from pipeman_desktop.components.choice_dialog import ask_choice
import gcapp.i18n.base as i18n
import threading
import uuid
import typing as t

from pipeman_desktop.panes.error_pane import ErrorPane
from pipeman_desktop.panes.graph_pane import GraphPane
from pipeman_desktop.panes.history_pane import HistoryPane
from pipeman_desktop.components.loading_wheel import LoadingWheel
from pipeman_desktop.panes.login_pane import LoginPane
from pipeman_desktop.components.menu_manager import MenuManager
from pipeman_desktop.messenger import CrossThreadMessenger
from pipeman_desktop.panes.parameter_pane import ParameterPane
from pipeman_desktop.panes.record_list_pane import RecordListPane
from pipeman_desktop.panes.platform_pane import PlatformPane
from pipeman_desktop.panes.map_pane import MapPane

from gcapp.i18n.base import TranslatableError, TranslationManager
from medsutil.dynamic import dynamic_object
from autoinject import injector

from pipeman_desktop.i18n import DesktopLanguageDetector


class PipemanDispatcher(threading.Thread):

    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self, loading_wheel: LoadingWheel):
        super().__init__()
        self._loading = loading_wheel
        self.halt = threading.Event()
        self.is_working = threading.Event()
        self.work_queue = queue.SimpleQueue()
        self.result_queue = queue.SimpleQueue()
        self._job_map = {}

    def close(self):
        while not self.work_queue.empty():
            self.work_queue.get_nowait()
        while not self.result_queue.empty():
            self.result_queue.get_nowait()

    def run(self):
        while not self.halt.is_set():
            self._process_jobs_until_empty()
            self.halt.wait(1)
        self._process_jobs_until_empty()

    def _process_jobs_until_empty(self):
        while not self.work_queue.empty():
            self.is_working.set()
            info = self.work_queue.get_nowait()
            self._process_job(*info)

    def _process_job(self, job_id: str, cb: str, args: list, kwargs: dict):
        result = None
        try:
            obj = dynamic_object(cb)
            args = args or []
            kwargs = kwargs or {}
            result = obj(*args, **kwargs)
        except Exception as ex:
            result = (traceback.TracebackException.from_exception(ex), ex)
        finally:
            self.result_queue.put_nowait((job_id, result))
            self.is_working.clear()

    def submit_job(self,
                   job_callable: str,
                   on_success: t.Callable,
                   on_error: t.Callable = None,
                   job_args: list = None,
                   job_kwargs: dict = None):

        _ = dynamic_object(job_callable)
        job_id = str(uuid.uuid4())
        self.work_queue.put_nowait((job_id, job_callable, job_args, job_kwargs))
        self._job_map[job_id] = (on_success, on_error)

    def process_results(self, delay: float = 0.1, max_fetch: int = 5):
        while max_fetch > 0:
            try:
                item = self.result_queue.get(True, delay)
                self._process_result(item[0], item[1])
            except queue.Empty:
                break
        if self.is_working.is_set() or not self.work_queue.empty() or not self.result_queue.empty():
            self._loading.enable()
        else:
            self._loading.disable()

    def _process_result(self, job_id: str, results: t.Any):
        if job_id in self._job_map:
            try:
                if isinstance(results, tuple) and len(results) == 2 and isinstance(results[0], traceback.TracebackException):
                    output = ''.join(results[0].format())
                    zrlog.get_logger('desktop.dispatcher').error(f"Exception in dispatched method: {output}")
                    if self._job_map[job_id][1] is not None:
                        self._job_map[job_id][1](results[1])
                else:
                    self._job_map[job_id][0](results)
            except Exception as ex:
                # TODO: user error handling
                zrlog.get_logger('desktop.dispatcher').exception('Exception during dispatch handling')
            finally:
                del self._job_map[job_id]


class PipemanDesktop:

    local_db: LocalDatabase = None
    messenger: CrossThreadMessenger = None
    detector: i18n.LanguageDetector = None
    translations: TranslationManager = None

    @injector.construct
    def __init__(self, system: System):
        self._is_closing: bool = False
        self._save_data = SaveData(pathlib.Path("~/.pipeman.preferences.json").expanduser().absolute(), True)
        self.log = zrlog.get_logger('cnodc.desktop')
        self.state = ApplicationState(self)
        self._current_screen_size = None
        self._last_screen_width_change_time = None
        self._screen_resize_in_progress = False
        self.root = tk.Tk()
        self.root.state('zoomed')
        self.root.title(i18n.tr('root_title'))
        self.root.geometry('900x500')
        self.root.bind("<Configure>", self.on_configure)
        self.root.protocol('WM_DELETE_WINDOW', self.close)
        self._run_on_startup = True
        s = ttk.Style()
        s.configure('Errored.BorderedEntry.TFrame', background='red')
        s.configure('TButton', background='#FFFFFF')
        s.configure('Treeview', indent=5)
        self.menus: MenuManager = MenuManager(self.root)
        self.menus.add_sub_menu('file', 'menu.file')
        self.root.rowconfigure(0, weight=0)
        self.root.rowconfigure(1, weight=1)
        self.root.rowconfigure(2, weight=0)
        self.root.rowconfigure(3, weight=0)
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=4)
        self.root.columnconfigure(2, weight=1)

        # top bar
        self.top = ttk.Frame(self.root)
        self.top.grid(row=0, column=0, sticky='NSEW', columnspan=3)
        self.top.rowconfigure(0, weight=1)

        # entire left-hand side
        self.left = ttk.Frame(self.root)
        self.left.grid(row=1, column=0, sticky='NSEW', rowspan=2)
        self.left.rowconfigure(0, weight=1)
        self.left.columnconfigure(0, weight=1)

        # middle
        self.middle = ttk.Notebook(self.root)
        self.middle.grid(row=1, column=1, sticky='NSEW')

        # right
        self.right = ttk.Frame(self.root)
        self.right.grid(row=1, column=2, sticky='NSEW')
        self.right.rowconfigure(0, weight=1)
        self.right.columnconfigure(0, weight=1)

        # below middle and right
        self.middle_bottom = ttk.Notebook(self.root)
        self.middle_bottom.grid(row=2, column=1, sticky='NSEW', columnspan=2)

        # bottom bar
        self.bottom_bar = ttk.Frame(self.root)
        self.bottom_bar.grid(row=3, column=0, sticky='EWNS', columnspan=3)
        self.bottom_bar.columnconfigure(0, weight=0)
        self.bottom_bar.columnconfigure(1, weight=1)
        self.bottom_bar.columnconfigure(2, weight=0)

        # Bottom bar stuff
        self.loading_wheel = LoadingWheel(self.root, self.bottom_bar)
        self.loading_wheel.grid(row=0, column=0, sticky='W')
        self.status_info = ttk.Label(self.bottom_bar, text="W", relief="solid", borderwidth=2)
        self.status_info.grid(row=0, column=1, ipadx=5, ipady=2,  sticky='NSEW')

        self.dispatcher = PipemanDispatcher(self.loading_wheel)

        self._panes = []
        self._panes.append(LoginPane(self))
        self._panes.append(QCPane(self))
        self._panes.append(RecordListPane(self))
        self._panes.append(MapPane(self))
        self._panes.append(GraphPane(self))
        self._panes.append(ParameterPane(self))
        self._panes.append(ErrorPane(self))
        self._panes.append(ActionPane(self))
        self._panes.append(HistoryPane(self))
        self._panes.append(PlatformPane(self))
        self._pane_broadcast('on_init')

        # make sure the Exit command is last
        self.menus.add_command("file/exit", "menu.exit", self.close)

    def refresh_display(self, app_state, change_type: DisplayChange):
        if change_type & DisplayChange.LANGUAGE:
            self.root.title(i18n.tr('root.title'))
            self.menus.update_languages()
        self._pane_broadcast('refresh_display', app_state, change_type)

    def after(self, delay_ms: int, cb: t.Callable[[], t.Any], *args):
        self.root.after(delay_ms, cb, *args)  # note: this is an error in tkinter's typing not mine

    def launch(self):
        self.root.mainloop()

    def _pane_broadcast(self, call_name: str, *args, **kwargs):
        for pane in self._panes:
            try:
                getattr(pane, call_name)(*args, **kwargs)
            except Exception as ex:
                self.log.exception(f"error broadcasting {call_name} to {pane}")

    def show_user_info(self, title: str, message: str):
        tkmb.showinfo(title, message)

    def show_user_exception(self, ex: Exception):
        if isinstance(ex, TranslatableError):
            tkmb.showerror(
                title=i18n.tr('dialog.error_message.title'),
                message=i18n.tr(ex.message_key)
            )
        else:
            tkmb.showerror(
                title=i18n.tr('dialog.error_message.title'),
                message=f"{ex.__class__.__name__}: {str(ex)}"
            )


    # CLOSE ROUTINE

    def close(self):
        if self._is_closing:
            return
        self._is_closing = True
        self.state.logout(
            after_success=self._actual_close,
            after_error=self._actual_close,
            after_cancel=self._cancel_close
        )

    def _cancel_close(self):
        self.show_user_info(
            i18n.tr('dialog.unable_to_close.title'),
            i18n.tr('dialog.unable_to_close.message')
        )
        self._is_closing = False

    def _actual_close(self, e=None):
        self._pane_broadcast('on_close')
        if self.dispatcher.is_alive():
            self.dispatcher.halt.set()
            self.dispatcher.join()
        self.dispatcher.close()
        self.messenger.close()
        self.status_info.destroy()
        self.root.destroy()


    # EVENTS

    def on_configure(self, e):
        if self._run_on_startup:
            self._run_on_startup = False
            self.after(250, self.on_startup)
        screen_size = (self.root.winfo_width(), self.root.winfo_height())
        if screen_size != self._current_screen_size:
            self._current_screen_size = screen_size
            self._last_screen_width_change_time = time.monotonic()
            self._screen_resize_in_progress = True

    def on_startup(self):
        self.dispatcher.start()
        if isinstance(self.detector, DesktopLanguageDetector):
            language: str = str(self._save_data.get("language", "und"))
            self.detector.set_language(language)
            if self.detector.detect_language(self.translations.supported_languages("interface")) == "und":
                sel = ask_choice(
                    self.root,
                    options=self.translations.language_options(),
                    title="Select Language"  # todo: add a English / French style title here
                )
                if sel is None:
                    self.close()
                else:
                    self.detector.set_language(sel)
        self.refresh_display(self.state, DisplayChange.LANGUAGE)
        self.check_dispatcher()
        self.check_messages()
        self.check_screen_resize_complete()

    # REGULAR EVENT CHECKS

    def check_dispatcher(self):
        self.dispatcher.process_results()
        self.after(500, self.check_dispatcher)

    def check_messages(self):
        self.messenger.receive_into_label(self.status_info)
        self.after(50, self.check_messages)

    def check_screen_resize_complete(self):
        if self._screen_resize_in_progress:
            elapsed = time.monotonic() - self._last_screen_width_change_time
            if elapsed >= 1:
                self._screen_resize_in_progress = False
                self._last_screen_width_change_time = None
                self.state.refresh_display(DisplayChange.SCREEN_SIZE)
        self.after(500, self.check_screen_resize_complete)

