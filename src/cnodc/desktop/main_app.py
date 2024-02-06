import queue
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk
import traceback

import zrlog

from cnodc.desktop.client.local_db import LocalDatabase
from cnodc.desktop.gui.action_pane import ActionPane
from cnodc.desktop.gui.button_pane import ButtonPane
from cnodc.desktop.gui.choice_dialog import ask_choice
import cnodc.desktop.translations as i18n
import threading
import uuid
import typing as t

from cnodc.desktop.gui.error_pane import ErrorPane
from cnodc.desktop.gui.history_pane import HistoryPane
from cnodc.desktop.gui.loading_wheel import LoadingWheel
from cnodc.desktop.gui.login_pane import LoginPane
from cnodc.desktop.gui.menu_manager import MenuManager
from cnodc.desktop.gui.messenger import CrossThreadMessenger
from cnodc.desktop.gui.metadata_pane import MetadataPane
from cnodc.desktop.gui.record_list_pane import RecordListPane
from cnodc.desktop.gui.station_pane import StationPane
from cnodc.desktop.gui.tool_pane import ToolPane
from cnodc.desktop.util import TranslatableException
from cnodc.util import dynamic_object
from autoinject import injector


class CNODCQCAppDispatcher(threading.Thread):

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
                   on_success: callable,
                   on_error: callable = None,
                   job_args: list = None,
                   job_kwargs: dict = None,):
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


class CNODCQCApp:

    local_db: LocalDatabase = None
    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(i18n.get_text('root_title'))
        self.root.geometry('500x500')
        self.root.bind("<Configure>", self.on_configure)
        self.root.protocol('WM_DELETE_WINDOW', self.on_close)
        self._run_on_startup = True
        s = ttk.Style()
        s.configure('Errored.BorderedEntry.TFrame', background='red')
        self.menus = MenuManager(self.root)
        self.menus.add_sub_menu('file', 'menu_file')
        self.menus.add_sub_menu('qc', 'menu_qc')
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)
        self.top_bar = ttk.Frame(self.root)
        self.top_bar.grid(row=0, column=0, sticky='NSEW')
        self.middle_frame = ttk.Frame(self.root)
        self.middle_frame.grid(row=1, column=0, sticky='NSEW')
        self.bottom_bar = ttk.Frame(self.root)
        self.bottom_bar.grid(row=2, column=0, sticky='EWNS')
        self.bottom_bar.columnconfigure(0, weight=0)
        self.bottom_bar.columnconfigure(1, weight=1)
        self.bottom_bar.columnconfigure(2, weight=0)
        self.loading_wheel = LoadingWheel(self.root, self.bottom_bar)
        self.loading_wheel.grid(row=0, column=0, sticky='W')
        self.loading_wheel.enable()
        self.status_info = ttk.Label(self.bottom_bar, text="W", relief=tk.SOLID, borderwidth=2)
        self.status_info.grid(row=0, column=1, ipadx=5, ipady=2,  sticky='NSEW')
        self.dispatcher = CNODCQCAppDispatcher(self.loading_wheel)
        self._current_batch_type = None
        self._panes = []
        self._panes.append(LoginPane(self))
        self._panes.append(StationPane(self))
        self._panes.append(ButtonPane(self))
        self._panes.append(RecordListPane(self))
        self._panes.append(ToolPane(self))
        self._panes.append(MetadataPane(self))
        self._panes.append(ErrorPane(self))
        self._panes.append(ActionPane(self))
        self._panes.append(HistoryPane(self))
        for pane in self._panes:
            pane.on_init()

    def check_dispatcher(self):
        self.dispatcher.process_results()
        self.root.after(500, self.check_dispatcher)

    def check_messages(self):
        self.messenger.receive_into_label(self.status_info)
        self.root.after(50, self.check_messages)

    def show_user_info(self, title: str, message: str):
        tkmb.showinfo(title, message)

    def show_user_exception(self, ex: Exception):
        if isinstance(ex, TranslatableException):
            tkmb.showerror(
                title=i18n.get_text('error_message_title'),
                message=i18n.get_text(ex.key, **ex.kwargs)
            )
        else:
            tkmb.showerror(
                title=i18n.get_text('error_message_title'),
                message=f"{ex.__class__.__name__}: {str(ex)}"
            )

    def update_user_access(self, access_list: list[str]):
        for x in self._panes:
            x.on_user_access_update(access_list)

    def on_close(self):
        self.status_info.destroy()
        for x in self._panes:
            x.on_close()
        if self.dispatcher.is_alive():
            self.dispatcher.halt.set()
            self.dispatcher.join()
        while not self.dispatcher.result_queue.empty():
            self.dispatcher.result_queue.get_nowait()
        while not self.dispatcher.work_queue.empty():
            self.dispatcher.work_queue.get_nowait()
        self.root.destroy()

    def on_language_change(self):
        self.root.title(i18n.get_text('root_title'))
        self.menus.update_languages()
        for x in self._panes:
            x.on_language_change()

    def open_qc_batch(self, batch_type: str):
        self._current_batch_type = batch_type
        for x in self._panes:
            x.on_open_qc_batch(batch_type)

    def close_current_batch(self, load_next: bool = False):
        self.close_qc_batch(self._current_batch_type, load_next)
        self._current_batch_type = None

    def close_qc_batch(self, batch_type: str, load_next: bool = False):
        for x in self._panes:
            x.on_close_qc_batch(batch_type, load_next)

    def on_configure(self, e):
        if self._run_on_startup:
            self._run_on_startup = False
            self.root.after(250, self.on_startup)

    def on_startup(self):
        self.dispatcher.start()
        sel = ask_choice(self.root, options=i18n.supported_langauges(), title=i18n.get_text('language_select_dialog_title'))
        if sel is None:
            self.on_close()
        else:
            i18n.set_language(sel)
            self.on_language_change()
        self.check_dispatcher()
        self.check_messages()

    def launch(self):
        self.root.mainloop()