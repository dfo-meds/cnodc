import queue
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk
import traceback

import zrlog

from cnodc_app.client.local_db import LocalDatabase
from cnodc_app.gui.choice_dialog import ask_choice
import cnodc_app.translations as i18n
import threading
import uuid
import typing as t

from cnodc_app.gui.login_pane import LoginPane
from cnodc_app.gui.menu_manager import MenuManager
from cnodc_app.gui.station_pane import StationPane
from cnodc_app.util import dynamic_object, TranslatableException
from autoinject import injector


class CNODCQCAppDispatcher(threading.Thread):

    def __init__(self):
        super().__init__()
        self.halt = threading.Event()
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

    def _process_result(self, job_id: str, results: t.Any):
        if job_id in self._job_map:
            try:
                if isinstance(results, tuple) and len(results) == 2 and isinstance(results[0], traceback.TracebackException):
                    output = ''.join(results[0].format())
                    zrlog.get_logger('cnodc_app.dispatcher').error(f"Exception in dispatched method: {output}")
                    if self._job_map[job_id][1] is not None:
                        self._job_map[job_id][1](results[1])
                else:
                    self._job_map[job_id][0](results)
            except Exception as ex:
                # TODO: user error handling
                zrlog.get_logger('cnodc_app.dispatcher').exception('Exception during dispatch handling')
            finally:
                del self._job_map[job_id]


class CNODCQCApp:

    local_db: LocalDatabase = None

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
        self.dispatcher = CNODCQCAppDispatcher()
        self._dispatch_list = {}
        self._panes = []
        self._panes.append(LoginPane(self))
        self._panes.append(StationPane(self))
        self.bottom_bar = ttk.Frame(self.root)
        self.bottom_bar.grid(row=1, column=0)
        for pane in self._panes:
            pane.on_init()

    def check_dispatcher(self):
        self.dispatcher.process_results()
        self.root.after(500, self.check_dispatcher)

    def reload_stations(self):
        self.dispatcher.submit_job(
            'cnodc_app.client.api_client.reload_stations',
            on_success=self.on_station_reload,
            on_error=self.on_station_reload_error
        )

    def on_station_reload(self, result: bool):
        pass

    def on_station_reload_error(self, ex: Exception):
        pass

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

    def launch(self):
        self.root.mainloop()
