import queue
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk

import zrlog

from cnodc_app.gui.choice_dialog import ask_choice
import cnodc_app.translations as i18n
import threading
import uuid
import typing as t

from cnodc_app.util import dynamic_object, TranslatableException


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
            result = ex
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
                if isinstance(results, Exception):
                    zrlog.get_logger('cnodc_app.dispatcher').error(f"Exception in dispatched method: {repr(results)}")
                    if self._job_map[job_id][1] is not None:
                        self._job_map[job_id][1](results)
                else:
                    self._job_map[job_id][0](results)
            except Exception as ex:
                # TODO: user error handling
                zrlog.get_logger('cnodc_app.dispatcher').exception('Exception during dispatch handling')
            finally:
                del self._job_map[job_id]


class CNODCQCApp:

    def __init__(self):
        self.root = tk.Tk()
        s = ttk.Style()
        s.configure('Errored.BorderedEntry.TFrame', background='red')
        self.username = None
        self.access_list = []
        self.root.title(i18n.get_text('root_title'))
        self.root.geometry('500x500')
        self.root.bind("<Configure>", self.on_configure)
        self.root.bind('<<LanguageChange>>', self.on_language_change)
        self.root.protocol('WM_DELETE_WINDOW', self.on_close)
        self.menubar = tk.Menu(self.root, tearoff=0)
        self.filemenu = tk.Menu(self.menubar, tearoff=0)
        self.filemenu.add_command(label=i18n.get_text('menu_login'), command=self.do_login)
        self.menubar.add_cascade(label=i18n.get_text('menu_file'), menu=self.filemenu)
        self.root.config(menu=self.menubar)
        self._run_on_startup = True
        self.dispatcher = CNODCQCAppDispatcher()
        self.dispatcher.start()
        self._dispatch_list = {}
        bf = ttk.Frame(self.root)
        self.login_label = ttk.Label(bf, text=i18n.get_text('no_user_logged_in'))
        self.login_label.pack()
        bf.grid(row=1, column=0)

    def do_login(self):
        from cnodc_app.gui.login_dialog import ask_login
        unpw = ask_login(self.root)
        if unpw is not None:
            self.dispatcher.submit_job(
                'cnodc_app.client.api_client.login',
                job_kwargs={
                    'username': unpw[0],
                    'password': unpw[1]
                },
                on_success=self.on_login,
                on_error=self.on_login_fail
            )

    def check_dispatcher(self):
        self.dispatcher.process_results()
        self.root.after(500, self.check_dispatcher)

    def auto_refresh_session(self):
        self.dispatcher.submit_job(
            'cnodc_app.client.api_client.refresh',
            on_success=self.after_refresh,
            on_error=self.after_refresh
        )

    def after_refresh(self, res: bool = None):
        if res is False and self.username is not None:
            self.username = None
            self._update_username_display()
        self.root.after(5000, self.auto_refresh_session)

    def on_login(self, result: tuple[str, list[str]]):
        self.username = result[0]
        self.access_list = result[1]
        self._update_username_display()

    def on_login_fail(self, ex: Exception):
        self._show_user_exception(ex)
        if self.username is not None:
            self.username = None
            self.access_list = []
            self._update_username_display()

    def _show_user_exception(self, ex: Exception):
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

    def _update_username_display(self):
        if self.username is None:
            self.login_label.configure(text=i18n.get_text('no_user_logged_in'))
            self.filemenu.entryconfigure(0, state=tk.NORMAL)
        else:
            self.login_label.configure(text=i18n.get_text('user_logged_in', username=self.username))
            self.filemenu.entryconfigure(0, state=tk.DISABLED)

    def on_close(self):
        self.dispatcher.halt.set()
        self.dispatcher.join()
        while not self.dispatcher.result_queue.empty():
            self.dispatcher.result_queue.get_nowait()
        self.root.destroy()

    def on_language_change(self, e):
        self.root.title(i18n.get_text('root_title'))
        self.filemenu.entryconfigure(0, label=i18n.get_text('menu_login'))
        self.menubar.entryconfigure(0, label=i18n.get_text('menu_file'))
        self._update_username_display()

    def on_configure(self, e):
        if self._run_on_startup:
            self._run_on_startup = False
            self.on_startup()

    def on_startup(self):
        sel = ask_choice(self.root, options=i18n.supported_langauges(), title=i18n.get_text('language_select_dialog_title'))
        if sel is None:
            self.on_close()
        else:
            i18n.set_language(sel)
            self.root.event_generate('<<LanguageChange>>')
        self.check_dispatcher()
        self.auto_refresh_session()

    def launch(self):
        self.root.mainloop()
