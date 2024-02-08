import enum
import functools
import json
import queue
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk
import traceback

import zrlog
import cnodc.ocproc2.structures as ocproc2
from cnodc.desktop.gui.base_pane import QCBatchCloseOperation, ApplicationState, BatchOpenState
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
from cnodc.desktop.gui.parameter_pane import ParameterPane
from cnodc.desktop.gui.record_list_pane import RecordListPane
from cnodc.desktop.gui.station_pane import StationPane
from cnodc.desktop.gui.map_pane import MapPane
from cnodc.desktop.util import TranslatableException, StopAction
from cnodc.ocproc2.operations import QCOperator
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
            if not self.halt.is_set():
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
        self.root.state('zoomed')
        self.root.title(i18n.get_text('root_title'))
        self.root.geometry('900x500')
        self.root.bind("<Configure>", self.on_configure)
        self.root.protocol('WM_DELETE_WINDOW', self.on_close)
        self._run_on_startup = True
        s = ttk.Style()
        s.configure('Errored.BorderedEntry.TFrame', background='red')
        s.configure('Treeview', indent=5)
        self.menus = MenuManager(self.root)
        self.menus.add_sub_menu('file', 'menu_file')
        self.menus.add_sub_menu('qc', 'menu_qc')
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=4)
        self.root.columnconfigure(2, weight=1)
        self.root.rowconfigure(1, weight=1)
        self.top_bar = ttk.Frame(self.root)
        self.top_bar.grid(row=0, column=0, sticky='NSEW', columnspan=3)
        self.left_frame = ttk.Frame(self.root)
        self.left_frame.grid(row=1, column=0, sticky='NSEW')
        self.middle_frame = ttk.Frame(self.root)
        self.middle_frame.grid(row=1, column=1, sticky='NSEW')
        self.right_frame = ttk.Frame(self.root)
        self.right_frame.grid(row=1, column=2, sticky='NSEW')
        self.bottom_bar = ttk.Frame(self.root)
        self.bottom_bar.grid(row=2, column=0, sticky='EWNS', columnspan=3)
        self.bottom_bar.columnconfigure(0, weight=0)
        self.bottom_bar.columnconfigure(1, weight=1)
        self.bottom_bar.columnconfigure(2, weight=0)
        self.loading_wheel = LoadingWheel(self.root, self.bottom_bar)
        self.loading_wheel.grid(row=0, column=0, sticky='W')
        self.status_info = ttk.Label(self.bottom_bar, text="W", relief=tk.SOLID, borderwidth=2)
        self.status_info.grid(row=0, column=1, ipadx=5, ipady=2,  sticky='NSEW')
        self.dispatcher = CNODCQCAppDispatcher(self.loading_wheel)
        self._panes = []
        self._panes.append(LoginPane(self))
        self._panes.append(StationPane(self))
        self._panes.append(ButtonPane(self))
        self._panes.append(RecordListPane(self))
        self._panes.append(MapPane(self))
        self._panes.append(ParameterPane(self))
        self._panes.append(ErrorPane(self))
        self._panes.append(ActionPane(self))
        self._panes.append(HistoryPane(self))
        for pane in self._panes:
            pane.on_init()
        self.app_state = ApplicationState()

    def check_dispatcher(self):
        self.dispatcher.process_results()
        self.root.after(500, self.check_dispatcher)

    def check_messages(self):
        self.messenger.receive_into_label(self.status_info)
        self.root.after(50, self.check_messages)

    def save_operations(self, actions: list[QCOperator]):
        if self.app_state.record_uuid is not None:
            action_dict: dict[int, QCOperator] = {}
            with self.local_db.cursor() as cur:
                for action in actions:
                    rowid = cur.insert('actions', {
                        'record_uuid': self.app_state.record_uuid,
                        'action_text': json.dumps(action.to_map())
                    })
                    action_dict[rowid] = action
            self.reload_record()

    def delete_operation(self, db_index: int):
        with self.local_db.cursor() as cur:
            cur.execute('DELETE FROM actions WHERE rowid = ?', [db_index])
            cur.commit()
        self.reload_record()

    def reload_record(self):
        if self.app_state.record_uuid is not None:
            self.load_record(self.app_state.record_uuid, True)

    def load_record(self, record_uuid, force_reload: bool = False):
        if force_reload or self.app_state.record_uuid is None or self.app_state.record_uuid != record_uuid:
            with self.local_db.cursor() as cur:
                cur.execute("SELECT record_uuid, record_content FROM records WHERE record_uuid = ?", [record_uuid])
                row = cur.fetchone()
                self.app_state.record = ocproc2.DataRecord()
                self.app_state.record.from_mapping(json.loads(row[1]))
                self.app_state.record_uuid = record_uuid
                cur.execute('SELECT rowid, action_text FROM actions WHERE record_uuid = ?', [self.app_state.record_uuid])
                self.app_state.actions = {}
                for rowid, action_text in cur.fetchall():
                    operator = QCOperator.from_map(json.loads(action_text))
                    operator.apply(self.app_state.record)
                    self.app_state.actions[rowid] = operator
                if self.app_state.record_uuid is None or self.app_state.record_uuid != record_uuid:
                    self.app_state.subrecord_path = None
                elif self.app_state.subrecord_path is not None:
                    subpath = self.app_state.subrecord_path
                    self.app_state.subrecord_path = None
                    self.load_child_item(subpath, False)
                self.refresh_display()
        elif self.app_state.subrecord_path is not None:
            self.app_state.subrecord_path = None
            self.refresh_display()

    def load_child_item(self, path: str, refresh: bool = True):
        if self.app_state.record_uuid is not None and path != self.app_state.subrecord_path:
            child = self.app_state.record.find_child(path)
            if isinstance(child, ocproc2.DataRecord):
                self.app_state.subrecord_path = path
                self.app_state.child_record = child
                self.app_state.child_recordset = None
            elif isinstance(child, ocproc2.RecordSet):
                self.app_state.subrecord_path = path
                self.app_state.child_record = None
                self.app_state.child_recordset = child
            else:
                self.app_state.child_recordset = None
                self.app_state.child_record = None
            if refresh:
                self.refresh_display()

    def refresh_display(self):
        for pane in self._panes:
            pane.refresh_display(self.app_state)

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

    def save_changes(self, after_save: callable = None):
        self.app_state.save_in_progress = True
        self.refresh_display()
        self.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.save_work',
            on_error=self._on_save_error,
            on_success=functools.partial(self._after_save, after_save=after_save)
        )

    def _on_save_error(self, ex):
        self.show_user_exception(ex)
        self.app_state.save_in_progress = False
        self.refresh_display()

    def _after_save(self, res: bool, after_save: callable = None):
        if not res:
            self.show_user_info(
                i18n.get_text('save_partial_fail_title'),
                i18n.get_text('save_partial_fail_message')
            )
        self.app_state.save_in_progress = False
        if after_save:
            after_save(res)
        else:
            self.refresh_display()

    def update_user_access(self, username: str, access_list: list[str]):
        self.app_state.username = username
        self.app_state.user_access = access_list
        self.refresh_display()

    def on_close(self):
        if self.app_state.batch_type is not None:
            self.close_current_batch(QCBatchCloseOperation.RELEASE, after_close=self._on_close)
        else:
            self._on_close()

    def _on_close(self):
        self.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.logout',
            on_success=self._actual_close,
            on_error=self._actual_close
        )

    def _actual_close(self, e=None):
        if self.dispatcher.is_alive():
            self.dispatcher.halt.set()
            self.dispatcher.join()
        self.status_info.destroy()
        for x in self._panes:
            x.on_close()
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

    def open_qc_batch(self, name: str, load_fn: str):
        # TODO: check if the batch is open and prompt?
        self.app_state.open_in_progress = True
        self.app_state.batch_type = name
        self.app_state.batch_function = load_fn
        self.app_state.batch_state = BatchOpenState.OPENING
        self.refresh_display()
        self.dispatcher.submit_job(
            load_fn,
            on_success=self._open_qc_batch_success,
            on_error=self._open_qc_batch_error
        )

    def _open_qc_batch_success(self, available_actions: t.Optional[list[str]]):
        if available_actions is not None:
            self.app_state.batch_actions = available_actions
            self.app_state.batch_state = BatchOpenState.OPEN
            with self.local_db.cursor() as cur:
                # TODO: load in batch_record_info from the DB
                pass
        else:
            self.show_user_info(
                title=i18n.get_text(f'no_items_title_{self.app_state.batch_type}'),
                message=i18n.get_text(f'no_items_message_{self.app_state.batch_type}')
            )
            self.app_state.batch_state = None
            self.app_state.batch_actions = None
            self.app_state.batch_type = None
            self.app_state.batch_function = None
            self.app_state.batch_record_info = None
            self.refresh_display()

    def _open_qc_batch_error(self, ex):
        self.show_user_exception(ex)
        self.app_state.batch_state = BatchOpenState.OPEN_ERROR
        self.refresh_display()
        self.app_state.batch_state = None
        self.app_state.batch_actions = None
        self.app_state.batch_type = None
        self.app_state.batch_function = None
        self.app_state.batch_record_info = None
        self.refresh_display()

    def close_current_batch(self, op: QCBatchCloseOperation, load_next: bool = False, after_close: callable = None):
        self.app_state.batch_state = BatchOpenState.CLOSING
        self.app_state.batch_closing_op = op
        # TODO: prompt for closing if there are unsaved actions for any record
        self.refresh_display()
        self.dispatcher.submit_job(
            op.value,
            on_success=functools.partial(self._close_qc_batch_success, load_next=load_next, after_close=after_close),
            on_error=self._close_qc_batch_error
        )

    def _close_qc_batch_success(self, res, load_next: bool, after_close: callable = None):
        self.app_state.batch_actions = None
        self.app_state.batch_state = BatchOpenState.CLOSED
        self.refresh_display()
        if after_close:
            after_close()
        if load_next:
            self.open_qc_batch(self.app_state.batch_type, self.app_state.batch_function)
        else:
            self.app_state.batch_state = None
            self.app_state.batch_function = None
            self.app_state.batch_actions = None
            self.app_state.batch_type = None
            self.app_state.batch_record_info = None

    def _close_qc_batch_error(self, ex, op: QCBatchCloseOperation, batch_type: str, load_next: bool):
        self.app_state.batch_state = BatchOpenState.CLOSE_ERROR
        self.refresh_display()
        self.show_user_exception(ex)
        self.app_state.batch_state = BatchOpenState.OPEN
        self.refresh_display()

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
