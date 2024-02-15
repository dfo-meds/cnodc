import functools
import json
import queue
import time
import tkinter as tk
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk
import traceback

import zrlog
import cnodc.ocproc2.structures as ocproc2
from cnodc.desktop import VERSION
from cnodc.desktop.gui.base_pane import QCBatchCloseOperation, ApplicationState, BatchOpenState, DisplayChange, \
    BatchType, SimpleRecordInfo, CloseBatchResult
from cnodc.desktop.client.local_db import LocalDatabase
from cnodc.desktop.gui.action_pane import ActionPane
from cnodc.desktop.gui.button_pane import ButtonPane
from cnodc.desktop.gui.choice_dialog import ask_choice
import cnodc.desktop.translations as i18n
import threading
import uuid
import typing as t

from cnodc.desktop.gui.error_pane import ErrorPane
from cnodc.desktop.gui.graph_pane import GraphPane
from cnodc.desktop.gui.history_pane import HistoryPane
from cnodc.desktop.gui.loading_wheel import LoadingWheel
from cnodc.desktop.gui.login_pane import LoginPane
from cnodc.desktop.gui.menu_manager import MenuManager
from cnodc.desktop.gui.messenger import CrossThreadMessenger
from cnodc.desktop.gui.parameter_pane import ParameterPane
from cnodc.desktop.gui.record_list_pane import RecordListPane
from cnodc.desktop.gui.station_pane import StationPane
from cnodc.desktop.gui.map_pane import MapPane
from cnodc.desktop.util import TranslatableException
from cnodc.ocproc2.operations import QCOperator, QCSetWorkingQuality, QCAddHistory
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

    def close(self):
        while not self.work_queue.empty():
            self.work_queue.get_nowait()
        while not self.result_queue.empty():
            self.result_queue.empty()

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
                   job_kwargs: dict = None):

        obj = dynamic_object(job_callable)
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
        self.log = zrlog.get_logger('cnodc.desktop')
        self.app_state = ApplicationState(self.refresh_display)
        self._current_screen_size = None
        self._last_screen_width_change_time = None
        self._screen_resize_in_progress = False
        self.root = tk.Tk()
        self.root.state('zoomed')
        self.root.title(i18n.get_text('root_title'))
        self.root.geometry('900x500')
        self.root.bind("<Configure>", self.on_configure)
        self.root.protocol('WM_DELETE_WINDOW', self.on_close)
        self._run_on_startup = True
        s = ttk.Style()
        s.configure('Errored.BorderedEntry.TFrame', background='red')
        s.configure('TButton', background='#FFFFFF')
        s.configure('Treeview', indent=5)
        self.menus = MenuManager(self.root)
        self.menus.add_sub_menu('file', 'menu_file')
        self.menus.add_sub_menu('qc', 'menu_qc')
        self.root.rowconfigure(0, weight=0)
        self.root.rowconfigure(1, weight=0)
        self.root.rowconfigure(2, weight=0)
        self.root.rowconfigure(3, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=0)
        self.root.columnconfigure(2, weight=0)
        self.root.columnconfigure(3, weight=1)
        self.top_bar = ttk.Frame(self.root)
        self.top_bar.grid(row=0, column=1, sticky='NSEW', columnspan=3)
        self.top_bar.rowconfigure(0, weight=1)
        self.top_bar.columnconfigure(0, weight=0)
        self.top_bar.columnconfigure(1, weight=1)
        self.middle_left = ttk.Frame(self.root)
        self.middle_left.grid(row=1, column=1, sticky='NSEW', rowspan=2)
        self.middle_left.rowconfigure(0, weight=1)
        self.middle_left.columnconfigure(0, weight=1)
        self.middle_right = ttk.Frame(self.root)
        self.middle_right.grid(row=1, column=2, sticky='NSEW', rowspan=2)
        self.middle_right.rowconfigure(0, weight=1)
        self.middle_right.columnconfigure(0, weight=1)
        self.far_right = ttk.Frame(self.root)
        self.far_right.grid(row=1, column=3, sticky='NSEW', rowspan=2)
        self.far_right.rowconfigure(0, weight=1)
        self.far_right.columnconfigure(0, weight=1)
        self.bottom_notebook = ttk.Notebook(self.root)
        self.bottom_notebook.grid(row=3, column=1, sticky='NSEW', columnspan=3)
        self.bottom_bar = ttk.Frame(self.root)
        self.bottom_bar.grid(row=4, column=0, sticky='EWNS', columnspan=4)
        self.bottom_bar.columnconfigure(0, weight=0)
        self.bottom_bar.columnconfigure(1, weight=1)
        self.bottom_bar.columnconfigure(2, weight=0)
        self.left_frame = ttk.Frame(self.root)
        self.left_frame.grid(row=0, column=0, sticky='NSEW', rowspan=4)
        self.left_frame.rowconfigure(0, weight=1)
        self.left_frame.columnconfigure(0, weight=1)
        self.loading_wheel = LoadingWheel(self.root, self.bottom_bar)
        self.loading_wheel.grid(row=0, column=0, sticky='W')
        self.status_info = ttk.Label(self.bottom_bar, text="W", relief=tk.SOLID, borderwidth=2)
        self.status_info.grid(row=0, column=1, ipadx=5, ipady=2,  sticky='NSEW')
        self.dispatcher = CNODCQCAppDispatcher(self.loading_wheel)
        self._panes = []
        self._panes.append(LoginPane(self))
        self._panes.append(ButtonPane(self))
        self._panes.append(RecordListPane(self))
        self._panes.append(MapPane(self))
        self._panes.append(GraphPane(self))
        self._panes.append(ParameterPane(self))
        self._panes.append(ErrorPane(self))
        self._panes.append(ActionPane(self))
        self._panes.append(HistoryPane(self))
        self._panes.append(StationPane(self))
        self._is_closing: bool = False
        self._pane_broadcast('on_init')

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
            self.app_state.extend_actions(action_dict)

    def delete_operation(self, db_index: int):
        with self.local_db.cursor() as cur:
            cur.execute('DELETE FROM actions WHERE rowid = ?', [db_index])
            cur.commit()
        self.reload_record()

    def reload_record(self):
        if self.app_state.record_uuid is not None:
            self.load_record(self.app_state.record_uuid, True)

    def load_child(self, child_path: t.Optional[str]):
        if child_path != self.app_state.subrecord_path:
            self.app_state.set_record_subpath(child_path)

    def load_record(self, record_uuid, force_reload: bool = False):
        if force_reload or self.app_state.record_uuid is None or self.app_state.record_uuid != record_uuid:
            with self.local_db.cursor() as cur:
                cur.execute("SELECT record_uuid, record_content FROM records WHERE record_uuid = ?", [record_uuid])
                row = cur.fetchone()
                record = ocproc2.DataRecord()
                record.from_mapping(json.loads(row[1]))
                cur.execute('SELECT rowid, action_text FROM actions WHERE record_uuid = ?', [record_uuid])
                actions = {}
                for rowid, action_text in cur.fetchall():
                    operator = QCOperator.from_map(json.loads(action_text))
                    operator.apply(record, None)
                    actions[rowid] = operator
                subrecord_path = None
                if self.app_state.record_uuid is None or self.app_state.record_uuid != record_uuid:
                    subrecord_path = self.app_state.subrecord_path
                self.app_state.set_record_info(record_uuid, record, subrecord_path, actions)
        elif self.app_state.subrecord_path is not None:
            self.app_state.set_record_subpath(None)

    def refresh_display(self, app_state, change_type: DisplayChange):
        self._pane_broadcast('refresh_display', app_state, change_type)

    def _pane_broadcast(self, call_name: str, *args, **kwargs):
        for pane in self._panes:
            try:
                getattr(pane, call_name)(*args, **kwargs)
            except Exception as ex:
                self.log.exception(f"error broadcasting {call_name} to {pane}")

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
        if self.app_state.is_batch_action_available('apply_working'):
            if self.app_state.has_unsaved_changes:
                self.app_state.set_save_flag(True)
                self.dispatcher.submit_job(
                    'cnodc.desktop.client.api_client.save_work',
                    on_error=self._on_save_error,
                    on_success=functools.partial(self._after_save, after_save=after_save)
                )
            elif after_save is not None:
                after_save(True)

    def quality_color(self, wq: int, ind_wq: int = None):
        if wq == 1:
            return 'forestgreen'
        elif wq == 2:
            return 'lightseagreen'
        elif wq == 12:
            return 'turquoise'
        elif wq == 3:
            return 'darkorange'
        elif wq == 13:
            return 'goldenrod'
        elif wq == 4:
            return 'maroon'
        elif wq == 14:
            return 'coral'
        elif wq == 9:
            return 'purple'
        elif wq == 19:
            return 'pink'
        elif wq in (20, 21):
            return 'red'
        elif ind_wq is not None and ind_wq in (3, 4, 13, 14, 19, 9):
            return 'silver'
        return 'black'

    def _on_save_error(self, ex):
        self.show_user_exception(ex)
        self.app_state.set_save_flag(False, self.app_state.has_unsaved_changes)

    def _after_save(self, res: bool, after_save: callable = None):
        if not res:
            self.show_user_info(
                i18n.get_text('save_partial_fail_title'),
                i18n.get_text('save_partial_fail_message')
            )
            self.app_state.set_save_flag(False, self.app_state.has_unsaved_changes)
        else:
            self.app_state.set_save_flag(False, False)
        if after_save:
            after_save(res)

    def update_user_info(self, username: t.Optional[str], access_list: dict[str, dict[str, str]]):
        if self.app_state.update_user_info(username, access_list) and username is None:
            self.force_close_current_batch()

    def on_close(self):
        if self._is_closing:
            return
        self._is_closing = True
        result = self.close_current_batch(QCBatchCloseOperation.RELEASE, after_close=self._on_close)
        if result == CloseBatchResult.CANCELLED:
            self._is_closing = False

    def _on_close(self):
        self.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.logout',
            on_success=self._actual_close,
            on_error=self._actual_close
        )

    def _actual_close(self, e=None):
        self.app_state.clear_batch()
        if self.dispatcher.is_alive():
            self.dispatcher.halt.set()
            self.dispatcher.join()
        self._pane_broadcast('on_close')
        self.dispatcher.close()
        self.messenger.close()
        self.status_info.destroy()
        self.root.destroy()

    def create_flag_operator(self, target_path: str, flag: int):
        return QCSetWorkingQuality(
            value_path=target_path,
            new_value=flag,
            children=[
                QCAddHistory(
                    message=f"CHANGE QC FLAG [{target_path}] to [{flag}]",
                    source_name="manual_qc",
                    source_version=VERSION,
                    source_instance=self.app_state.username,
                    message_type=ocproc2.MessageType.INFO.value
                )
            ]
        )

    def on_language_change(self):
        self.root.title(i18n.get_text('root_title'))
        self.menus.update_languages()
        self._pane_broadcast('on_language_change', i18n.current_language())

    def load_closest_child(self, full_path: str):
        path: list[str] = full_path.split('/')
        if 'subrecords' in path:
            idx = -1
            while path[idx] != 'subrecords':
                idx -= 1
            record_path = '/'.join(path[0:idx + 4])
            self.load_child(record_path)
        else:
            self.load_child(None)

    def open_qc_batch(self, batch_service_name: str):
        # TODO: check if the batch is open and prompt?
        self.app_state.start_batch_open(batch_service_name)
        self.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.next_queue_item',
            job_kwargs={
                'service_name': batch_service_name
            },
            on_success=self._open_qc_batch_success,
            on_error=self._open_qc_batch_error
        )

    def _open_qc_batch_success(self, result: tuple[list[str], list[str]]):
        if result is not None and result[0] is not None:
            with self.local_db.cursor() as cur:
                cur.execute("SELECT rowid, record_uuid, lat, lon, datetime, has_errors, lat_qc, lon_qc, datetime_qc, station_id FROM records ORDER BY station_id ASC, datetime ASC")
                record_info = [SimpleRecordInfo(idx + 1, *x) for idx, x in enumerate(cur.fetchall())]
                self.app_state.complete_batch_open(result[0], result[1], record_info)
        else:
            self.show_user_info(
                title=i18n.get_text(f'no_items_title_{self.app_state.batch_service_name}'),
                message=i18n.get_text(f'no_items_message_{self.app_state.batch_service_name}')
            )
            self.app_state.clear_batch()

    def _open_qc_batch_error(self, ex):
        self.show_user_exception(ex)
        self.app_state.handle_batch_open_error()

    def force_close_current_batch(self):
        if self.app_state.batch_state == BatchOpenState.OPEN:
            self.app_state.start_batch_close(QCBatchCloseOperation.FORCE_CLOSE, False)
            self.app_state.complete_batch_close()
            self.app_state.clear_batch()

    def close_current_batch(self,
                            op: QCBatchCloseOperation,
                            load_next: bool = False,
                            after_close: callable = None) -> CloseBatchResult:
        if self.app_state.batch_state == BatchOpenState.OPEN:
            if self.app_state.has_unsaved_changes:
                result = tkmb.askyesno(
                    title=i18n.get_text('close_without_saving_title'),
                    message=i18n.get_text('close_without_saving_message')
                )
                if not result:
                    return CloseBatchResult.CANCELLED
            self.app_state.start_batch_close(op, load_next)
            self.dispatcher.submit_job(
                op.value if '.' in op.value else QCBatchCloseOperation.RELEASE.value,
                on_success=functools.partial(self._close_qc_batch_success, after_close=after_close),
                on_error=self._close_qc_batch_error
            )
            return CloseBatchResult.CLOSING
        elif after_close is not None:
            after_close()
        return CloseBatchResult.ALREADY_CLOSED

    def _close_qc_batch_success(self, res, after_close: callable = None):
        self.app_state.complete_batch_close()
        if self.app_state.batch_load_after_close:
            self.open_qc_batch(self.app_state.batch_service_name)
        else:
            self.app_state.clear_batch()
        if after_close is not None:
            after_close()

    def _close_qc_batch_error(self, ex):
        self.show_user_exception(ex)
        self.app_state.handle_batch_close_error()

    def on_configure(self, e):
        if self._run_on_startup:
            self._run_on_startup = False
            self.root.after(250, self.on_startup)
        screen_size = (self.root.winfo_width(), self.root.winfo_height())
        if screen_size != self._current_screen_size:
            self._current_screen_size = screen_size
            self._last_screen_width_change_time = time.monotonic()
            self._screen_resize_in_progress = True

    def check_screen_resize_complete(self):
        if self._screen_resize_in_progress:
            elapsed = time.monotonic() - self._last_screen_width_change_time
            if elapsed >= 1:
                self._screen_resize_in_progress = False
                self._last_screen_width_change_time = None
                self.app_state.refresh_display(DisplayChange.SCREEN_SIZE)
        self.root.after(500, self.check_screen_resize_complete)

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
        self.check_screen_resize_complete()

    def launch(self):
        self.root.mainloop()
