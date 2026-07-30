import enum
import functools
import socket
import typing as t
from tkinter import messagebox as tkmb

from gcapp import i18n as i18n
from medsutil import ocproc2 as ocproc2, json
from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import RecordAction
from pipeman_desktop.util import BatchOpenState, ReviewResult, CloseBatchResult

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop


class DisplayChange(enum.IntFlag):

    USER = enum.auto()
    BATCH_STATE = enum.auto()
    RECORD = enum.auto()
    RECORD_CHILD = enum.auto()
    ACTION = enum.auto()
    SAVING = enum.auto()
    SCREEN_SIZE = enum.auto()
    QUEUE_INFO = enum.auto()
    RECORD_SET = enum.auto()


class SimpleRecordInfo:

    def __init__(self,
                 idx: int,
                 rowid: int,
                 record_uuid: str,
                 lat: t.Optional[float] = None,
                 lon: t.Optional[float] = None,
                 ts: t.Optional[str] = None,
                 has_errors: t.Optional[bool] = None,
                 lat_qc: t.Optional[int] = None,
                 lon_qc: t.Optional[int] = None,
                 time_qc: t.Optional[int] = None,
                 platform_id: t.Optional[str] = None):
        self.index: int = idx
        self.rowid: int = rowid
        self.record_uuid: str = record_uuid
        self.latitude: float | None = float(lat) if lat is not None else None
        self.longitude: float | None = float(lon) if lon is not None else None
        self.timestamp: AwareDateTime | None = AwareDateTime.fromisoformat(ts) if ts else None
        self.has_errors: bool = bool(has_errors)
        self.platform_id: str | None = platform_id
        self.latitude_qc: int = int(lat_qc) if lat_qc is not None else 0
        self.longitude_qc: int = int(lon_qc) if lon_qc is not None else 0
        self.time_qc: int = int(time_qc) if time_qc is not None else 0


class ApplicationState:

    def __init__(self, app: PipemanDesktop):
        self._app = app
        self._username: t.Optional[str] = None
        self._available_services: list[str] | None = None
        self._queue_ready_report: list[tuple[str, str | None, int, int]] = []
        self._save_in_progress: bool = False
        self._batch_service_name: str | None = None
        self._batch_state: t.Optional[BatchOpenState] = None
        self._has_unsaved_changes: bool = False
        self._batch_records: dict[str, SimpleRecordInfo] | None = {}
        self._batch_close_op: t.Optional[ReviewResult] = None
        self._batch_actions: list[str] | None = None
        self._current_working_uuid: str | None = None
        self._current_parent: ocproc2.ParentRecord | None = None
        self._current_actions: dict[int, RecordAction] | None = None
        self._current_recordset: ocproc2.RecordSet | None = None
        self._current_record: ocproc2.BaseRecord | None = None
        self._current_child_path: str | None = None

        self.subrecord_path: t.Optional[str] = None
        self.child_record: t.Optional[ocproc2.ChildRecord] = None
        self.child_recordset: t.Optional[ocproc2.RecordSet] = None
        self.actions: t.Optional[list[RecordAction]] = None

    @property
    def save_in_progress(self) -> bool:
        return self._save_in_progress

    @property
    def username(self) -> str | None:
        return self._username

    @property
    def batch_state(self) -> BatchOpenState | None:
        return self._batch_state

    @property
    def batch_service_name(self) -> str | None:
        return self._batch_service_name

    @property
    def record_actions(self) -> dict[int, RecordAction]:
        return self._current_actions or {}

    @property
    def batch_records(self) -> dict[str, SimpleRecordInfo]:
        return self._batch_records or {}

    @property
    def has_unsaved_changes(self) -> bool:
        return self._has_unsaved_changes

    @property
    def current_working_uuid(self) -> str | None:
        return self._current_working_uuid

    @property
    def current_parent(self) -> ocproc2.ParentRecord | None:
        return self._current_parent

    @property
    def current_child_path(self) -> str | None:
        return self._current_child_path

    @property
    def current_recordset(self) -> ocproc2.RecordSet | None:
        return self._current_recordset

    @property
    def current_record(self) -> ocproc2.BaseRecord | None:
        return self._current_record

    def refresh_display(self, change_type: DisplayChange, *args, **kwargs):
        self._app.refresh_display(self, change_type)

    def has_access(self, service_name: str) -> bool:
        return self._available_services is not None and service_name in self._available_services

    def logout(self,
               after_success: t.Callable[[], t.Any] | None = None,
               after_error: t.Callable[[], t.Any] | None = None,
               after_cancel: t.Callable[[], t.Any] | None = None):
        result = CloseBatchResult.UNABLE_TO_CLOSE if self._batch_state is not None else CloseBatchResult.ALREADY_CLOSED
        if self.can_logout():
            result = self.close_current_batch(
                batch_action=ReviewResult.RELEASE,
                after_close=functools.partial(
                    self._finish_logout,
                    after_success=after_success,
                    after_error=after_error,
                    after_cancel=after_cancel
                )
            )
        self._finish_logout(result, after_success=after_success, after_error=after_error, after_cancel=after_cancel)
        return result

    def _finish_logout(self,
                       result: CloseBatchResult | None = None,
                       after_success: t.Callable[[], t.Any] | None = None,
                       after_error: t.Callable[[], t.Any] | None = None,
                       after_cancel: t.Callable[[], t.Any] | None = None):

        # this indicates we're coming in after the dispatcher has attempted to close the
        # current batch, so we try to determine the current state.
        if result is None:
            if self._batch_state is None:
                result = CloseBatchResult.ALREADY_CLOSED
            else:
                result = CloseBatchResult.UNABLE_TO_CLOSE

        # this indicates an error or the current state prohibits us from closing
        # we ask the user what they want to do - cancel or force close without saving changes
        if result is CloseBatchResult.UNABLE_TO_CLOSE:
            user_option = tkmb.askyesno(
                title=i18n.tr("error_close_without_saving_title"),
                message=i18n.tr("error_close_without_saving_message"),
            )
            if result:
                self.force_close_current_batch()
                result = CloseBatchResult.ALREADY_CLOSED
            else:
                result = CloseBatchResult.CANCELLED

        # we closed or force closed the batch, we call after_close()
        if result is CloseBatchResult.ALREADY_CLOSED:
            self._real_logout(after_success=after_success, after_error=after_error)

        # otherwise, we call the cancel handler
        elif result is CloseBatchResult.CANCELLED:
            after_cancel()

    def _real_logout(self,
                     after_success: t.Callable[[], t.Any] | None = None,
                     after_error: t.Callable[[], t.Any] | None = None,):
        self._app.dispatcher.submit_job(
            'pipeman_desktop.client.api_client.logout',
            on_success=functools.partial(self._logout_success, after_success=after_success),
            on_error=functools.partial(self._logout_error, after_error=after_error),
        )

    def _logout_success(self,
                        result,
                        after_success: t.Callable[[], t.Any] | None = None):
        self._app.show_user_info(
            i18n.tr('logout_success_title'),
            i18n.tr('logout_success_message')
        )
        self._app.state.update_user_info(None, None)
        if after_success is not None:
            after_success()

    def _logout_error(self,
                      result,
                      after_error: t.Callable[[], t.Any] | None = None):
        self._app.show_user_exception(result)
        if after_error is not None:
            after_error()

    def can_logout(self):
        if self.username is None:
            return False
        if self.save_in_progress:
            return False
        return self.batch_state is None or self.batch_state == BatchOpenState.OPEN

    def update_queue_ready_count(self, result: list[tuple[str, str | None, int, int]]):
        self._queue_ready_report = result
        self.refresh_display(DisplayChange.QUEUE_INFO)

    def queue_ready_count(self, queue_name: str, subqueue_name: str | None = None, escalation_level: int | None = None) -> int:
        total = 0
        for x in self._queue_ready_report:
            if x[0] == queue_name and (subqueue_name is None or subqueue_name == x[1]) and (escalation_level is None or x[2] == escalation_level):
                total += x[3]
        return total

    def save_changes(self, after_save: t.Callable[[bool | None], t.Any] | None = None):
        if self.can_save_changes():
            if self.has_unsaved_changes:
                self.update_save_flags(True)
                self._app.dispatcher.submit_job(
                    "pipeman_desktop.client.api_client.save_changes",
                    on_error=functools.partial(self._on_save_error, after_save=after_save),
                    on_success=functools.partial(self._on_save_success, after_save=after_save)
                )
            else:
                self._on_save_success(True, after_save=after_save)

    def _on_save_error(self, ex: Exception, after_save: t.Callable[[bool | None], t.Any] | None = None):
        self._app.show_user_exception(ex)
        self._on_save_success(None, after_save)

    def _on_save_success(self, result: bool | None, after_save: t.Callable[[bool | None], t.Any] | None = None):
        if result is False:
            self._app.show_user_info(
                i18n.tr("save_partial_fail_title"),
                i18n.tr("save_partial_fail_message"),
            )
        elif result:
            self._has_unsaved_changes = False
        self.update_save_flags(False)
        if after_save is not None:
            after_save(result)

    def can_save_changes(self) -> bool:
        if self.save_in_progress:
            return False
        if self.batch_state is None or self.batch_state != BatchOpenState.OPEN:
            return False
        return self.has_access(f"batch_qc.{self._batch_service_name}.save") and self.has_unsaved_changes

    def open_qc_batch(self,
                      batch_service_name: str,
                      on_no_item: t.Callable | None = None):
        self._batch_service_name = batch_service_name
        self._batch_state = BatchOpenState.OPENING
        self.refresh_display(DisplayChange.BATCH_STATE | DisplayChange.SAVING)
        self._app.dispatcher.submit_job(
            "pipeman_desktop.client.api_client.open_batch",
            job_kwargs={
                "batch_service_name": self._batch_service_name,
            },
            on_success=functools.partial(self._on_qc_batch_open_success, on_no_item=on_no_item),
            on_error=functools.partial(self._on_qc_batch_open_error, on_no_item=on_no_item)
        )

    def _on_qc_batch_open_error(self, ex: Exception, on_no_item: t.Callable | None = None):
        self._app.show_user_exception(ex)
        self._batch_state = BatchOpenState.OPEN_ERROR
        self.refresh_display(DisplayChange.BATCH_STATE | DisplayChange.SAVING)
        self._on_qc_batch_open_success(False, on_no_item)

    def _on_qc_batch_open_success(self, result: list[str] | None | bool, on_no_item: t.Callable | None = None):
        if isinstance(result, list):
            self._batch_actions = result
            self._batch_records = {}
            with self._app.local_db.cursor() as cur:
                cur.execute("SELECT rowid, record_uuid, lat, lon, datetime, has_errors, lat_qc, lon_qc, datetime_qc, platform_id FROM records ORDER BY platform_id ASC, datetime ASC")
                for idx, row in enumerate(cur.fetchall()):
                    record = SimpleRecordInfo(idx + 1, *row)
                    self._batch_records[record.record_uuid] = record
            self._batch_state = BatchOpenState.OPEN
            self._has_unsaved_changes = False
            self.refresh_display(DisplayChange.BATCH_STATE | DisplayChange.SAVING)
        else:
            self._batch_actions = None
            if result is not False:
                self._app.show_user_info(
                    title=i18n.tr(f'no_items_title_{self.batch_service_name}'),
                    message=i18n.tr(f'no_items_message_{self.batch_service_name}')
                )
            if on_no_item is not None:
                on_no_item()
            self.update_batch_state(None)

    def can_open_qc_batch(self) -> bool:
        if self._available_services is None:
            return False
        if self.batch_state is not None:
            return False
        return any(x.startswith("batch_qc.") and x.endswith(".open") for x in self._available_services)

    def force_close_current_batch(self):
        if self._batch_state is not None:
            self._batch_close_op = ReviewResult.FORCE_CLOSE
            self.update_batch_state(BatchOpenState.CLOSING)
            self._on_close_current_batch_success(True)

    def close_current_batch(self,
                            batch_action: ReviewResult,
                            on_success: t.Callable | None = None,
                            on_error: t.Callable | None = None,
                            after_close: t.Callable | None = None) -> CloseBatchResult:
        if self.can_close_current_batch(batch_action):
            if self.has_unsaved_changes:
                result = tkmb.askyesno(
                    title=i18n.tr("close_without_saving_title"),
                    message=i18n.tr("close_without_saving_message"),
                )
                if not result:
                    return CloseBatchResult.CANCELLED
            self._batch_close_op = batch_action
            self.update_batch_state(BatchOpenState.CLOSING)
            self._app.dispatcher.submit_job(
                "pipeman_desktop.client.api_client.close_batch",
                job_kwargs={
                    "close_operation": batch_action.value,
                },
                on_success=functools.partial(self._on_close_current_batch_success, on_success=on_success, after_close=after_close),
                on_error=functools.partial(self._on_close_current_batch_error, on_error=on_error, after_close=after_close)
            )
            return CloseBatchResult.CLOSING
        elif self._batch_state is None:
            self._on_close_current_batch_success(True, on_success=on_success, after_close=after_close)
            return CloseBatchResult.ALREADY_CLOSED
        else:
            return CloseBatchResult.UNABLE_TO_CLOSE

    def _on_close_current_batch_error(self, ex: Exception, on_error: t.Callable | None = None, after_close: t.Callable | None = None):
        self._app.show_user_exception(ex)
        self.update_batch_state(BatchOpenState.CLOSE_ERROR)
        if on_error is not None:
            on_error()
        self._on_close_current_batch_success(None, after_close=after_close)

    def _on_close_current_batch_success(self, result: bool | None, on_success: t.Callable | None = None, after_close: t.Callable | None = None):
        if result:
            if self._batch_state is not None:
                self.update_batch_state(BatchOpenState.CLOSED)
                self.refresh_display(DisplayChange.BATCH_STATE)
            self._batch_close_op = None
            self.update_batch_state(None)
            if on_success is not None:
                on_success()
        else:
            self._batch_close_op = None
            self.update_batch_state(BatchOpenState.OPEN)
        if after_close is not None:
            after_close()

    BATCH_VARIABLE_AVAILABILITY = {
        ReviewResult.DESCALATE,
        ReviewResult.ESCALATE,
        ReviewResult.CONTINUE,
    }

    def can_close_current_batch(self, batch_action: ReviewResult) -> bool:
        if self.save_in_progress:
            return False
        if self.batch_state is None or self.batch_state != BatchOpenState.OPEN:
            return False
        if batch_action.value.startswith("_"):
            return True
        if batch_action in self.BATCH_VARIABLE_AVAILABILITY:
            return self._batch_actions is not None and batch_action.value in self._batch_actions
        else:
            return True


    def batch_queue_choices(self) -> dict[str, str]:
        results = set()
        if self._available_services is not None:
            for key in self._available_services:
                pieces = key.split('.')
                if pieces[0] == "batch_qc" and len(pieces) > 1 and pieces[1]:
                    results.add(pieces[1])
        return {
            x: i18n.tr(f"batch_qc_{x}")
            for x in results
        }

    def load_closest(self, path: str):
        ...

    def add_action(self, action: RecordAction):
        from pipeman_desktop import VERSION
        action.source_name = "pipeman_desktop"
        action.source_version = VERSION
        action.process_id = socket.gethostname()
        action.username = self.username
        remove_keys = []
        if self._current_record is not None:
            for key, other_action in self._current_actions.items():
                if action.conflicts_with(other_action):
                    remove_keys.append(key)
        with self._app.local_db.cursor() as cur:
            for delete_id in remove_keys:
                cur.execute("DELETE FROM actions WHERE rowid = ?", (delete_id,))
            cur.execute("INSERT INTO actions (record_uuid, action_text) VALUES (?, ?)", (
                self._current_working_uuid,
                json.dumps(action.export())
            ))
            cur.commit()
        self.update_record(self._current_working_uuid, True)

    def delete_action(self, db_id: int):
        with self._app.local_db.cursor() as cur:
            cur.execute("DELETE FROM actions WHERE rowid = ?", (db_id,))
            cur.commit()

    def update_record(self, working_uuid: str | None, force_reload: bool = False):
        if working_uuid is None and self._current_record is not None:
            self._current_record = None
            self._current_parent = None
            self._current_actions = None
            self._current_recordset = None
            self._current_child_path = None
            self.refresh_display(DisplayChange.RECORD | DisplayChange.RECORD_CHILD | DisplayChange.RECORD_SET | DisplayChange.ACTION)
        elif force_reload or working_uuid != self._current_working_uuid:
            with self._app.local_db.cursor() as cur:
                cur.execute("SELECT record_content FROM records WHERE record_uuid = ?", (working_uuid,))
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Invalid record ID")
                self._current_parent = ocproc2.ParentRecord.build_from_mapping(json.load_dict(row[0]))
                self._current_recordset = None
                self._current_record = self._current_parent
                if working_uuid == self._current_working_uuid and self._current_child_path is not None:
                    self.update_subrecord(self._current_child_path, force_reload=True, _send_refresh=False)
                else:
                    self._current_child_path = None
                self._current_working_uuid = working_uuid
                self.refresh_display(DisplayChange.RECORD | DisplayChange.RECORD_CHILD | DisplayChange.RECORD_SET | DisplayChange.ACTION)

    def _update_actions(self, working_uuid: str | None = None):
        if working_uuid is None: working_uuid = self._current_working_uuid
        if working_uuid is None: return
        with self._app.local_db.cursor() as cur:
            cur.execute("SELECT rowid, action_text FROM actions WHERE record_uuid = ?", (working_uuid,))
            self._current_actions = {}
            for rowid, action in cur.fetchall():
                operation = RecordAction.from_map(json.load_dict(action))
                operation.apply(self._current_parent)
                self._current_actions[rowid] = operation

    def update_subrecord(self, subrecord_path: str | None, force_reload: bool = False, _send_refresh: bool = True):
        if subrecord_path is None and self._current_child_path is not None:
            self._current_record = None
            self._current_recordset = None
            self._current_child_path = None
            if _send_refresh:
                self.refresh_display(DisplayChange.RECORD_CHILD | DisplayChange.RECORD_SET)
        if force_reload or subrecord_path != self._current_child_path:
            self._current_child_path = subrecord_path
            child = self._current_parent.find_child(subrecord_path) if subrecord_path is not None else None
            if isinstance(child, ocproc2.RecordSet):
                self._current_recordset = child
                self._current_record = None
            elif isinstance(child, ocproc2.ChildRecord):
                self._current_record = child
                self._current_recordset = None
            else:
                self._current_record = None
                self._current_recordset = None
                self._current_child_path = None
            if _send_refresh:
                self.refresh_display(DisplayChange.RECORD_CHILD | DisplayChange.RECORD_SET)

    def update_user_info(self, username: str | None, access_list: list[str]) -> bool:
        if self.username != username or self._available_services != access_list:
            self._username = username
            self._available_services = access_list
            self.refresh_display(DisplayChange.USER)
            if self._username is None:
                self.force_close_current_batch()
            return True
        return False

    def update_save_flags(self, is_saving: bool | None = None, has_unsaved_changes: t.Optional[bool] = None):
        broadcast: bool = False
        if is_saving is not None and is_saving != self._save_in_progress:
            self._save_in_progress = bool(is_saving)
            broadcast = True
        if has_unsaved_changes is not None and has_unsaved_changes != self._has_unsaved_changes:
            self._has_unsaved_changes = bool(has_unsaved_changes)
            broadcast = True
        if broadcast:
            self.refresh_display(DisplayChange.SAVING)

    def update_batch_state(self, batch_state: BatchOpenState | None):
        if batch_state is None:
            if self._batch_state is not None:
                self._batch_state = None
                self._batch_service_name = None
                self._batch_records = None
                self._batch_close_op = None
                self._batch_actions = None
                self.refresh_display(DisplayChange.BATCH_STATE)
            self.update_save_flags(False, False)
        elif batch_state is not self._batch_state:
            self._batch_state = batch_state
            self.refresh_display(DisplayChange.BATCH_STATE)

    def ordered_simple_records(self) -> list[SimpleRecordInfo]:
        srs = list(self.batch_records.values())
        srs.sort(key=lambda x: (x.platform_id, x.timestamp))
        return srs

    def current_coordinates(self) -> t.Optional[tuple[float, float]]:
        if self.current_working_uuid is None:
            return None
        if self.batch_records is None:
            return None
        if self.current_working_uuid not in self.batch_records:
            return None
        info = self.batch_records[str(self.current_working_uuid)]
        if info.latitude is None or info.longitude is None:
            return None
        return info.latitude, info.longitude
