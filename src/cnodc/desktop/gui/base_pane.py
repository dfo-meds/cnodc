from __future__ import annotations

import datetime
import typing as t
import enum

from cnodc.ocproc2.operations import QCOperator
import cnodc.ocproc2.structures as ocproc2
import cnodc.desktop.translations as i18n


if t.TYPE_CHECKING:
    from cnodc.desktop.main_app import CNODCQCApp
    import cnodc.ocproc2.structures as ocproc2


class QCBatchCloseOperation(enum.Enum):

    COMPLETE = 'cnodc.desktop.client.api_client.complete_item'
    RELEASE = 'cnodc.desktop.client.api_client.release_item'
    FAIL = 'cnodc.desktop.client.api_client.fail_item'
    ESCALATE = 'cnodc.desktop.client.api_client.escalate_item'
    DESCALATE = 'cnodc.desktop.client.api_client.descalate_item'
    LOAD_ERROR = 'E'
    LOGOUT = 'L'
    FORCE_CLOSE = 'F'


class BatchOpenState(enum.Enum):

    OPENING = 'O'
    OPEN = 'O2'
    OPEN_ERROR = 'OE'
    CLOSING = 'C'
    CLOSED = 'C2'
    CLOSE_ERROR = 'CE'


class CloseBatchResult(enum.Enum):

    CLOSING = 'C'
    ALREADY_CLOSED = 'L'
    CANCELLED = 'N'


class DisplayChange(enum.Flag):

    USER = enum.auto()
    BATCH = enum.auto()
    RECORD = enum.auto()
    RECORD_CHILD = enum.auto()
    ACTION = enum.auto()
    OP_ONGOING = enum.auto()
    SCREEN_SIZE = enum.auto()


class BatchType(enum.Enum):

    STATION = 'cnodc.desktop.client.api_client.next_station_failure'


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
                 station_id: t.Optional[str] = None):
        self.index = idx
        self.rowid = rowid
        self.record_uuid = record_uuid
        self.latitude = lat
        self.longitude = lon
        self.timestamp = datetime.datetime.fromisoformat(ts) if ts else None
        self.has_errors = has_errors
        self.station_id = station_id
        self.latitude_qc = lat_qc or 0
        self.longitude_qc = lon_qc or 0
        self.time_qc = time_qc or 0


class ApplicationState:

    def __init__(self, display_callable: callable):
        self._display_cb = display_callable
        self.batch_state: t.Optional[BatchOpenState] = None
        self.batch_service_name: t.Optional[str] = None
        self.batch_actions: t.Optional[list[str]] = None
        self.batch_close_op: t.Optional[QCBatchCloseOperation] = None
        self.batch_load_after_close = None
        self.batch_test_names: t.Optional[list[str]] = None
        self.record: t.Optional[ocproc2.DataRecord] = None
        self.record_uuid: t.Optional[str] = None
        self.subrecord_path: t.Optional[str] = None
        self.child_record: t.Optional[ocproc2.DataRecord] = None
        self.child_recordset: t.Optional[ocproc2.RecordSet] = None
        self.actions: t.Optional[list[QCOperator]] = None
        self.save_in_progress: bool = False
        self.username: t.Optional[str] = None
        self.has_unsaved_changes: bool = False
        self.user_access: t.Optional[dict[str, dict[str, str]]] = None
        self.batch_record_info: t.Optional[dict[str, SimpleRecordInfo]] = None

    def can_logout(self):
        if self.username is None:
            return False
        if self.save_in_progress:
            return False
        return self.batch_state is None or self.batch_state == BatchOpenState.OPEN

    def has_access(self, access_name: str) -> bool:
        return self.user_access is not None and access_name in self.user_access

    def ordered_simple_records(self) -> list[SimpleRecordInfo]:
        srs = list(self.batch_record_info.values())
        srs.sort(key=lambda x: (x.station_id, x.timestamp))
        return srs

    def can_open_new_queue_item(self) -> bool:
        if self.user_access is None:
            return False
        if self.batch_state is not None:
            return False
        if not any(x.startswith('service_queues:') for x in self.user_access.keys()):
            return False
        return True

    def service_choices(self, language: str = None) -> dict[str, str]:
        if language is None:
            language = i18n.current_language()
        results = {}
        for key in self.user_access.keys():
            if key.startswith('service_queues:'):
                results[key] = self.user_access[key][language] if language in self.user_access[key] else key
        return results

    def is_batch_action_available(self, action_name: str):
        if self.save_in_progress:
            return False
        if self.batch_state is None or self.batch_state != BatchOpenState.OPEN:
            return False
        return self.batch_actions is not None and action_name in self.batch_actions

    def start_batch_open(self, batch_service_name: str):
        self.batch_service_name = batch_service_name
        self.batch_state = BatchOpenState.OPENING
        self.batch_actions = None
        self.batch_close_op = None
        self.batch_load_after_close = None
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def current_coordinates(self) -> t.Optional[tuple[float, float]]:
        if self.record_uuid is None:
            return None
        if self.batch_record_info is None:
            return None
        if self.record_uuid not in self.batch_record_info:
            return None
        info = self.batch_record_info[self.record_uuid]
        if info.latitude is None or info.longitude is None:
            return None
        return info.latitude, info.longitude

    def complete_batch_open(self, batch_actions: list[str], batch_test_names: list[str], record_info: list[SimpleRecordInfo]):
        self.batch_actions = batch_actions
        self.batch_state = BatchOpenState.OPEN
        self.batch_test_names = batch_test_names
        self.has_unsaved_changes = False
        self.batch_record_info = {x.record_uuid: x for x in record_info} if record_info else {}
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def handle_batch_open_error(self):
        self.batch_state = BatchOpenState.OPEN_ERROR
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)
        self.clear_batch()

    def clear_batch(self):
        if self.batch_service_name is not None:
            self.batch_state = None
            self.batch_service_name = None
            self.batch_actions = None
            self.batch_record_info = None
            self.has_unsaved_changes = False
            self.batch_actions = None
            self.batch_test_names = None
            self.batch_close_op = None
            self.batch_load_after_close = None
            self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def start_batch_close(self, op: QCBatchCloseOperation, load_next: bool):
        self.batch_close_op = op
        self.batch_load_after_close = load_next
        self.batch_state = BatchOpenState.CLOSING
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def complete_batch_close(self):
        self.batch_state = BatchOpenState.CLOSED
        self.batch_actions = []
        self.record = None
        self.has_unsaved_changes = False
        self.record_uuid = None
        self.child_record = None
        self.child_recordset = None
        self.subrecord_path = None
        self.refresh_display(DisplayChange.BATCH | DisplayChange.RECORD | DisplayChange.RECORD_CHILD | DisplayChange.ACTION | DisplayChange.OP_ONGOING)

    def handle_batch_close_error(self):
        self.batch_state = BatchOpenState.CLOSE_ERROR
        self.refresh_display(DisplayChange.BATCH)
        self.batch_state = BatchOpenState.OPEN
        self.batch_close_op = None
        self.batch_load_after_close = None
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def update_user_info(self, username: str, access_list: dict[str, dict[str, str]]) -> bool:
        if self.username != username or self.user_access != access_list:
            self.username = username
            self.user_access = access_list
            self.refresh_display(DisplayChange.USER)
            return True
        return False

    def set_save_flag(self, is_saving: bool, has_unsaved_changes: t.Optional[bool] = None):
        self.save_in_progress = is_saving
        if has_unsaved_changes is not None:
            self.has_unsaved_changes = has_unsaved_changes
        self.refresh_display(DisplayChange.OP_ONGOING)

    def set_record_info(self, record_uuid: str, record: ocproc2.DataRecord, subrecord_path: t.Optional[str], actions):
        self.record = record
        self.record_uuid = record_uuid
        self.actions = actions
        self.subrecord_path = subrecord_path
        self._set_child_item()
        self._update_batch_info_from_current_record()
        self.refresh_display(DisplayChange.ACTION | DisplayChange.RECORD | DisplayChange.RECORD_CHILD)

    def set_record_subpath(self, subpath: t.Optional[str]):
        self.subrecord_path = subpath
        self._set_child_item()
        self.refresh_display(DisplayChange.RECORD_CHILD)

    def extend_actions(self, actions: dict[int, QCOperator]):
        if self.actions is None:
            self.actions = actions
        else:
            self.actions.update(actions)
        for action in actions.values():
            action.apply(self.record, None)
        self.has_unsaved_changes = True
        mode = DisplayChange.ACTION | DisplayChange.RECORD_CHILD
        if self._update_batch_info_from_current_record():
            mode |= DisplayChange.RECORD
        self.refresh_display(mode)

    def _set_child_item(self):
        if self.record_uuid is not None and self.subrecord_path is not None:
            child = self.record.find_child(self.subrecord_path)
            if isinstance(child, ocproc2.DataRecord):
                self.child_record = child
                self.child_recordset = None
            elif isinstance(child, ocproc2.RecordSet):
                self.child_record = None
                self.child_recordset = child
            else:
                self.child_recordset = None
                self.child_record = None
                self.subrecord_path = None
        else:
            self.child_record = None
            self.child_recordset = None

    def refresh_display(self, change_type: DisplayChange, *args, **kwargs):
        self._display_cb(self, change_type)

    def _update_batch_info_from_current_record(self) -> bool:
        # TODO:
        # return true if the record was updated
        # check latitude, longitude, time, and if there are any errors
        pass


class BasePane:

    def __init__(self, app: CNODCQCApp):
        self.app = app

    def on_init(self):
        pass

    def on_language_change(self, language: str):
        pass

    def on_close(self):
        pass

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        pass

