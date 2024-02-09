from __future__ import annotations

import datetime
import typing as t
import enum

from cnodc.ocproc2.operations import QCOperator
import cnodc.ocproc2.structures as ocproc2

if t.TYPE_CHECKING:
    from cnodc.desktop.main_app import CNODCQCApp
    import cnodc.ocproc2.structures as ocproc2


class QCBatchCloseOperation(enum.Enum):

    COMPLETE = 'cnodc.desktop.client.api_client.complete_item'
    RELEASE = 'cnodc.desktop.client.api_client.release_item'
    FAIL = 'cnodc.desktop.client.api_client.fail_item'
    ESCALATE = 'cnodc.desktop.client.api_client.escalate_item'
    DESCALATE = 'cnodc.desktop.client.api_client.descalate_item'
    LOAD_ERROR = ''
    LOGOUT = ''


class BatchOpenState(enum.Enum):

    OPENING = 'O'
    OPEN = 'O2'
    OPEN_ERROR = 'OE'
    CLOSING = 'C'
    CLOSED = 'C2'
    CLOSE_ERROR = 'CE'


class DisplayChange(enum.Flag):

    USER = enum.auto()
    BATCH = enum.auto()
    RECORD = enum.auto()
    RECORD_CHILD = enum.auto()
    ACTION = enum.auto()
    OP_ONGOING = enum.auto()


class BatchType(enum.Enum):

    STATION = 'cnodc.desktop.client.api_client.next_station_failure'


class SimpleRecordInfo:

    def __init__(self,
                 rowid: int,
                 record_uuid: str,
                 lat: t.Optional[float],
                 lon: t.Optional[float],
                 ts: t.Optional[str],
                 has_errors: t.Optional[bool]):
        self.rowid = rowid
        self.record_uuid = record_uuid
        self.latitude = lat
        self.longitude = lon
        self.timestamp = datetime.datetime.fromisoformat(ts) if ts else None
        self.has_errors = has_errors


class ApplicationState:

    def __init__(self, display_callable: callable):
        self._display_cb = display_callable
        self.batch_state = None
        self.batch_type = None
        self.batch_actions = None
        self.batch_close_op = None
        self.batch_load_after_close = None
        self.record = None
        self.record_uuid = None
        self.subrecord_path = None
        self.child_record = None
        self.child_recordset = None
        self.actions = None
        self.save_in_progress = False
        self.username = None
        self.user_access = None
        self.batch_record_info: t.Optional[dict[str, SimpleRecordInfo]] = None

    def is_batch_action_available(self, action_name: str):
        if self.save_in_progress:
            return False
        if self.batch_state is None or self.batch_state != BatchOpenState.OPEN:
            return False
        return self.batch_actions is not None and action_name in self.batch_actions

    def start_batch_open(self, batch_type: BatchType):
        self.batch_type = batch_type
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

    def complete_batch_open(self, batch_actions: list[str], record_info: list[SimpleRecordInfo]):
        self.batch_actions = batch_actions
        self.batch_state = BatchOpenState.OPEN
        self.batch_record_info = {x.record_uuid: x for x in record_info}
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)

    def handle_batch_open_error(self):
        self.batch_state = BatchOpenState.OPEN_ERROR
        self.refresh_display(DisplayChange.BATCH | DisplayChange.OP_ONGOING)
        self.clear_batch()

    def clear_batch(self):
        if self.batch_type is not None:
            self.batch_state = None
            self.batch_type = None
            self.batch_actions = None
            self.batch_record_info = None
            self.batch_actions = None
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

    def update_user_info(self, username: str, access_list: list[str]) -> bool:
        if self.username != username or self.user_access != access_list:
            self.username = username
            self.user_access = access_list
            self.refresh_display(DisplayChange.USER)
            return True
        return False

    def set_save_flag(self, is_saving: bool):
        self.save_in_progress = is_saving
        self.refresh_display(DisplayChange.OP_ONGOING)

    def set_record_info(self, record_uuid: str, record: ocproc2.DataRecord, subrecord_path: t.Optional[str], actions):
        self.record = record
        self.record_uuid = record_uuid
        self.actions = actions
        self.subrecord_path = subrecord_path
        self._set_child_item()
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
        self.refresh_display(DisplayChange.ACTION | DisplayChange.RECORD_CHILD)

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


class BasePane:

    def __init__(self, app: CNODCQCApp):
        self.app = app

    def on_init(self):
        pass

    def on_language_change(self):
        pass

    def on_close(self):
        pass

    def before_save(self):
        pass

    def after_save(self, ex: Exception = None):
        pass

    def on_user_access_update(self, username: str, permissions: list[str]):
        pass

    def before_open_batch(self, batch_type: str):
        pass

    def after_open_batch(self, batch_type: str, available_actions: list[str]):
        pass

    def before_close_batch(self, op: QCBatchCloseOperation, batch_type: str, load_next: bool):
        pass

    def after_close_batch(self, op: QCBatchCloseOperation, batch_type: str, load_next: bool, ex=None):
        pass

    def show_recordset(self, record_set: ocproc2.RecordSet, path: str):
        pass

    def show_record(self, record: ocproc2.DataRecord, path: str):
        pass

    def on_record_change(self, record_uuid: str, record: ocproc2.DataRecord):
        pass

    def on_new_actions(self, actions: dict[int, QCOperator]):
        pass

    def on_reapply_actions(self, actions: dict[int, QCOperator]):
        pass

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        pass

