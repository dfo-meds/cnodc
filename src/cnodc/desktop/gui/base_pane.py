from __future__ import annotations

import datetime
import typing as t
import enum

from cnodc.ocproc2.operations import QCOperator

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


class BatchOpenState(enum.Enum):

    OPENING = 'O'
    OPEN = 'O2'
    OPEN_ERROR = 'OE'
    CLOSING = 'C'
    CLOSED = 'C2'
    CLOSE_ERROR = 'CE'


class ApplicationState:

    def __init__(self):
        self.batch_state = None
        self.batch_type = None
        self.batch_function = None
        self.batch_actions = None
        self.batch_closing_op = None
        self.record = None
        self.record_uuid = None
        self.subrecord_path = None
        self.child_record = None
        self.child_recordset = None
        self.actions = None
        self.save_in_progress = False
        self.username = None
        self.user_access = None
        self.batch_record_info: t.Optional[list[tuple[str, t.Optional[float], t.Optional[float], t.Optional[datetime.datetime]]]]

    def is_batch_action_available(self, action_name: str):
        if self.save_in_progress:
            return False
        if self.batch_state is None or self.batch_state != BatchOpenState.OPEN:
            return False
        return self.batch_actions is not None and action_name in self.batch_actions


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

    def refresh_display(self, app_state: ApplicationState):
        pass

