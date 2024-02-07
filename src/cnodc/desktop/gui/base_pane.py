from __future__ import annotations
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
    LOAD_ERROR = ''


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

    def after_open_batch(self, batch_type: str):
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

