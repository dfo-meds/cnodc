from __future__ import annotations
import typing as t

if t.TYPE_CHECKING:
    from cnodc_app.main_app import CNODCQCApp


class BasePane:

    def __init__(self, app: CNODCQCApp):
        self.app = app

    def on_init(self):
        pass

    def on_language_change(self):
        pass

    def on_close(self):
        pass

    def on_user_access_update(self, permissions: list[str]):
        pass

    def on_open_qc_batch(self, batch_type: str):
        pass

    def on_close_qc_batch(self, batch_type: str, load_next: bool):
        pass
