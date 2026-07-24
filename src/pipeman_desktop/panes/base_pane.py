import typing as t

from pipeman_desktop.util import DisplayChange, ApplicationState

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop


class BasePane:

    def __init__(self, app: PipemanDesktop):
        self.app = app

    def on_init(self):
        pass

    def on_language_change(self):
        pass

    def on_close(self):
        pass

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        pass

