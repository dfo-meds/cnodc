from __future__ import annotations
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, BatchType
import cnodc.desktop.translations as i18n


class StationPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_init(self):
        self.app.menus.add_command('qc/reload_stations', 'menu_reload_stations', self.reload_stations, True)
        self.app.menus.add_command('qc/next_station_failure', 'menu_next_station_failure', self.next_station_failure, True)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.USER:
            has_station_access = app_state.user_access and 'queue:station-failure' in app_state.user_access
            self.app.menus.set_state('qc/reload_stations', has_station_access)
            self.app.menus.set_state('qc/next_station_failure', has_station_access)
        elif change_type & DisplayChange.BATCH:
            self.app.menus.set_state('qc/next_station_failure', app_state.batch_type is None)

    def next_station_failure(self):
        self.app.menus.disable_command('qc/next_station_failure')
        self.app.open_qc_batch(
            BatchType.STATION,
        )

    def reload_stations(self):
        self.app.menus.disable_command('qc/reload_stations')
        self.app.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.reload_stations',
            on_success=self._reload_success,
            on_error=self._reload_error
        )

    def _reload_success(self, res):
        self.app.menus.enable_command('qc/reload_stations')

    def _reload_error(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('qc/reload_stations')

