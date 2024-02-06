from __future__ import annotations
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation
import cnodc.desktop.translations as i18n


class StationPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_init(self):
        self.app.menus.add_command('qc/reload_stations', 'menu_reload_stations', self.reload_stations, True)
        self.app.menus.add_command('qc/next_station_failure', 'menu_next_station_failure', self.next_station_failure, True)

    def on_user_access_update(self, permissions: list[str]):
        if 'queue:station-failure' in permissions:
            self.app.menus.enable_command('qc/reload_stations')
            self.app.menus.enable_command('qc/next_station_failure')
        else:
            self.app.menus.disable_command('qc/reload_stations')
            self.app.menus.disable_command('qc/next_station_failure')

    def before_open_batch(self, batch_type: str):
        self.app.menus.disable_command('qc/next_station_failure')

    def after_close_batch(self, op: QCBatchCloseOperation, batch_type: str, load_next: bool, ex=None):
        if not load_next:
            self.app.menus.enable_command('qc/next_station_failure')

    def next_station_failure(self):
        self.app.menus.disable_command('qc/next_station_failure')
        self.app.open_qc_batch(
            'station',
            'cnodc.desktop.client.api_client.next_station_failure',
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

