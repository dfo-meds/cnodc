from __future__ import annotations
from cnodc_app.gui.base_pane import BasePane
import typing as t
import tkinter as tk
import cnodc_app.translations as i18n


class StationPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_init(self):
        self.app.menus.add_command('qc/reload_stations', 'menu_reload_stations', self.reload_stations, True)
        self.app.menus.add_command('qc/next_station_failure', 'menu_next_station_failure', self.load_station_failure, True)

    def on_user_access_update(self, permissions: list[str]):
        if 'queue:station-failure' in permissions:
            self.app.menus.enable_command('qc/reload_stations')
            self.app.menus.enable_command('qc/next_station_failure')
        else:
            self.app.menus.disable_command('qc/reload_stations')
            self.app.menus.disable_command('qc/next_station_failure')

    def load_station_failure(self):
        print('there')

    def reload_stations(self):
        self.app.menus.disable_command('qc/reload_stations')
        self.app.dispatcher.submit_job(
            'cnodc_app.client.api_client.reload_stations',
            on_success=self._reload_success,
            on_error=self._reload_error
        )

    def _reload_success(self, res):
        self.app.menus.enable_command('qc/reload_stations')

    def _reload_error(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('qc/reload_stations')

