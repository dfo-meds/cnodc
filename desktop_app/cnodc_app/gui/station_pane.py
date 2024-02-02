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

    def on_user_access_update(self, permissions: list[str]):
        if 'queue:station-failure' in permissions:
            self.app.menus.enable_command('qc/reload_stations')
        else:
            self.app.menus.disable_command('qc/reload_stations')

    def reload_stations(self):
        print('here')
