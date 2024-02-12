import functools

from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, BatchOpenState
import tkintermapview as tkmv
import typing as t
import cnodc.ocproc2.structures as ocproc2
import tkinter.ttk as ttk
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase


class MapPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._map_frame: t.Optional[ttk.Frame] = None
        self._map: t.Optional[tkmv.TkinterMapView] = None
        self._current_position = (45.41694, -75.70131)
        self._current_zoom = 10

    def on_init(self):
        self._map_frame = ttk.Frame(self.app.middle_left)
        self._map_frame.grid(row=0, column=0, sticky='NSEW')
        self._rebuild_map(600, 600)

    def _rebuild_map(self, width: int, height: int):
        if self._map is not None:
            self._current_position = self._map.get_position()
            self._current_zoom = int(self._map.last_zoom)
            self._map.delete_all_marker()
            self._map.destroy()
            self._map = None
        if width > 1 and height > 1:
            self._map = tkmv.TkinterMapView(self._map_frame, width=width, height=height, corner_radius=0)
            self._map.grid(row=0, column=0, sticky='NSEW')
            # TODO configurable map servers and starting configuration
            self._map.set_tile_server("https://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}&s=Ga", max_zoom=22)
            self._map.set_position(*self._current_position)
            self._map.set_zoom(self._current_zoom)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.SCREEN_SIZE:
            #self._rebuild_map(self._map_frame.winfo_width(), self._map_frame.winfo_height())
            # TODO: better way to resize map when window resizes?
            pass
        if self._map is not None and (change_type & (DisplayChange.BATCH | DisplayChange.SCREEN_SIZE)):
            self._map.delete_all_marker()
            if app_state.batch_state == BatchOpenState.OPEN:
                for sr in app_state.batch_record_info.values():
                    if sr.latitude is not None and sr.longitude is not None:
                        self._map.set_marker(
                            sr.latitude,
                            sr.longitude,
                            text=str(sr.index),
                            command=functools.partial(self._open_record, record_uuid=sr.record_uuid)
                        )
        if change_type & DisplayChange.RECORD:
            coordinates = app_state.current_coordinates()
            if coordinates is not None:
                self._map.set_position(*coordinates)
                self._map.set_zoom(10)

    def _open_record(self, marker, record_uuid: str):
        self.app.load_record(record_uuid)
