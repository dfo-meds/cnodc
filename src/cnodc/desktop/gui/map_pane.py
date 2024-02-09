import functools

from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, BatchOpenState
import tkintermapview as tkmv
import typing as t
import cnodc.ocproc2.structures as ocproc2
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase


class MapPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._map: t.Optional[tkmv.TkinterMapView] = None

    def on_init(self):
        self._map = tkmv.TkinterMapView(self.app.middle_frame, width=500, height=500, corner_radius=0)
        self._map.grid(row=0, column=0)
        # TODO configurable map servers and starting configuration
        self._map.set_tile_server("https://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}&s=Ga", max_zoom=22)
        self._map.set_position(45.41694, -75.70131)
        self._map.set_zoom(10)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.BATCH:
            self._map.delete_all_marker()
            if app_state.batch_state == BatchOpenState.OPEN:
                for sr in app_state.batch_record_info.values():
                    if sr.latitude is not None and sr.longitude is not None:
                        self._map.set_marker(
                            sr.latitude,
                            sr.longitude,
                            text=sr.record_uuid,
                            command=functools.partial(self._open_record, record_uuid=sr.record_uuid)
                        )
        if change_type & DisplayChange.RECORD:
            coordinates = app_state.current_coordinates()
            if coordinates is not None:
                self._map.set_position(*coordinates)
                self._map.set_zoom(10)

    def _open_record(self, marker, record_uuid: str):
        self.app.load_record(record_uuid)
