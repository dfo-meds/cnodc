from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation
import tkintermapview as tkmv
import typing as t
import cnodc.ocproc2.structures as ocproc2
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase


class ToolPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._map: t.Optional[tkmv.TkinterMapView] = None

    def on_init(self):
        self._map = tkmv.TkinterMapView(self.app.middle_frame, width=500, height=500, corner_radius=0)
        self._map.grid(row=0, column=0)
        self._map.set_tile_server("https://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}&s=Ga", max_zoom=22)
        self._map.set_position(48.0123002, -62.0119521)
        self._map.set_zoom(8)

    def after_open_batch(self, batch_type: str):
        with self.local_db.cursor() as cur:
            cur.execute("SELECT record_uuid, lat, lon, datetime FROM records")
            for record_uuid, lat, lon, ts in cur.fetchall():
                if lat is not None and lon is not None:
                    self._map.set_marker(lat, lon, text=record_uuid)

    def after_close_batch(self, op: QCBatchCloseOperation, batch_type: str, load_next: bool, ex=None):
        self._map.delete_all_marker()

    def on_record_change(self, record_uuid: str, record: ocproc2.DataRecord):
        try:
            if not record.coordinates.has_value('Latitude'):
                return
            if not record.coordinates.has_value('Longitude'):
                return
            lat = record.coordinates['Latitude'].to_float()
            lon = record.coordinates['Longitude'].to_float()
            self._map.set_position(lat, lon)
            self._map.set_zoom(8)
        except (ValueError, TypeError):
            pass




