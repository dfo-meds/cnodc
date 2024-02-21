import functools
import pathlib
import tkinter

from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, BatchOpenState
import tkintermapview as tkmv
import typing as t
import cnodc.ocproc2 as ocproc2
import tkinter.ttk as ttk
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase
import PIL.Image as Image
import PIL.ImageTk as ImageTk


class MapPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._map_frame: t.Optional[ttk.Frame] = None
        self._map: t.Optional[tkmv.TkinterMapView] = None
        self._current_position = (45.41694, -75.70131)
        self._current_zoom = 10
        base_path = pathlib.Path(__file__).absolute().parent.parent / 'resources'
        self._error_image = ImageTk.PhotoImage(Image.open(str(base_path / 'red_dot.png')).resize((15, 15)))
        self._good_image = ImageTk.PhotoImage(Image.open(str(base_path / 'green_dot.png')).resize((15, 15)))

    def on_init(self):
        self._map_frame = ttk.Frame(self.app.middle_left, width=500, height=500)
        self._map_frame.grid(row=0, column=0, sticky='NSEW')
        self._rebuild_map(300, 300)

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
        if self._map is not None and (change_type & DisplayChange.BATCH):
            self._map.delete_all_marker()
            if app_state.batch_state == BatchOpenState.OPEN and app_state.batch_record_info:
                min_lat = None
                max_lat = None
                min_lon = None
                max_lon = None
                last_station = None
                station_path = []
                for sr in app_state.batch_record_info.values():
                    if last_station is not None and sr.station_id != last_station:
                        if len(station_path) > 1:
                            self._map.set_path(station_path, width=4, color='#666666')
                        station_path = []
                    last_station = sr.station_id
                    station_path.append((sr.latitude, sr.longitude))
                    if sr.latitude is not None and sr.longitude is not None:
                        if min_lat is None or sr.latitude < min_lat:
                            min_lat = sr.latitude
                        if max_lat is None or sr.latitude > max_lat:
                            max_lat = sr.latitude
                        if min_lon is None or sr.longitude < min_lon:
                            min_lon = sr.longitude
                        if max_lon is None or sr.longitude > max_lon:
                            max_lon = sr.longitude
                        self._map.set_marker(
                            sr.latitude,
                            sr.longitude,
                            text=str(sr.index),
                            icon=self._error_image if sr.has_errors else self._good_image,
                            command=functools.partial(self._open_record, record_uuid=sr.record_uuid),
                            text_color="#FFFFFF"
                        )
                if len(station_path) > 1:
                    self._map.set_path(station_path, width=4, color='#666666')
                if max_lat == min_lat and max_lon == min_lon:
                    self._map.set_position(max_lat, max_lon)
                else:
                    self._map.fit_bounding_box(
                        (max_lat, min_lon),
                        (min_lat, max_lon)
                    )
        if change_type & DisplayChange.RECORD:
            coordinates = app_state.current_coordinates()
            if coordinates is not None:
                self._map.set_position(*coordinates)
                self._map.set_zoom(10)

    def _open_record(self, marker, record_uuid: str):
        self.app.load_record(record_uuid)
