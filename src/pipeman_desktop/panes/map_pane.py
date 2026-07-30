import functools
import pathlib

import gcapp.i18n as i18n

from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.util import BatchOpenState
from pipeman_desktop.state import DisplayChange, ApplicationState
import tkintermapview as tkmv
import typing as t
import tkinter.ttk as ttk
from autoinject import injector
from pipeman_desktop.client.local_db import LocalDatabase
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
        self._current_zoom = 18
        base_path = pathlib.Path(__file__).absolute().parent.parent / 'resources'
        self._error_image = ImageTk.PhotoImage(Image.open(str(base_path / 'red_dot.png')).resize((15, 15)))
        self._good_image = ImageTk.PhotoImage(Image.open(str(base_path / 'green_dot.png')).resize((15, 15)))
        self._pane_id: str | None = None

    def on_init(self):
        self._map_frame = ttk.Frame(self.app.middle)
        self.app.middle.add(self._map_frame, text=i18n.tr("pane_map"), sticky="NSEW")
        self._pane_id = self.app.middle.tabs()[-1]
        width = self.app.root.winfo_screenwidth() / 2.5
        self._rebuild_map(width, width)

    def _rebuild_map(self, width: int, height: int):
        if self._map is not None:
            self._current_position = self._map.get_position()
            self._current_zoom = int(self._map.last_zoom)
            self._map.delete_all_marker()
            self._map.destroy()
            self._map = None
        if width > 1 and height > 1:
            # 25 workers seems to set off Google's rate limiting
            # 2-3 works fine.
            self._map = tkmv.TkinterMapView(
                self._map_frame,
                width=width,
                height=height,
                corner_radius=0,
                background_load_workers=3,
                cache_tile_radius=2,)
            self._map.grid(row=0, column=0, sticky='NSEW')
            self._map.set_tile_server("https://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}&s=Ga", max_zoom=22)
            self._map.set_zoom(self._current_zoom)
            self._map.set_position(*self._current_position)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.SCREEN_SIZE:
            if self._map is not None:
                self._rebuild_map(self.app.middle.winfo_width(), self.app.middle.winfo_height())
        if self._map is not None and (change_type & DisplayChange.BATCH_STATE):
            if self._map is not None:
                self._rebuild_markers()
        if change_type & DisplayChange.RECORD:
            if self._map is not None:
                self._update_map_position()
        if change_type & DisplayChange.LANGUAGE:
            if self._pane_id is not None:
                self.app.middle.tab(self._pane_id, text=i18n.tr("pane_map"))

    def _update_map_position(self):
        coordinates = self.app.state.current_coordinates()
        if coordinates is not None:
            self._map.set_position(*coordinates)
            self._map.set_zoom(10)

    def _rebuild_markers(self):
        self._map.delete_all_marker()
        if self.app.state.batch_state == BatchOpenState.OPEN and self.app.state.batch_records:
            min_lat = None
            max_lat = None
            min_lon = None
            max_lon = None
            last_station = None
            station_path = []
            for sr in self.app.state.batch_records.values():
                if last_station is not None and sr.platform_id != last_station:
                    if len(station_path) > 1:
                        self._map.set_path(station_path, width=4, color='#666666')
                    station_path = []
                last_station = sr.platform_id
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
            elif min_lon is not None and max_lon is not None and min_lat is not None and max_lat is not None:
                self._map.fit_bounding_box(
                    (max_lat, min_lon),
                    (min_lat, max_lon)
                )

    def _open_record(self, marker, record_uuid: str):
        self.app.load_record(record_uuid)
