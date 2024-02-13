from __future__ import annotations

import datetime

from cnodc.desktop.gui.base_pane import BasePane, ApplicationState, DisplayChange, BatchType
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import cnodc.desktop.translations as i18n
import tkinter.simpledialog as tksd
import tkinter as tk
import typing as t
import tkinter.ttk as ttk
from autoinject import injector


if t.TYPE_CHECKING:
    import cnodc.desktop.client.local_db.LocalDatabase


class StationCreationDialog(tksd.Dialog):

    def __init__(self, parent):
        self.wmo_id = tk.StringVar(parent)
        self.wigos_id = tk.StringVar(parent)
        self.station_id = tk.StringVar(parent)
        self.station_name = tk.StringVar(parent)
        self.start_date = tk.StringVar(parent)
        self.end_date = tk.StringVar(parent)
        self.keep_external_qc = tk.IntVar(parent)
        self.require_review = tk.IntVar(parent)
        self.skip_bathymetry = tk.IntVar(parent)
        self.skip_speed = tk.IntVar(parent)
        self.top_speed = tk.StringVar()
        self.embargo_days = tk.StringVar()
        self._units_box = None
        super().__init__(parent=parent, title=i18n.get_text('station_creation_title'))

    def body(self, parent):
        ttk.Label(parent, text=i18n.get_text('station_wmo_id')).grid(row=0, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.wmo_id).grid(row=0, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_wigos_id')).grid(row=1, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.wigos_id).grid(row=1, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_id')).grid(row=2, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.station_id).grid(row=2, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_name')).grid(row=3, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.station_name).grid(row=3, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_start_date')).grid(row=4, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.start_date).grid(row=4, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_end_date')).grid(row=5, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.end_date).grid(row=5, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        # TODO instrumentation
        # TODO map to uuid
        # TODO status
        # TODO station type
        ttk.Label(parent, text=i18n.get_text('station_embargo_days')).grid(row=6, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.embargo_days).grid(row=6, column=1, sticky='EW', columnspan=2, padx=2, pady=2)
        ttk.Label(parent, text=i18n.get_text('station_top_speed')).grid(row=7, column=0, sticky='W', padx=2, pady=2)
        ttk.Entry(parent, textvariable=self.top_speed).grid(row=7, column=1, sticky='EW', padx=2, pady=2)
        self._units_box = ttk.Combobox(parent, values=['m s-1', 'knot', 'km h-1'])
        self._units_box.current(0)
        self._units_box.grid(row=7, column=2, sticky='EW', padx=2, pady=2)
        ttk.Checkbutton(parent, text=i18n.get_text('station_keep_external_qc'), onvalue=1, offvalue=0, variable=self.keep_external_qc).grid(row=8, column=1, sticky='W', columnspan=2, padx=2, pady=2)
        ttk.Checkbutton(parent, text=i18n.get_text('station_require_review'), onvalue=1, offvalue=0, variable=self.require_review).grid(row=9, column=1, sticky='W', columnspan=2, padx=2, pady=2)
        ttk.Checkbutton(parent, text=i18n.get_text('station_skip_bathymetry'), onvalue=1, offvalue=0, variable=self.skip_bathymetry).grid(row=10, column=1, sticky='W', columnspan=2, padx=2, pady=2)
        ttk.Checkbutton(parent, text=i18n.get_text('station_skip_speed'), onvalue=1, offvalue=0, variable=self.skip_speed).grid(row=11, column=1, sticky='W', columnspan=2, padx=2, pady=2)
        # other metadata?

    def validate(self):
        station_data = {
            'wmo_id': self.wmo_id.get(),
            'wigos_id': self.wigos_id.get(),
            'station_id': self.station_id.get(),
            'station_name': self.station_name.get(),
            'service_start_date': self.start_date.get(),
            'service_end_date': self.end_date.get(),
            'instrumentation': [],
            'embargo_data_days': self.embargo_days.get(),
            'metadata': {
                'keep_external_qc': self.keep_external_qc.get() == 1,
                'require_review': self.require_review.get() == 1,
                'skip_bathymetry_check': self.skip_bathymetry.get() == 1,
                'skip_speed_check': self.skip_speed.get() == 1,
            }
        }
        check = 1
        # TODO: flagging fields with class to get a red box
        if self.top_speed.get() != '':
            if self._units_box.current() > -1:
                station_data['metadata']['top_speed'] = f'{self.top_speed.get()} {self._units_box["values"][self._units_box.current()]}'
            else:
                check = 0
        try:
            if station_data['service_start_date']:
                dt = datetime.datetime.fromisoformat(station_data['service_start_date'])
        except (ValueError, TypeError):
            check = 0
        try:
            if station_data['service_end_date']:
                dt = datetime.datetime.fromisoformat(station_data['service_end_date'])
        except (ValueError, TypeError):
            check = 0
        try:
            station_data['embargo_data_days'] = int(station_data['embargo_data_days']) if station_data['embargo_data_days'] else None
        except (ValueError, TypeError):
            check = 0
        if station_data['wmo_id']:
            if not all(x.isdigit() for x in station_data['wmo_id']):
                check = 0
            if len(station_data['wmo_id']) not in (5, 7):
                check = 0
        if len(station_data['wigos_id']) > 126:
            check = 0
        if len(station_data['station_name']) > 126:
            check = 0
        if len(station_data['station_id']) > 126:
            check = 0
        if all(station_data[x] == '' for x in ('wmo_id', 'wigos_id', 'station_id', 'station_name')):
            check = 0
        self.result = station_data if check == 1 else None
        return check


class StationPane(BasePane):

    local_db: cnodc.desktop.client.local_db.LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._station_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        self.app.menus.add_command('qc/reload_stations', 'menu_reload_stations', self.reload_stations, True)
        self.app.menus.add_command('qc/next_station_failure', 'menu_next_station_failure', self.next_station_failure, True)
        self.app.menus.add_command('qc/create_station', 'menu_create_station', self.create_station, True)
        station_frame = ttk.Frame(self.app.bottom_notebook)
        station_frame.rowconfigure(0, weight=1)
        station_frame.columnconfigure(0, weight=1)
        self._station_list = ScrollableTreeview(
            parent=station_frame,
            selectmode="browse",
            show="headings",
            headers=[
                i18n.get_text('station_uuid'),
                i18n.get_text('station_wmo_id'),
                i18n.get_text('station_wigos_id'),
                i18n.get_text('station_name'),
                i18n.get_text('station_id'),
                i18n.get_text('station_start_date'),
                i18n.get_text('station_end_date')
            ],
            displaycolumns=(0, 1, 2, 3, 4, 5, 6)
        )
        self._station_list.grid(row=0, column=0, sticky='NSEW')
        self.app.bottom_notebook.add(station_frame, text=i18n.get_text('station_list'), sticky='NSEW')

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.USER:
            has_station_access = app_state.user_access and 'queue:station-failure' in app_state.user_access
            self.app.menus.set_state('qc/reload_stations', has_station_access)
            self.app.menus.set_state('qc/next_station_failure', has_station_access)
            self.app.menus.set_state('qc/create_station', has_station_access)
            self._update_station_list()
        elif change_type & DisplayChange.BATCH:
            self.app.menus.set_state('qc/next_station_failure', app_state.batch_type is None)

    def create_station(self):
        s = StationCreationDialog(self.app.root)
        if s.result is not None:
            self.app.menus.set_state('qc/create_station', False)
            self.app.dispatcher.submit_job(
                'cnodc.desktop.client.api_client.create_station',
                job_kwargs={
                    'station_def': s.result,
                },
                on_success=self._on_station_creation,
                on_error=self._on_station_creation_fail
            )

    def _update_station_list(self):
        self._station_list.clear_items()
        with self.local_db.cursor() as cur:
            cur.execute("""
                SELECT station_uuid, wmo_id, wigos_id, station_name, station_id, service_start_date, service_end_date
                FROM stations
            """)
            for row in cur.fetchall():
                self._station_list.table.insert(
                    parent='',
                    index='end',
                    values=row,
                    text=''
                )

    def _on_station_creation(self, res):
        self._update_station_list()
        self.app.menus.set_state('qc/create_station', True)

    def _on_station_creation_fail(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.set_state('qc/create_station', True)

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
        self._update_station_list()
        self.app.menus.enable_command('qc/reload_stations')

    def _reload_error(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('qc/reload_stations')

