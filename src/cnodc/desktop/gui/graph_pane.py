import functools
from matplotlib.lines import Line2D
from cnodc.desktop.gui.base_pane import BasePane, ApplicationState, DisplayChange, BatchOpenState, SimpleRecordInfo
import typing as t
import cnodc.ocproc2 as ocproc2
import tkinter.ttk as ttk
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase
import matplotlib.axes as mpla
import matplotlib.figure as mplf
import matplotlib.backends.backend_tkagg as mpltk
import matplotlib.backend_bases as mplbb
import tkinter as tk
import cnodc.desktop.translations as i18n
from cnodc.desktop.gui.ocproc_graph import OCProc2Graph
from cnodc.ocean_math.geodesy import uhaversine
from cnodc.units import UnitConverter
import cnodc.ocean_math.ocproc2int as oom


class GraphPane(BasePane):

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._oc2graph: t.Optional[OCProc2Graph] = None

    def on_init(self):
        self._oc2graph = OCProc2Graph(self.app.middle_right, self.app)
        self._oc2graph.grid(row=0, column=0, sticky='NSEW')

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD | DisplayChange.BATCH):
            self._oc2graph.clear_graph_data()
            if app_state.batch_state == BatchOpenState.OPEN:
                self._oc2graph.update_graph_options()
        elif change_type & DisplayChange.ACTION:
            if app_state.batch_state == BatchOpenState.OPEN:
                self._oc2graph.update_graph_data()
            else:
                self._oc2graph.clear_graph_data()

    def on_language_change(self, language: str):
        pass


class _GraphPane(BasePane):

    local_db: LocalDatabase = None
    converter: UnitConverter = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._chart_frame: t.Optional[ttk.Frame] = None
        self._figure: t.Optional[mplf.Figure] = None
        self._canvas: t.Optional[mpltk.FigureCanvasTkAgg] = None
        self._axes: list[mpla.Axes] = []
        self._axes2: t.Optional[mpla.Axes] = None
        self._current_coordinate: t.Optional[str] = None
        self._current_parameter: t.Optional[str] = None
        self._current_recordset_id: t.Optional[str] = None
        self._current_recordset: t.Optional[ocproc2.RecordSet] = None
        self._combo_recordset: t.Optional[ttk.Combobox] = None
        self._combo_independent: t.Optional[ttk.Combobox] = None
        self._combo_dependent: t.Optional[ttk.Combobox] = None

    def on_init(self):
        self._chart_frame = ttk.Frame(self.app.middle_right, width=600, height=600)
        self._chart_frame.grid(row=0, column=0, sticky='NSEW')
        self._chart_frame.rowconfigure(0, weight=1)
        self._chart_frame.columnconfigure(0, weight=1)
        self._figure = mplf.Figure(dpi=100, figsize=(1, 1))
        self._canvas = mpltk.FigureCanvasTkAgg(self._figure, master=self._chart_frame)
        self._canvas.draw()
        self._canvas.mpl_connect('button_release_event', self._on_click)
        self._canvas.get_tk_widget().grid(row=0, column=0, sticky='NSEW')
        option_frame = ttk.Frame(self._chart_frame)
        option_frame.grid(row=1, column=0)
        option_frame.columnconfigure(0, weight=1)
        option_frame.columnconfigure(0, weight=2)
        option_frame.columnconfigure(0, weight=3)
        self._combo_recordset = ttk.Combobox(option_frame)
        self._combo_recordset.grid(row=0, column=0, padx=5, pady=5)
        self._combo_recordset.bind('<<ComboboxSelected>>', self._rs_change)
        self._combo_dependent = ttk.Combobox(option_frame)
        self._combo_dependent.grid(row=0, column=2, padx=5, pady=5)
        self._combo_dependent.bind('<<ComboboxSelected>>', self._var_change)
        self._combo_independent = ttk.Combobox(option_frame)
        self._combo_independent.grid(row=0, column=1, padx=5, pady=5)
        self._combo_independent.bind('<<ComboboxSelected>>', self._var_change)

    def on_language_change(self, language: str):
        # TODO: labels for table
        # TODO: if possible, drop-down list elements
        pass

    def _on_click(self, event: mplbb.MouseEvent):
        if event.ydata is None or event.button != 3 or self._current_coordinate is None or self._current_parameter is None:
            return
        if self._current_recordset is None:
            # TODO: check for BATCH selected and  give lat/long/time flagging options?
            return
        if self._current_coordinate in ('Depth', 'Pressure'):
            coordinate_value = round(event.ydata * -1, 4)
            flag_right_key = 'flag_at_and_below'
        else:
            coordinate_value = round(event.xdata, 4)
            flag_right_key = 'flag_at_and_right'
        menu = tk.Menu(self.app.root, tearoff=0)
        menu.add_command(
            label=coordinate_value
        )
        menu.add_command(
            label=i18n.get_text(flag_right_key, coordinate=str(coordinate_value), flag=str(4)),
            command=functools.partial(self._flag_right, coordinate_value=coordinate_value, flag=4)
        )
        menu.add_command(
            label=i18n.get_text(flag_right_key, coordinate=str(coordinate_value), flag=str(3)),
            command=functools.partial(self._flag_right, coordinate_value=coordinate_value, flag=3)
        )
        widget_x = self._canvas.get_tk_widget().winfo_rootx() + event.x
        widget_y = self._canvas.get_tk_widget().winfo_rooty() + (self._canvas.get_tk_widget().winfo_height() - event.y)
        menu.tk_popup(widget_x, widget_y, 0)

    def _flag_right(self, coordinate_value, flag):
        actions = []
        srt, rs_idx = self._combo_recordset['values'][self._combo_recordset.current()].split('#', maxsplit=1)
        rs_path = f'subrecords/{srt}/{rs_idx}'
        for idx, r in enumerate(self._current_recordset.records):
            if not r.coordinates.has_value(self._current_coordinate):
                continue
            if not r.parameters.has_value(self._current_parameter):
                continue
            if r.coordinates[self._current_coordinate].to_float() < coordinate_value:
                continue
            cwq = r.parameters[self._current_parameter].metadata.best_value('WorkingQuality', 0)
            if cwq == flag:
                continue
            if cwq == 9:
                continue
            if flag == 3 and cwq == 4:
                continue
            actions.append(self.app.create_flag_operator(
                f"{rs_path}/{idx}/parameters/{self._current_parameter}",
                flag
            ))
        self.app.save_operations(actions)

    def _update_graph(self):
        p = self._current_combobox_value(self._combo_dependent)
        c = self._current_combobox_value(self._combo_independent)
        if p != self._current_parameter or c != self._current_coordinate:
            if p is None or c is None:
                self._clear_graph()
            else:
                if self._current_recordset_id == 'Batch':
                    self._show_batch_graph(self.app.app_state.batch_record_info, c, p)
                else:
                    self._show_graph(
                        self._current_recordset,
                        c,
                        p
                    )
                self._current_parameter = p
                self._current_coordinate = c

    def _clear_graph(self):
        if self._axes is not None:
            self._figure.delaxes(self._axes)
            self._axes = None
            self._current_coordinate = None
            self._current_parameter = None

    def _show_batch_graph(self, batch_info, x_name: str, y_name: str):
        self._clear_graph()
        unit_map = {}
        batch_values = [x for x in batch_info.values()]
        if y_name == 'Speed':
            x_qc_values = [self._extract_diff_batch_value(batch_values[i-1], batch_values[i], x_name, unit_map) for i in range(1, len(batch_values))]
            y_qc_values = [self._extract_diff_batch_value(batch_values[i-1], batch_values[i], y_name, unit_map) for i in range(1, len(batch_values))]
        else:
            x_qc_values = [self._extract_batch_value(batch_values[i], x_name, unit_map) for i in range(0, len(batch_values))]
            y_qc_values = [self._extract_batch_value(batch_values[i], y_name, unit_map) for i in range(0, len(batch_values))]
        self._show_qc_graph(
            x_qc_values,
            y_qc_values,
            x_name,
            y_name,
            unit_map
        )

    def _extract_batch_value(self, batch_info: SimpleRecordInfo,  key: str, unit_map: dict) -> tuple[t.Optional[float], int]:
        # TODO: store QC values in simple record info too
        if key == 'Time':
            return batch_info.timestamp, batch_info.time_qc
        elif key == 'Latitude':
            return batch_info.latitude, batch_info.latitude_qc
        elif key == 'Longitude':
            return batch_info.longitude, batch_info.longitude_qc
        elif key == 'Index':
            return batch_info.index, 1
        return None, 9

    def _extract_diff_batch_value(self, batch_info_a: SimpleRecordInfo, batch_info_b: SimpleRecordInfo, key: str, unit_map: dict) -> tuple[t.Optional[float], int]:
        if key == 'Speed':
            # TODO: look at QC values in simple record info after we add them
            if batch_info_a.latitude is None or batch_info_b.latitude is None:
                return None, 9
            if batch_info_a.longitude is None or batch_info_b.longitude is None:
                return None, 9
            if batch_info_a.timestamp is None or batch_info_b.timestamp is None:
                return None, 9
            qc = 0
            if batch_info_a.latitude_qc == 4 or batch_info_b.latitude_qc == 4 or batch_info_a.longitude_qc == 4 or batch_info_b.longitude_qc == 4:
                qc = 4
            elif batch_info_a.latitude_qc == 3 or batch_info_b.latitude_qc == 3 or batch_info_a.longitude_qc == 3 or batch_info_b.longitude_qc == 3:
                qc = 3
            elif batch_info_a.latitude_qc == 2 or batch_info_b.latitude_qc == 2 or batch_info_a.longitude_qc == 2 or batch_info_b.longitude_qc == 2:
                qc = 2
            distance = uhaversine((batch_info_b.latitude, batch_info_b.longitude), (batch_info_a.latitude, batch_info_a.longitude)).nominal_value
            unit_map[key] = "m s-1"
            return abs(distance / (batch_info_b.timestamp - batch_info_a.timestamp).total_seconds()), qc
        else:
            return self._extract_batch_value(batch_info_b, key, unit_map)

    def _show_graph(self, record_set: ocproc2.RecordSet, x_name: str, y_name: str):
        self._clear_graph()
        if y_name == 'Temperature+Salinity':
            self._show_ts_graph(record_set, x_name)
        else:
            unit_map = {}
            x_qc_values = [self._extract_value(r.coordinates, x_name, unit_map) for r in record_set.records]
            if y_name == 'Density':
                y_qc_values = [self._calculate_density(r, unit_map) for r in record_set.records]
            else:
                y_qc_values = [self._extract_value(r.parameters, y_name, unit_map) for r in record_set.records]
            self._show_qc_graph(
                x_qc_values,
                y_qc_values,
                x_name,
                y_name,
                unit_map
            )

    def _extract_plot_list(self, qc_values: list[tuple[float, int]]) -> tuple[float, float, list[float]]:
        data = []
        min_value = None
        max_value = None
        for v, qc in qc_values:
            data.append(v)
            if v is not None:
                if min_value is None or min_value > v:
                    min_value = v
                if max_value is None or max_value < v:
                    max_value = v
        return min_value, max_value, data

    def _calculate_x_axis_range(self, xx1: tuple[float, float], xx2: tuple[float, float]) -> tuple[float, float]:
        m = (xx2[1] - xx1[1]) / (xx2[0] - xx1[0])
        b = xx1[1] - (xx1[0] * m)
        return b, m + b

    def _plot_graph(self,
                    axes: mpla.Axes,
                    x_values: list[float],
                    y_values: dict[str, list[float]],
                    min_x: t.Optional[float] = None,
                    max_x: t.Optional[float] = None,
                    min_x_pos: t.Optional[float] = None,
                    max_x_pos: t.Optional[float] = None):
        pass

    def _show_ts_graph(self, record_set: ocproc2.RecordSet, x_name):
        unit_map = {}
        x_qc_values = [self._extract_value(r.coordinates, x_name, unit_map) for r in record_set.records]
        y_temp_values = [self._extract_value(r.parameters, 'Temperature', unit_map) for r in record_set.records]
        y_salinity_values = [self._extract_value(r.parameters, 'Salinity', unit_map) for r in record_set.records]
        self._axes: mpla.Axes = self._figure.subplots(1, 1)
        x_values = [-1 * x[0] for x in x_qc_values]
        min_temp, max_temp, temp_values = self._extract_plot_list(y_temp_values)
        min_saln, max_saln, saln_values = self._extract_plot_list(y_salinity_values)
        if min_temp is not None:
            self._axes.set_xlim(self._calculate_x_axis_range(
                (0.03, min_temp),
                (0.49, max_temp)
            ))
            self._axes.set_xticks(self._build_ticks(min_temp, max_temp))
            self._axes.tick_params(axis='x', labelrotation=90)
        self._axes.plot(
            temp_values,
            x_values,
            '-',
            c='#6666CC',
            linewidth=0.5
        )
        self._axes2 = self._axes.twiny()
        if min_saln is not None:
            self._axes2.set_xlim(self._calculate_x_axis_range(
                (0.51, min_saln),
                (0.97, max_saln)
            ))
            self._axes2.set_xticks(self._build_ticks(min_saln, max_saln))
            self._axes2.tick_params(axis='x', labelrotation=90)
        self._axes2.plot(
            saln_values,
            x_values,
            '-',
            c='#CC6666',
            linewidth=0.5
        )
        self._axes.set_ylabel(self._get_label(x_name, unit_map[x_name] if x_name in unit_map else None))
        self._axes.set_xlabel(self._get_label('Temperature', unit_map['Temperature'] if 'Temperature' in unit_map else None))
        self._axes2.set_xlabel(self._get_label('Salinity', unit_map['Salinity'] if 'Salinity' in unit_map else None))
        box = self._axes.get_position()
        self._axes.set_position([box.x0, box.y0 + (box.height * 0.1), box.width, box.height * 0.9])
        self._axes.legend(
            [
                Line2D([0], [0], linewidth=0.5, c='#CC6666'),
                Line2D([0], [0], linewidth=0.5, c='#6666CC'),
            ], [
                self._get_label('Salinity', unit_map['Salinity'] if 'Salinity' in unit_map else None),
                self._get_label('Temperature', unit_map['Temperature'] if 'Temperature' in unit_map else None)
            ],
            loc="upper center",
            ncols=2,
            bbox_to_anchor=(0.5, -0.15),

        )
        self._canvas.draw()

    def _show_qc_graph(self, x_qc_values, y_qc_values, x_name, y_name, unit_map):
        x_values = [x[0] for x in x_qc_values]
        y_values = [y[0] for y in y_qc_values]
        self._axes: mpla.Axes = self._figure.subplots(1, 1)
        if x_name in ('Depth', 'Pressure'):
            # Reverse axes for depth
            x_name, y_name = y_name, x_name
            y_values, x_values = [x * -1 if x is not None else x for x in x_values], y_values
        self._axes.plot(
            x_values,
            y_values,
            "-",
            c="#999999",
            linewidth=0.5,
        )
        for i in range(0, len(x_values)):
            self._axes.scatter(
                x_values[i],
                y_values[i],
                c=self.app.quality_color(y_qc_values[i][1], x_qc_values[i][1])
            )
        self._axes.set_ylabel(self._get_label(y_name, unit_map[y_name] if y_name in unit_map else None))
        self._axes.set_xlabel(self._get_label(x_name, unit_map[x_name] if x_name in unit_map else None))
        self._canvas.draw()

    def _get_label(self, key, units: t.Optional[str] = None):
        # TODO: lookup in ontology and label graph with it

        if units:
            return f"{key} [{units}]"
        else:
            return key

    def _calculate_density(self, record: ocproc2.BaseRecord, unit_map: dict) -> tuple[t.Optional[float], int]:
        v, qc, _ = oom.calc_density_record(record, self.app.app_state.record, 'kg m-3')
        return v, qc

    def _extract_value(self, map_: ocproc2.ElementMap, value_name: str, unit_map: dict) -> tuple[t.Optional[float], int]:
        # TODO: Salinity (PSAL or ASAL)
        if value_name == 'Salinity':
            if 'PracticalSalinity' in map_:
                saln, qc_saln = self._extract_value(map_, 'PracticalSalinity', unit_map)
                if 'PracticalSalinity' not in unit_map:
                    unit_map['PracticalSalinity'] = '0.001'
                if 'Salinity' not in unit_map:
                    unit_map['Salinity'] = unit_map['PracticalSalinity']
                if unit_map['Salinity'] == unit_map['PracticalSalinity']:
                    return saln, qc_saln
                else:
                    # TODO: convert PSAL to ASAL
                    pass
                    return None, 9
            elif 'AbsoluteSalinity' in map_:
                saln, qc_saln = self._extract_value(map_, 'AbsoluteSalinity', unit_map)
                if 'AbsoluteSalinity' not in unit_map:
                    unit_map['AbsoluteSalinity'] = 'g kg-1'
                if 'Salinity' not in unit_map:
                    unit_map['Salinity'] = unit_map['AbsoluteSalinity']
                if unit_map['Salinity'] == unit_map['AbsoluteSalinity']:
                    return saln, qc_saln
                else:
                    # TODO: convert ASAL to PSAL
                    pass
                    return None, 9
        if not map_.has_value(value_name):
            return None, 9
        val = map_[value_name]
        if not val.is_numeric():
            return None, 9
        raw_value = val.to_float()
        wq = int(val.metadata.best_value('WorkingQuality', 0))
        if wq == 5:
            wq = 1
        if val.metadata.has_value('Units'):
            units = val.metadata.best_value('Units')
            if value_name not in unit_map:
                unit_map[value_name] = units
            if unit_map[value_name] != units:
                return float(self.converter.convert(raw_value, units, unit_map[value_name])), wq
        return raw_value, wq
