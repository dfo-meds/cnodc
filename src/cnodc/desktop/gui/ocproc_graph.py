from __future__ import annotations

import math
import tkinter.ttk as ttk
import typing as t
import cnodc.desktop.translations as i18n
import matplotlib.axes as mpla
import matplotlib.figure as mplf
import matplotlib.backends.backend_tkagg as mpltk
import matplotlib.backend_bases as mplbb
import matplotlib.style as mpls
import cnodc.ocean_math.ocproc2int as oom
from cnodc.desktop.gui.base_pane import SimpleRecordInfo
from cnodc.ocean_math.geodesy import uhaversine

if t.TYPE_CHECKING:
    from cnodc.desktop.main_app import CNODCQCApp
    import cnodc.ocproc2.structures as ocproc2


TICK_INTERVALS = [
    0.01,
    0.02,
    0.05,
    0.1,
    0.2,
    0.5,
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1000,
    2000,
    5000,
    10000
]


class OCProc2Graph(ttk.Frame):

    def __init__(self, parent, app: CNODCQCApp):
        super().__init__(parent, width=500, height=500)
        self.rowconfigure(0, weight=0)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        self.app = app
        mpls.use('ggplot')
        self._axes: t.Optional[mpla.Axes] = None
        self._extra_axes: list[mpla.Axes] = []
        self.graph_option_box = ttk.Combobox(
            self
        )
        self.graph_option_box.bind('<<ComboboxSelected>>', self.update_graph_data)
        self._graph_option_names: t.Optional[list[str]] = None
        self.graph_option_box.grid(row=0, column=0, padx=5, pady=5)
        self._figure = mplf.Figure(dpi=100, figsize=(1, 1))
        self._canvas = mpltk.FigureCanvasTkAgg(self._figure, master=self)
        self._canvas.draw()
        self._canvas.get_tk_widget().grid(row=1, column=0, sticky='NSEW')
        self._current_graph: t.Optional[str] = None
        self._current_record_uuid: t.Optional[str] = None

    def update_graph_options(self):
        current_opt = None
        if self._graph_option_names and self.graph_option_box.current() > -1:
            current_opt = self._graph_option_names[self.graph_option_box.current()]
        new_options = self._build_graph_list()
        self._graph_option_names = [x for x in new_options.keys()]
        self.graph_option_box.configure(values=[new_options[x] for x in self._graph_option_names], width=max(len(new_options[x]) for x in new_options) if new_options else 10)
        if current_opt is not None and current_opt in self._graph_option_names:
            self.graph_option_box.current(self._graph_option_names.index(current_opt))
        else:
            # TODO: better default selection? (i.e. T&S most of the time if available, speed if speed check, etc)
            self.graph_option_box.current(0)
        self.update_graph_data(force_redraw=self._current_record_uuid != self.app.app_state.record_uuid)
        self._current_record_uuid = self.app.app_state.record_uuid

    def update_graph_data(self, e=None, force_redraw: bool = False):
        if force_redraw:
            self.clear_graph_data()
        if self.graph_option_box.current() < 0:
            self.clear_graph_data()
            return
        selected_graph = self._graph_option_names[self.graph_option_box.current()]
        if self._current_graph is None or selected_graph != self._current_graph:
            self.clear_graph_data()
            self._current_graph = selected_graph
            self._build_graph()

    def clear_graph_data(self):
        if self._axes is not None:
            self._figure.delaxes(self._axes)
            self._axes = None
            self._current_graph = None
        if self._extra_axes:
            for x in self._extra_axes:
                self._figure.delaxes(x)
            self._extra_axes = []

    def _build_graph(self):
        self._axes: mpla.Axes = self._figure.subplots(1, 1)
        graph_pieces = self._current_graph.split('::')
        if graph_pieces[0] == 'batch':
            self._build_batch_graph(graph_pieces[1])
        elif graph_pieces[0] == 'recordset':
            self._build_recordset_graph(
                f'subrecords/{graph_pieces[1]}/{graph_pieces[2]}',
                graph_pieces[3],
                graph_pieces[4]
            )
        self._canvas.draw()

    def _build_batch_graph(self, graph_type: str):
        if graph_type == 'speed_chart':
            self._build_speed_chart()

    def _build_speed_chart(self):
        speeds = {}
        indexes = {}
        last_records = {}
        for idx, record in enumerate(self.app.app_state.ordered_simple_records()):
            if not record.station_id:
                continue
            if record.station_id not in speeds:
                speeds[record.station_id] = []
                indexes[record.station_id] = []
            indexes[record.station_id].append(idx)
            if record.station_id not in last_records:
                speeds[record.station_id].append(None)
            else:
                speeds[record.station_id].append(self._calculate_station_speed(last_records[record.station_id], record))
            last_records[record.station_id] = record
        self._axes = self._figure.subplots(1, 1)
        self._set_axis_info(
            self._axes,
            label='Index',
        )
        self._set_axis_info(
            self._axes,
            label='Speed',
            on_y_axis=True
        )
        for station_id in indexes.keys():
            self._plot_points_and_line(
                self._axes,
                indexes[station_id],
                speeds[station_id]
            )

    def _calculate_station_speed(self, r1: SimpleRecordInfo, r2: SimpleRecordInfo):
        if r1.latitude is None or r1.longitude is None or r2.latitude is None or r2.longitude is None:
            return None, 9
        # TODO: qc calculation
        qc = 0
        return uhaversine(
            (r2.latitude, r2.longitude),
            (r1.latitude, r1.longitude)
        ), qc

    def _build_recordset_graph(self, recordset_path: str, ind_var: str, dep_var: str):
        recordset = self.app.app_state.record.find_child(recordset_path)
        if dep_var == '_TnSP':
            self._build_tsp_graph(recordset, ind_var)
        elif dep_var == '_TnSA':
            self._build_tsa_graph(recordset, ind_var)
        elif dep_var == '_Density':
            self._build_density_graph(recordset, ind_var)
        elif dep_var[0] != '_':
            self._build_variable_graph(recordset, ind_var, dep_var)

    def _extract_recordset_values(self,
                                  rs: ocproc2.RecordSet,
                                  *variables: str) -> tuple[dict[str, list[tuple[t.Optional[float], int]]], dict[str, float], dict[str, float], dict[str, str]]:
        results = {v: [] for v in variables}
        min_values = {v: None for v in variables}
        max_values = {v: None for v in variables}
        unit_map = {
            'Depth': 'm',
            'PracticalSalinity': '0.001',
            'Pressure': 'dbar',
            'Temperature': '°C',
            '_Density': 'kg m-3',
        }
        if rs is not None:
            for record in rs.records:
                for v in variables:
                    value, value_qc = None, 9
                    if v == '_Density':
                        units = unit_map['_Density'] if '_Density' in unit_map else None
                        value, value_qc, rho_units = oom.calc_density_record(
                            level_record=record,
                            position_record=self.app.app_state.record,
                            units=units
                        )
                        unit_map['_Density'] = rho_units
                    else:
                        y = record.coordinates.get(v)
                        if y is None:
                            y = record.parameters.get(v)
                        if y is not None:
                            if v not in unit_map:
                                unit_map[v] = y.units()
                            value = y.to_float(unit_map[v])
                            value_qc = y.working_quality()
                    results[v].append((value, value_qc))
                    if value is not None:
                        if min_values[v] is None or min_values[v] > value:
                            min_values[v] = value
                        if max_values[v] is None or max_values[v] < value:
                            max_values[v] = value
        return results, min_values, max_values, unit_map

    def _build_tsp_graph(self, rs: ocproc2.RecordSet, ind_var: str):
        self._build_two_variable_graph(rs, ind_var, 'Temperature', 'PracticalSalinity')

    def _build_tsa_graph(self, rs: ocproc2.RecordSet, ind_var: str):
        self._build_two_variable_graph(rs, ind_var, 'Temperature', 'AbsoluteSalinity')

    def _build_density_graph(self, rs: ocproc2.RecordSet, ind_var: str):
        # TODO: have to update the controls to not work the same way
        self._build_variable_graph(rs, ind_var, '_Density')

    def _build_two_variable_graph(self, rs:ocproc2.RecordSet, ind_var: str, dep1_var: str, dep2_var: str):
        values, mins, maxs, units = self._extract_recordset_values(rs, dep1_var, dep2_var, ind_var)
        if ind_var in ('Depth', 'Pressure'):
            mins[ind_var] = 0
        reverse_plot = self._should_reverse(ind_var)
        other_axis = self._twin_axis(self._axes, reverse_plot)
        self._extra_axes.append(other_axis)
        self._set_axis_info(
            self._axes,
            label=self._get_variable_label(ind_var, units[ind_var] if ind_var in units else None),
            min_value=mins[ind_var],
            max_value=maxs[ind_var],
            on_y_axis=reverse_plot,
            invert_axis=reverse_plot,
            num_ticks=7
        )
        self._set_axis_info(
            self._axes,
            label=self._get_variable_label(dep1_var, units[dep1_var] if dep1_var in units else None),
            min_value=mins[dep1_var],
            max_value=maxs[dep1_var],
            max_value_pos=0.48,
            label_pos='left',
            on_y_axis=not reverse_plot,
            num_ticks=5
        )
        self._set_axis_info(
            other_axis,
            label=self._get_variable_label(dep2_var, units[dep2_var] if dep2_var in units else None),
            min_value=mins[dep2_var],
            max_value=maxs[dep2_var],
            min_value_pos=0.52,
            label_pos='right',
            on_y_axis=not reverse_plot,
            num_ticks=5
        )
        self._plot_points_and_line(
            self._axes,
            values[ind_var],
            values[dep1_var],
            reverse_plot,
            color='#6666CC',
        )
        self._plot_points_and_line(
            other_axis,
            values[ind_var],
            values[dep2_var],
            reverse_plot,
            color='#CC6666',
        )

    def _build_variable_graph(self, rs: ocproc2.RecordSet, ind_var: str, dep_var: str):
        values, mins, maxs, units = self._extract_recordset_values(rs, dep_var, ind_var)
        if ind_var in ('Depth', 'Pressure'):
            mins[ind_var] = 0
        reverse_plot = self._should_reverse(ind_var)
        self._set_axis_info(
            self._axes,
            label=self._get_variable_label(ind_var, units[ind_var] if ind_var in units else None),
            min_value=mins[ind_var],
            max_value=maxs[ind_var],
            on_y_axis=reverse_plot,
            invert_axis=reverse_plot,
            num_ticks=7
        )
        self._set_axis_info(
            self._axes,
            label=self._get_variable_label(dep_var, units[dep_var] if dep_var in units else None),
            min_value=mins[dep_var],
            max_value=maxs[dep_var],
            on_y_axis=not reverse_plot,
            num_ticks=7
        )
        self._plot_points_and_line(
            self._axes,
            values[ind_var],
            values[dep_var],
            reverse_plot
        )

    def _plot_points_and_line(self,
                              axes: mpla.Axes,
                              ind_v_qc: list[tuple[t.Optional[float], int]],
                              dep_v_qc: list[tuple[t.Optional[float], int]],
                              reverse_axes: bool = False,
                              use_qc_color: bool = True,
                              color="#666666",
                              linewidth=1):
        x_values = []
        y_values = []
        for i in range(0, len(ind_v_qc)):
            if reverse_axes:
                x = dep_v_qc[i]
                y = ind_v_qc[i]
            else:
                x = ind_v_qc[i]
                y = dep_v_qc[i]
            axes.scatter(
                x[0], y[0], c=(self.app.quality_color(y[1], x[1]) if use_qc_color else color)
            )
            x_values.append(x[0])
            y_values.append(y[0])
        axes.plot(
            x_values,
            y_values,
            '-',
            c=color,
            linewidth=linewidth
        )

    def _should_reverse(self, ind_var):
        return ind_var in ('Pressure', 'Depth')

    def _get_variable_label(self,
                            var_name: str,
                            units: str = None):
        if var_name == 'PracticalSalinity' and units == '0.001':
            units = 'psu'
        return var_name if units is None else f"{var_name} [{units}]"

    def _calculate_axis_range(self,
                              xx1: tuple[float, float],
                              xx2: tuple[float, float]) -> tuple[float, float]:
        m = (xx2[1] - xx1[1]) / (xx2[0] - xx1[0])
        b = xx1[1] - (xx1[0] * m)
        return b, m + b

    def _build_ticks(self, min_val: float, max_val: float, num_ticks: int = 7):
        step_size = self._normalize_tick_size((max_val - min_val) / (num_ticks - 1))
        current = int(min_val / step_size) * step_size
        result = []
        while True:
            result.append(current)
            if current >= max_val:
                break
            current += step_size
        return result

    def _normalize_tick_size(self, tick_size: float) -> float:
        return max(x for x in TICK_INTERVALS if x < tick_size)

    def _twin_axis(self, axes: mpla.Axes, twin_y_axis: bool = False) -> mpla.Axes:
        if twin_y_axis:
            return axes.twiny()
        else:
            return axes.twinx()

    def _set_axis_info(self,
                        axes: mpla.Axes,
                        label: str,
                        min_value: t.Optional[float] = None,
                        max_value: t.Optional[float] = None,
                        min_value_pos: t.Optional[float] = 0.02,
                        max_value_pos: t.Optional[float] = 0.98,
                        on_y_axis: bool = False,
                        num_ticks: t.Optional[int] = None,
                        invert_axis: bool = False,
                        label_pos: t.Optional[str] = None):
        if on_y_axis:
            axes.tick_params(axis='y', labelsize=9)
            axes.set_ylabel(label, loc=label_pos or 'center')
            if min_value is not None and min_value != max_value:
                if num_ticks is not None and num_ticks > 1:
                    ticks = self._build_ticks(min_value, max_value, num_ticks)
                    axes.set_yticks(ticks)
                    min_value = ticks[0]
                    max_value = ticks[-1]
                axes.set_ylim(self._calculate_axis_range(
                    (min_value_pos, min_value),
                    (max_value_pos, max_value)
                ))
            if invert_axis:
                axes.invert_yaxis()
        else:
            axes.set_xlabel(label, loc=label_pos or 'center')
            axes.tick_params(axis='x', labelsize=9)
            if min_value is not None and min_value != max_value:
                if num_ticks is not None and num_ticks > 1:
                    ticks = self._build_ticks(min_value, max_value, num_ticks)
                    axes.tick_params(axis='x', labelrotation=90)
                    axes.set_xticks(ticks)
                    min_value = ticks[0]
                    max_value = ticks[-1]
                axes.set_xlim(self._calculate_axis_range(
                    (min_value_pos, min_value),
                    (max_value_pos, max_value)
                ))
            if invert_axis:
                axes.invert_xaxis()

    def _build_graph_list(self) -> dict[str, str]:
        graph_options = {}
        if self.app.app_state.batch_record_info is not None and len(self.app.app_state.batch_record_info) > 1:
            graph_options['batch::speed_chart'] = i18n.get_text('graph_speed_chart')
        if self.app.app_state.record is not None:
            graph_options.update(self._record_graph_options(self.app.app_state.record))
        return graph_options

    def _record_graph_options(self, record: ocproc2.DataRecord) -> dict[str, str]:
        options = {}
        for srt in record.subrecords:
            for rs_idx in record.subrecords[srt]:
                options.update(self._recordset_graph_options(record.subrecords[srt][rs_idx], srt, rs_idx))
        return options

    def _recordset_graph_options(self, recordset: ocproc2.RecordSet, srt: str, rs_idx: int) -> dict[str, str]:
        coordinates = set()
        parameters = set()
        for record in recordset.records:
            coordinates.update(x for x in record.coordinates.keys() if not record.coordinates[x].is_empty())
            parameters.update(x for x in record.parameters.keys() if not record.parameters[x].is_empty())
        options = {}
        for c in coordinates:
            if 'Temperature' in parameters:
                has_sp = 'PracticalSalinity' in parameters
                has_sa = 'AbsoluteSalinity' in parameters
                if has_sp:
                    options[f'recordset::{srt}::{rs_idx}::{c}::_TnSP'] = self._tsp_graph_name(srt, rs_idx, c)
                if has_sa:
                    options[f'recordset::{srt}::{rs_idx}::{c}::_TnSA'] = self._tsa_graph_name(srt, rs_idx, c)
                if ('Depth' in coordinates or 'Pressure' in coordinates) and (has_sp or has_sa):
                    options[f'recordset::{srt}::{rs_idx}::{c}::_Density'] = self._density_graph_name(srt, rs_idx, c)
            for p in parameters:
                options[f"recordset::{srt}::{rs_idx}::{c}::{p}"] = self._coord_param_graph_name(srt, rs_idx, c, p)
        return options

    def _density_graph_name(self, srt, rs_idx, x_name):
        return f'{srt}#{rs_idx} - Density by {x_name}'

    def _tsp_graph_name(self, srt, rs_idx, x_name):
        return f'{srt}#{rs_idx} - T&Sp by {x_name}'

    def _tsa_graph_name(self, srt, rs_idx, x_name):
        return f'{srt}#{rs_idx} - T&Sa by {x_name}'

    def _coord_param_graph_name(self, srt, rs_idx, x_name, y_name):
        # TODO: better translations
        return f"{srt}#{rs_idx} - {y_name} by {x_name}"




