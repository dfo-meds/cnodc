from autoinject import injector
import math
import tkinter.ttk as ttk
import typing as t
import gcapp.i18n.base as i18n
import matplotlib.axes as mpla
import matplotlib.figure as mplf
import matplotlib.backends.backend_tkagg as mpltk
import matplotlib.style as mpls
from medsutil.ocproc2 import RecordSet, BaseRecord
from pipeman_desktop.i18n import OCProc2Translator
from pipeman_desktop.state import SimpleRecordInfo, ApplicationState
from medsutil.geodesy import YXPoint, geodesic_distance
from pipeman_desktop.util import quality_color

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop
    import medsutil.ocproc2 as ocproc2


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


class Graph:

    @property
    def display_name(self) -> str:
        raise NotImplementedError

    def _set_axis_info(self,
                        axes: mpla.Axes,
                        label: str,
                        min_value: t.Optional[float] = None,
                        max_value: t.Optional[float] = None,
                        min_value_pos: t.Optional[float] = 0.02,
                        max_value_pos: t.Optional[float] = 0.98,
                        is_integer_data: bool = False,
                        on_y_axis: bool = False,
                        num_ticks: t.Optional[int] = None,
                        invert_axis: bool = False,
                        label_pos: str | None = None):
        if on_y_axis:
            axes.tick_params(axis='y', labelsize=9)
            axes.set_ylabel(label, loc=label_pos or "center")
            if min_value is not None and max_value is not None and min_value != max_value:
                if num_ticks is not None and num_ticks > 1:
                    ticks = self._build_ticks(min_value, max_value, num_ticks, is_integer_data)
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
            axes.set_xlabel(label, loc=label_pos or "center")
            axes.tick_params(axis='x', labelsize=9)
            if min_value is not None and max_value is not None and min_value != max_value:
                if num_ticks is not None and num_ticks > 1:
                    ticks = self._build_ticks(min_value, max_value, num_ticks, is_integer_data)
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

    def _calculate_axis_range(self,
                              xx1: tuple[float, float],
                              xx2: tuple[float, float]) -> tuple[float, float]:
        m = (xx2[1] - xx1[1]) / (xx2[0] - xx1[0])
        b = xx1[1] - (xx1[0] * m)
        return b, m + b

    def _build_ticks(self, min_val: float, max_val: float, num_ticks: int = 7, is_integer_data: bool = False):
        step_size = self._normalize_tick_size((max_val - min_val) / (num_ticks - 1))
        if is_integer_data:
            step_size = int(math.ceil(step_size))
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
        x_qc, y_qc = (dep_v_qc, ind_v_qc) if reverse_axes else (ind_v_qc, dep_v_qc)
        for x, y in zip(x_qc, y_qc):
            axes.scatter(
                x[0], y[0], c=(quality_color(y[1], x[1]) if use_qc_color else color)
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

    def build_graph(self, figure, state: ApplicationState) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        raise NotImplementedError


class SpeedGraph(Graph):

    @property
    def display_name(self) -> str:
        return i18n.tr("graph_speed")

    def build_graph(self, figure, state: ApplicationState) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        speeds = {}
        indexes = {}
        last_records = {}
        for idx, record in enumerate(state.ordered_simple_records()):
            if not record.platform_id:
                continue
            if record.platform_id not in speeds:
                speeds[record.platform_id] = []
                indexes[record.platform_id] = []
            indexes[record.platform_id].append((idx, 1))
            if record.platform_id not in last_records:
                speeds[record.platform_id].append((None, 1))
            else:
                speeds[record.platform_id].append(self._calculate_station_speed(last_records[record.platform_id], record))
            last_records[record.platform_id] = record
        axes = figure.subplots(1, 1)
        self._set_axis_info(
            axes,
            label='Index',
            is_integer_data=True,
        )
        self._set_axis_info(
            axes,
            label='Speed',
            on_y_axis=True
        )
        for station_id in indexes.keys():
            self._plot_points_and_line(
                axes,
                indexes[station_id],
                speeds[station_id]
            )
        return axes, None

    def _calculate_station_speed(self, r1: SimpleRecordInfo, r2: SimpleRecordInfo) -> tuple[float | None, int]:
        if r1.latitude is None or r1.longitude is None or r2.latitude is None or r2.longitude is None:
            return None, 9
        # TODO: qc calculation
        qc = 0
        return float(geodesic_distance(
            YXPoint(r2.latitude, r2.longitude),
            YXPoint(r1.latitude, r1.longitude)
        )), qc


class ParameterGraph(Graph):

    translator: OCProc2Translator

    @injector.construct
    def __init__(self, rs_path: str, coordinate_name: str, parameter_name: str, second_parameter_name: str | None = None):
        self._rs_path = rs_path
        self._cname = coordinate_name
        self._pname = parameter_name
        self._p2name = second_parameter_name

    @property
    def display_name(self) -> str:
        pieces = self._rs_path.strip('/').split('/')
        return "{rs_type}#{rs_index} - {variables} {by} {coordinate}".format(
            rs_type=self.translate_recordset_name(pieces[-2]),
            rs_index=pieces[-1],
            variables=self.translate_element_name(self._pname) + (f", {self.translate_element_name(self._p2name)}" if self._p2name is not None else ''),
            by=i18n.tr("graph_word_by"),
            coordinate=self.translate_element_name(self._cname)
        )

    def translate_recordset_name(self, rs_type: str) -> str:
        return self.translator.translate_recordset_type(rs_type)

    def translate_element_name(self, element_name: str):
        return self.translator.translate_element_name(element_name)

    def build_graph(self, figure, state: ApplicationState) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        recordset = state.current_record.find_child(self._rs_path)
        axes = figure.subplots(1, 1)
        if not isinstance(recordset, RecordSet):
            return axes, None
        if self._p2name is not None:
            return self._build_two_variable_graph(axes, recordset, self._cname, self._pname, self._p2name)
        else:
            return self._build_variable_graph(axes, recordset, self._cname, self._pname)

    def _build_two_variable_graph(self, axes, rs: ocproc2.RecordSet, ind_var: str, dep1_var: str, dep2_var: str) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        values, mins, maxs, units = self._extract_recordset_values(rs, dep1_var, dep2_var, ind_var)
        if ind_var in ('Depth', 'Pressure'):
            mins[ind_var] = 0
        reverse_plot = self._should_reverse(ind_var)
        other_axis = self._twin_axis(axes, reverse_plot)
        self._set_axis_info(
            axes,
            label=self._get_variable_label(ind_var, units[ind_var] if ind_var in units else None),
            min_value=mins[ind_var],
            max_value=maxs[ind_var],
            on_y_axis=reverse_plot,
            invert_axis=reverse_plot,
            num_ticks=7
        )
        self._set_axis_info(
            axes,
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
            axes,
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
        return axes, [other_axis]

    def _build_variable_graph(self, axes, rs: ocproc2.RecordSet, ind_var: str, dep_var: str) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        values, mins, maxs, units = self._extract_recordset_values(rs, dep_var, ind_var)
        if ind_var in ('Depth', 'Pressure'):
            mins[ind_var] = 0
        reverse_plot = self._should_reverse(ind_var)
        self._set_axis_info(
            axes,
            label=self._get_variable_label(ind_var, units[ind_var] if ind_var in units else None),
            min_value=mins[ind_var],
            max_value=maxs[ind_var],
            on_y_axis=reverse_plot,
            invert_axis=reverse_plot,
            num_ticks=7
        )
        self._set_axis_info(
            axes,
            label=self._get_variable_label(dep_var, units[dep_var] if dep_var in units else None),
            min_value=mins[dep_var],
            max_value=maxs[dep_var],
            on_y_axis=not reverse_plot,
            num_ticks=7
        )
        self._plot_points_and_line(
            axes,
            values[ind_var],
            values[dep_var],
            reverse_plot
        )
        return axes, None

    def _should_reverse(self, ind_var: str) -> bool:
        return ind_var in ('Pressure', 'Depth')

    def _get_variable_label(self,
                            var_name: str,
                            units: str | None = None) -> str:
        if var_name == 'PracticalSalinity' and units in ('0.001', '1e-3'):
            units = 'psu'
        return var_name if units is None else f"{var_name} [{units}]"

    def _twin_axis(self, axes: mpla.Axes, twin_y_axis: bool = False) -> mpla.Axes:
        if twin_y_axis:
            return axes.twiny()
        else:
            return axes.twinx()

    def _extract_recordset_values(self,
                                  rs: ocproc2.RecordSet,
                                  *variables: str) -> tuple[
        dict[str, list[tuple[float | None, int]]],
        dict[str, float | None],
        dict[str, float | None],
        dict[str, str | None]
    ]:
        # TODO: better data structure?
        results: dict[str, list[tuple[float | None, int]]] = {v: [] for v in variables}
        min_values: dict[str, float | None] = {v: None for v in variables}
        max_values: dict[str, float | None] = {v: None for v in variables}
        unit_map: dict[str, str | None] = {
            'Depth': 'm',
            'PracticalSalinity': '0.001',
            'Pressure': 'dbar',
            'Temperature': '°C',
            'Density': 'kg m-3',
        }
        if rs is not None:
            for record in rs.records:
                for v in variables:
                    value, value_qc = None, 9

                    if v.startswith("_") and v.endswith("_"):
                        value, value_qc = self._derived_parameter(v, record, unit_map)
                    else:
                        value, value_qc = self._observed_parameter(v, record, unit_map)
                    results[v].append((value, value_qc))
                    if value is not None:
                        if min_values[v] is None or min_values[v] > value:
                            min_values[v] = value
                        if max_values[v] is None or max_values[v] < value:
                            max_values[v] = value
        return results, min_values, max_values, unit_map

    def _observed_parameter(self, parameter_name: str, record: BaseRecord, units: dict[str, str | None]) -> tuple[float | None, int]:
        # TODO: sensor ranks?
        y = record.coordinates.ideal(parameter_name)
        if y is None:
            y = record.parameters.ideal(parameter_name)
        if y is not None:
            if parameter_name not in units:
                units[parameter_name] = y.units()
            return y.to_float(units[parameter_name]), (y.quality or 0)
        return None, 9

    def _derived_parameter(self, parameter_name: str, record: BaseRecord, units: dict[str, str | None]) -> tuple[float | None, int]:
        #if parameter_name == "_Density_":
        # TODO
        return None, 9


class OCProc2Graph(ttk.Frame):

    def __init__(self, parent, app: PipemanDesktop):
        super().__init__(parent)
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
        self._graph_options: dict[str, Graph] = {}
        self._graph_name_options: list[str] = []
        self.graph_option_box.grid(row=0, column=0, padx=5, pady=5)
        self._figure = mplf.Figure(dpi=100, figsize=(1, 1))
        self._canvas = mpltk.FigureCanvasTkAgg(self._figure, master=self)
        self._canvas.draw()
        self._canvas.get_tk_widget().grid(row=1, column=0, sticky='NSEW')
        self._current_graph_name: t.Optional[str] = None
        self._current_record_uuid: t.Optional[str] = None

    def update_graph(self):

        # save old selected option
        current_opt = None
        if self._graph_options and self.graph_option_box.current() > -1:
            current_opt = self._graph_name_options[self.graph_option_box.current()]

        # build new list
        self._graph_options = self._build_graph_list()
        self._graph_name_options = list(self._graph_options.keys())

        # build the display options
        new_values = [self._graph_options[x].display_name for x in self._graph_name_options]
        self.graph_option_box.configure(
            values=new_values,
            width=max(len(x) for x in new_values) if new_values else 10)

        # reset the previously selected one
        if current_opt is not None and current_opt in self._graph_options:
            self.graph_option_box.current(self._graph_name_options.index(current_opt))
        else:
            # TODO: better default selection? (i.e. T&S most of the time if available, speed if speed check, etc)
            self.graph_option_box.current(0)

        # refresh graph data
        self.update_graph_data(force_redraw=self._current_record_uuid != self.app.state.current_working_uuid)

        # ensure we are tracking the correct current record
        self._current_record_uuid = self.app.state.current_working_uuid

    def _build_graph_list(self) -> dict[str, Graph]:
        graph_options: dict[str, Graph] = {}
        if self.app.state.batch_records is not None and len(self.app.state.batch_records) > 1:
            graph_options['speed_graph'] = SpeedGraph()
        if self.app.state.current_parent is not None:
            graph_options.update(self._record_graph_options(self.app.state.current_parent, ""))
        return graph_options

    def _record_graph_options(self, record: ocproc2.BaseRecord, path: str = "") -> dict[str, Graph]:
        options = {}
        for srt in record.subrecords:
            for rs_idx in record.subrecords[srt]:
                options.update(self._recordset_graph_options(record.subrecords[srt][rs_idx], f"{path.rstrip('/')}/{srt}/{rs_idx}".lstrip('/')))
        return options

    def _recordset_graph_options(self, recordset: ocproc2.RecordSet, rs_path: str) -> dict[str, Graph]:
        coordinates = set()
        parameters = set()
        # TODO: sensor ranks
        for record in recordset.records:
            coordinates.update(x for x in record.coordinates.keys() if not record.coordinates[x].is_empty())
            parameters.update(x for x in record.parameters.keys() if not record.parameters[x].is_empty())
        options = {}
        for c in coordinates:
            if 'Temperature' in parameters:
                has_sp = 'PracticalSalinity' in parameters
                if has_sp:
                    options[f'recordset::{rs_path}::{c}::_TnSP'] = ParameterGraph(rs_path, c, "Temperature", "PracticalSalinity")
                    if 'Depth' in coordinates or 'Pressure' in coordinates:
                        options[f'recordset::{rs_path}::{c}::_Density'] = ParameterGraph(rs_path, c, "_Density_")
            for p in parameters:
                options[f"recordset::{rs_path}::{c}::{p}"] = ParameterGraph(rs_path, c, p)
        return options

    def update_graph_data(self, e=None, force_redraw: bool = False):
        if force_redraw:
            self.clear_graph_data()
        if self.graph_option_box.current() < 0:
            self.clear_graph_data()
            return
        selected_graph_name = self._graph_name_options[self.graph_option_box.current()]
        if self._current_graph_name is None or selected_graph_name != self._current_graph_name:
            self.clear_graph_data()
            self._current_graph_name = selected_graph_name
            self._build_graph()

    def clear_graph_data(self):
        if self._axes is not None:
            self._figure.delaxes(self._axes)
            self._axes = None
            self._current_graph_name = None
        if self._extra_axes:
            for x in self._extra_axes:
                self._figure.delaxes(x)
            self._extra_axes = []

    def _build_graph(self):
        if self._current_graph_name is not None:
            self._axes, self._extra_axes = self._graph_options[self._current_graph_name].build_graph(self._figure, self.app.state)
        self._canvas.draw()




