import functools

from autoinject import injector
import math
import tkinter.ttk as ttk
import typing as t
import gcapp.i18n.base as i18n
import matplotlib.axes as mpla
import matplotlib.figure as mplf
import matplotlib.backends.backend_tkagg as mpltk
import matplotlib.style as mpls
from medsutil.ocproc2 import RecordSet, BaseRecord, AbstractElement, ChangeQuality
from medsutil import ocproc2
from medsutil.ocproc2.operations import ChangeQualityAtLevelAndDeeper
from pipeman_desktop.components.context_menu import ContextMenuWithHover
from pipeman_desktop.i18n import OCProc2Translator
from pipeman_desktop.state import SimpleRecordInfo, ApplicationState
from medsutil.geodesy import YXPoint, geodesic_distance
from pipeman_desktop.util import quality_color

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop


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

    def on_button_press(self, app, canvas, event):
        if event.inaxes:
            self._on_button_press(app, canvas, event.xdata, event.ydata, event)

    def _on_button_press(self, app, canvas, data_x, data_y, event):
        ...

    def _set_axis_info(self,
                        axes: mpla.Axes,
                        label: str,
                        min_value: t.Optional[float] = None,
                        max_value: t.Optional[float] = None,
                        min_value_pos: float = 0.02,
                        max_value_pos: float = 0.98,
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
        graph_range = (xx2[1] - xx1[1]) / (xx2[0] - xx1[0])

        left = xx1[1] - (xx1[0] * graph_range)
        return left, left + graph_range + ((1-xx2[0]) * graph_range)

    def _build_ticks(self, min_val: float, max_val: float, num_ticks: int = 7, is_integer_data: bool = False):
        step_size = self._normalize_tick_size((max_val - min_val) / (num_ticks - 1))
        if is_integer_data:
            step_size = int(math.ceil(step_size))
        current = math.floor(min_val / step_size) * step_size
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
            if not use_qc_color:
                actual_color = color
            elif reverse_axes:
                actual_color = quality_color(x[1], y[1])
            else:
                actual_color = quality_color(y[1], x[1])
            axes.scatter(
                x[0], y[0], c=actual_color
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
        return i18n.tr("graph.speed_chart.title")

    def build_graph(self, figure, state: ApplicationState) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        speeds = {}
        indexes = {}
        last_records = {}
        current_platform_id = None
        for idx, record in enumerate(state.ordered_simple_records()):
            platform_id = record.platform_id if record.platform_id else "_____"
            if record.record_uuid == state.current_working_uuid:
                current_platform_id = platform_id
            if platform_id not in speeds:
                speeds[platform_id] = []
                indexes[platform_id] = []
            indexes[platform_id].append((idx + 1, 1))
            if platform_id not in last_records:
                speeds[platform_id].append((None, 1))
            else:
                speeds[platform_id].append(self._calculate_station_speed(last_records[platform_id], record))
            last_records[platform_id] = record
        axes = figure.subplots(1, 1)
        self._set_axis_info(
            axes,
            label=i18n.tr("graph.speed_chart.observation_index"),
            is_integer_data=True,
        )
        self._set_axis_info(
            axes,
            label=i18n.tr("graph.speed_chart.speed") + " [m s-1]",
            on_y_axis=True
        )
        if current_platform_id:
            self._plot_points_and_line(
                axes,
                indexes[current_platform_id],
                speeds[current_platform_id],
            )
        return axes, None

    def _calculate_station_speed(self, r1: SimpleRecordInfo, r2: SimpleRecordInfo) -> tuple[float | None, int]:
        if r1.latitude is None or r1.longitude is None or r2.latitude is None or r2.longitude is None or r1.timestamp is None or r2.timestamp is None:
            return None, 9
        qc = 0
        for qc_flag in (9, 4, 3, 2, 5, 1):
            if any(x == qc_flag for x in (r1.latitude_qc, r2.latitude_qc, r1.longitude_qc, r2.longitude_qc, r1.time_qc, r2.time_qc)):
                qc = qc_flag
                break
        delta_d = float(geodesic_distance(
            YXPoint(r2.latitude, r2.longitude),
            YXPoint(r1.latitude, r1.longitude)
        ))
        delta_t = (r2.timestamp - r1.timestamp).total_seconds()
        if delta_t == 0:
            if math.isclose(delta_d, 0, abs_tol=1e-6):
                return 0, qc
            return -1, qc  # indicates inf result
        return delta_d / delta_t, qc



class ParameterGraph(Graph):

    translator: OCProc2Translator

    OVERRIDE_DISPLAY_UNITS = {
        "Temperature": "°C",
        "Pressure": "dbar",
        "Depth": "m",
        "PracticalSalinity": "psu",
    }

    @injector.construct
    def __init__(self,
                 rs_path: str,
                 coordinate_name: str,
                 parameter_name: str,
                 parameter_sensor: int | None = None,
                 second_parameter_name: str | None = None,
                 second_parameter_sensor: int | None = None):
        self._rs_path = rs_path
        self._cname = coordinate_name
        self._pname = parameter_name
        self._psensor = parameter_sensor
        self._p2name = second_parameter_name
        self._p2sensor = second_parameter_sensor
        self._element_map: dict[float, dict[str | tuple[str, int | None], tuple[str, float | None]]] = {}
        self._axes1: mpla.Axes | None = None
        self._axes2: mpla.Axes | None = None
        self._current_series = None
        self._highlight_color = "#FFEB3B"

    @property
    def display_name(self) -> str:
        pieces = self._rs_path.strip('/').split('/')
        return i18n.tr("graph.parameter_chart.title",
            rs_type=self.translate_recordset_name(pieces[-2]),
            rs_index=pieces[-1],
            parameters=self.translate_element_name(self._pname, self._psensor) + (f", {self.translate_element_name(self._p2name, self._p2sensor)}" if self._p2name is not None else ''),
            coordinate=self.translate_element_name(self._cname)
        )

    def _on_button_press(self, app, canvas, data_x, data_y, event):
        is_depth_graph = self._should_reverse(self._cname)
        context_menu = ContextMenuWithHover(app.root)
        for n in (0,1,2,3,4,9):
            context_menu.add_command(
                i18n.tr(f"context_menu.graph.flag{n}_next_deeper" if is_depth_graph else f"context_menu.graph.flag{n}_next_greater", parameter=self.translate_element_name(self._pname, self._psensor)),
                command=functools.partial(self._flag_next_greater_than, parameter=(self._pname, self._psensor), independent_value=data_y, app=app, flag=n),
                on_mouse_in=functools.partial(self._highlight_next_greater_than, parameter=(self._pname, self._psensor), independent_value=data_y, invert=True, canvas=canvas, axes=self._axes1),
                on_mouse_out=functools.partial(self._clear_highlight, canvas=canvas)
            )
            context_menu.add_command(
                i18n.tr(f"context_menu.graph.flag{n}_all_deeper" if is_depth_graph else f"context_menu.graph.flag{n}_all_greater", parameter=self.translate_element_name(self._pname, self._psensor)),
                command=functools.partial(self._flag_all_greater_than, parameter=(self._pname, self._psensor), independent_value=data_y, app=app, flag=n),
                on_mouse_in=functools.partial(self._highlight_all_greater_than, parameter=(self._pname, self._psensor), independent_value=data_y, invert=True, canvas=canvas, axes=self._axes1),
                on_mouse_out=functools.partial(self._clear_highlight, canvas=canvas)
            )
        if self._p2name is not None:
            context_menu.add_command(
                i18n.tr(f"context_menu.graph.flag{n}_next_deeper" if is_depth_graph else f"context_menu.graph.flag{n}_next_greater", parameter=self.translate_element_name(self._p2name, self._p2sensor)),
                command=functools.partial(self._flag_next_greater_than, parameter=(self._p2name, self._p2sensor), independent_value=data_y, app=app, flag=n),
                on_mouse_in=functools.partial(self._highlight_next_greater_than, parameter=(self._p2name, self._p2sensor), independent_value=data_y, invert=True, canvas=canvas, axes=self._axes2),
                on_mouse_out=functools.partial(self._clear_highlight, canvas=canvas)
            )
            context_menu.add_command(
                i18n.tr(f"context_menu.graph.flag{n}_all_deeper" if is_depth_graph else f"context_menu.graph.flag{n}_all_greater", parameter=self.translate_element_name(self._p2name, self._p2sensor)),
                command=functools.partial(self._flag_all_greater_than, parameter=(self._p2name, self._p2sensor), independent_value=data_y, app=app, flag=n),
                on_mouse_in=functools.partial(self._highlight_all_greater_than, parameter=(self._p2name, self._p2sensor), independent_value=data_y, invert=True, canvas=canvas, axes=self._axes2),
                on_mouse_out=functools.partial(self._clear_highlight, canvas=canvas)
            )
        context_menu.popup_from_event(event.guiEvent)

    def _find_next_greater_than(self, independent_value: float, parameter_name: str | tuple[str, int | None]) -> tuple[float | None, str | None, float | None]:
        data = sorted(x for x in self._element_map.keys())
        for x in data:
            if x > independent_value:
                return x, self._element_map[x][parameter_name][0], self._element_map[x][parameter_name][1]
        return None, None, None

    def _highlight_next_greater_than(self, independent_value: float, parameter: str | tuple[str, int | None], invert: bool, axes: mpla.Axes, canvas):
        self._clear_highlight(canvas=canvas)
        series = []
        ind, _, dep = self._find_next_greater_than(independent_value, parameter)
        if ind is not None and dep is not None:
            if invert:
                series.append(axes.scatter(dep, ind, c=[self._highlight_color]))
            else:
                series.append(axes.scatter(ind, dep, c=[self._highlight_color]))
            canvas.draw_idle()
        self._current_series = series

    def _find_all_greater_than(self, independent_value: float, parameter: str | tuple[str, int | None]) -> t.Iterable[tuple[float | None, str | None, float | None]]:
        for ind_value, parameters in self._element_map.items():
            if ind_value is not None and ind_value > independent_value:
                if parameters[parameter][1] is not None:
                    yield ind_value, parameters[parameter][0], parameters[parameter][1]

    def _highlight_all_greater_than(self, independent_value: float, parameter: str | tuple[str, int | None], invert: bool, axes: mpla.Axes, canvas):
        self._clear_highlight(canvas)
        series = []
        for ind, _, dep in self._find_all_greater_than(independent_value, parameter):
            if invert:
                series.append(axes.scatter(dep, ind, c=[self._highlight_color]))
            else:
                series.append(axes.scatter(ind, dep, c=[self._highlight_color]))
        canvas.draw_idle()
        self._current_series = series

    def _clear_highlight(self, canvas):
        if self._current_series:
            for x in self._current_series:
                x.remove()
            self._current_series = None
        canvas.draw_idle()

    def _flag_all_greater_than(self, app: PipemanDesktop, independent_value: float, parameter: str | tuple[str, int | None], flag: int):
        first = None
        rest = []
        for _, path, _ in self._find_all_greater_than(independent_value, parameter):
            if first is None:
                first = path
            else:
                rest.append(path)
        app.state.add_action(ChangeQualityAtLevelAndDeeper(
            path=first,
            other_paths=rest,
            new_flag=flag
        ))

    def _flag_next_greater_than(self, app: PipemanDesktop, independent_value: float, parameter: str | tuple[str, int | None], flag: int):
        app.state.add_action(ChangeQuality(
            path=self._find_next_greater_than(independent_value, parameter)[1],
            new_flag=flag
        ))

    def translate_recordset_name(self, rs_type: str) -> str:
        return self.translator.translate_recordset_type(rs_type)

    def translate_element_name(self, element_name: str, sensor_rank: int | None = None) -> str:
        element_tr_name = self.translator.translate_element_name(element_name)
        if sensor_rank is None:
            return element_tr_name
        else:
            return i18n.tr("graph.parameter_chart.element_and_sensor",
                           element=element_tr_name,
                           sensor=f"R{sensor_rank}" if sensor_rank >= 0 else f"U{sensor_rank * -1}")

    def build_graph(self, figure, state: ApplicationState) -> tuple[mpla.Axes, list[mpla.Axes] | None]:
        self._figure = figure
        recordset = state.current_record.find_child(self._rs_path)
        axes = figure.subplots(1, 1)
        if not isinstance(recordset, RecordSet):
            # TODO: should be a warning here
            return axes, None
        if self._p2name is not None:
            self._axes1, self._axes2 = self._build_two_variable_graph(axes, recordset, self._cname, (self._pname, self._psensor), (self._p2name, self._p2sensor))
        else:
            self._axes1, self._axes2 = self._build_variable_graph(axes, recordset, self._cname, (self._pname, self._psensor))
        return t.cast(mpla.Axes, self._axes1), ([self._axes2] if self._axes2 else None)

    def _build_two_variable_graph(self, axes, rs: ocproc2.RecordSet, ind_var: str, dep1_var: str | tuple[str, int | None], dep2_var: str | tuple[str, int | None]) -> tuple[mpla.Axes, mpla.Axes | None]:
        values, mins, maxs, units = self._extract_recordset_values(rs, ind_var, dep1_var, dep2_var)
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
        return axes, other_axis

    def _build_variable_graph(self, axes, rs: ocproc2.RecordSet, ind_var: str, dep_var: str | tuple[str, int | None]) -> tuple[mpla.Axes, mpla.Axes | None]:
        values, mins, maxs, units = self._extract_recordset_values(rs, ind_var, dep_var)
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
                            variable: str | tuple[str, int | None],
                            units: str | None = None) -> str:
        if isinstance(variable, tuple):
            var_name, sensor = variable
        else:
            var_name = variable
            sensor = None
        if var_name.startswith("_") and var_name.endswith("_"):
            tr_name = i18n.tr(f"derived_parameter.{var_name.strip("_")}")
        else:
            tr_name = self.translate_element_name(var_name, sensor)
        return tr_name if units is None else f"{tr_name} [{units}]"

    def _twin_axis(self, axes: mpla.Axes, twin_y_axis: bool = False) -> mpla.Axes:
        if twin_y_axis:
            return axes.twiny()
        else:
            return axes.twinx()

    def _extract_recordset_values(self,
                                  rs: ocproc2.RecordSet,
                                  ind_variable: str,
                                  *dep_vars: str | tuple[str, int | None]) -> tuple[
        dict[str | tuple[str, int | None], list[tuple[float | None, int]]],
        dict[str | tuple[str, int | None], float | None],
        dict[str | tuple[str, int | None], float | None],
        dict[str | tuple[str, int | None], str | None]
    ]:
        all_variables = [ind_variable, *dep_vars]
        # TODO: better data structure?
        results: dict[str | tuple[str, int | None], list[tuple[float | None, int]]] = {v: [] for v in all_variables}
        min_values: dict[str | tuple[str, int | None], float | None] = {v: None for v in all_variables}
        max_values: dict[str | tuple[str, int | None], float | None] = {v: None for v in all_variables}
        unit_map: dict[str | tuple[str, int | None], str | None] = {}
        if rs is not None:
            base_path = self._rs_path.rstrip('/')
            for idx, record in enumerate(rs.records.iterate_with_load()):
                record_path = f"{base_path}/{idx}"
                ind_val = None
                dep_vals = {}
                for v in all_variables:
                    value, value_qc, element_path = self._get_value(v, record, unit_map, record_path)
                    results[v].append((value, value_qc))
                    if value is not None:
                        if min_values[v] is None or min_values[v] > value:
                            min_values[v] = value
                        if max_values[v] is None or max_values[v] < value:
                            max_values[v] = value
                    if v == ind_variable:
                        ind_val = value
                    elif element_path:
                        dep_vals[v] = (element_path, value)
                if ind_val is not None:
                    self._element_map[ind_val] = dep_vals
        return results, min_values, max_values, unit_map

    def _get_value(self,
                   v: str | tuple[str, int | None],
                   record: ocproc2.BaseRecord,
                   unit_map: dict[str | tuple[str, int | None], str | None],
                   record_path: str) -> tuple[float | None, int, str | None]:
        var_name = v[0] if isinstance(v, tuple) else v
        if var_name.startswith("_") and var_name.endswith("_"):
            return self._derived_parameter(var_name, record, unit_map)
        else:
            return self._observed_parameter(v, record, unit_map, record_path)

    def get_element(self, element_map: ocproc2.ElementMap, parameter_name: str, sensor_rank: int | None) -> tuple[str | None, ocproc2.SingleElement | None]:
        for sub_path, element_sensor_rank, element in self._find_elements(element_map.get(parameter_name)):
            if element.is_empty() or not element.is_numeric():
                continue
            if sensor_rank is None or sensor_rank == element_sensor_rank:
                return (parameter_name if not sub_path else f"{parameter_name}/{sub_path}"), element
        return None, None

    def _find_elements(self, element: ocproc2.AbstractElement, base_path: str = "", mem: dict | None = None) -> t.Iterable[tuple[str, int, ocproc2.SingleElement]]:
        # TODO: we should consider making sure this aligns with the sensor ranks in QC testing (or better yet, assign them after the data comes in)
        if mem is None:
            mem = {"next": -1}
        if isinstance(element, ocproc2.SingleElement):
            sr = element.sensor_rank
            if sr is None:
                sr = mem["next"]
                mem["next"] += 1
            yield base_path, sr, element
        else:
            for idx, sub_element in enumerate(element.value):
                yield from self._find_elements(sub_element, f"{base_path}/{idx}", mem)

    def _observed_parameter(self,
                            v: str | tuple[str, int | None],
                            record: BaseRecord,
                            units: dict[str | tuple[str, int | None], str | None],
                            base_path: str) -> tuple[float | None, int, str | None]:
        if isinstance(v, tuple):
            parameter_name = v[0]
            sensor_rank = v[1]
        else:
            parameter_name = v
            sensor_rank = None

        if parameter_name in record.coordinates:
            map_name = "coordinates"
            element_map = record.coordinates
        else:
            map_name = "parameters"
            element_map = record.parameters

        subpath, element = self.get_element(element_map, parameter_name, sensor_rank)
        if element is not None:
            if v not in units:
                units[v] = self.get_display_units(element, parameter_name)
            return element.to_float(units[v]), (element.quality or 0), f"{base_path.rstrip('/')}/{map_name}/{subpath}"
        return None, 9, None

    def get_display_units(self, element: AbstractElement, parameter_name: str) -> str | None:
        if parameter_name in self.OVERRIDE_DISPLAY_UNITS:
            return self.OVERRIDE_DISPLAY_UNITS[parameter_name]
        return element.units()

    def _derived_parameter(self,
                           parameter_name: str,
                           record: BaseRecord,
                           units: dict[str | tuple[str, int | None], str | None]) -> tuple[float | None, int, str | None]:
        #if parameter_name == "_Density_":
        # TODO
        return None, 9, None


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
        self._canvas.mpl_connect("button_press_event", self._on_button_press)
        self._canvas.draw()
        self._canvas.get_tk_widget().grid(row=1, column=0, sticky='NSEW')
        self._current_graph_name: t.Optional[str] = None
        self._current_record_uuid: t.Optional[str] = None

    def _on_button_press(self, event):
        if self._current_graph_name is not None and self._current_graph_name in self._graph_options:
            self._graph_options[self._current_graph_name].on_button_press(self.app, self._canvas, event)

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
                options.update(self._recordset_graph_options(record.subrecords[srt][rs_idx], f"{path.rstrip('/')}/subrecords/{srt}/{rs_idx}".lstrip('/')))
        return options

    def _sensor_rank_options(self, known_ranks: list[int], max_unlabelled_ranks: int) -> t.Iterable[int | None]:
        if len(known_ranks) + max_unlabelled_ranks == 1:
            # one unranked sensor, we can just display it
            yield None
        else:
            # two or more sensors are present somewhere, we'll do the sensor thing
            yield from known_ranks
            yield from range(-1, (-1 * max_unlabelled_ranks) - 1, -1)

    def _recordset_graph_options(self, recordset: ocproc2.RecordSet, rs_path: str) -> dict[str, Graph]:
        coordinates: set[str] = set()
        parameter_sensor_ranks: dict[str, list[int]] = {}
        parameter_max_unlabelled: dict[str, int] = {}
        for record in recordset.records:
            coordinates.update(x for x in record.coordinates.keys() if record.coordinates[x].is_numeric() and not record.coordinates[x].is_empty())
            for x in record.parameters.keys():
                if record.parameters[x].is_empty() or not record.parameters[x].is_numeric():
                    continue
                if x not in parameter_sensor_ranks:
                    parameter_sensor_ranks[x] = []
                    parameter_max_unlabelled[x] = 0
                unlabelled = 0
                for y in record.parameters[x].all_values():
                    sr = y.sensor_rank
                    if sr is not None:
                        parameter_sensor_ranks[x].append(sr)
                    else:
                        unlabelled += 1
                parameter_max_unlabelled[x] = max(parameter_max_unlabelled[x], unlabelled)
        options = {}
        for c in coordinates:
            if 'Temperature' in parameter_sensor_ranks and 'PracticalSalinity' in parameter_sensor_ranks:
                for t_rank in self._sensor_rank_options(parameter_sensor_ranks["Temperature"], parameter_max_unlabelled["Temperature"]):
                    for p_rank in self._sensor_rank_options(parameter_sensor_ranks["PracticalSalinity"], parameter_max_unlabelled["PracticalSalinity"]):
                        options[f'recordset::{rs_path}::{c}::_TnSP'] = ParameterGraph(rs_path, c, "Temperature", t_rank, "PracticalSalinity", p_rank)
                # TODO: add back in when Density is working again
                #if 'Depth' in coordinates or 'Pressure' in coordinates:
                #    options[f'recordset::{rs_path}::{c}::_Density'] = ParameterGraph(rs_path, c, "_Density_")
            for p in parameter_sensor_ranks.keys():
                for rank in self._sensor_rank_options(parameter_sensor_ranks[p], parameter_max_unlabelled[p]):
                    options[f"recordset::{rs_path}::{c}::{p}::{rank}"] = ParameterGraph(rs_path, c, p, rank)
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




