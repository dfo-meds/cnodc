import functools

from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, BatchOpenState
import tkintermapview as tkmv
import typing as t
import cnodc.ocproc2.structures as ocproc2
import tkinter.ttk as ttk
from autoinject import injector
from cnodc.desktop.client.local_db import LocalDatabase
import matplotlib.axes as mpla
import matplotlib.figure as mplf
import matplotlib.backends.backend_tkagg as mpltk
import matplotlib.backend_bases as mplbb
import tkinter as tk
import cnodc.desktop.translations as i18n
from cnodc.ocproc2.operations import QCSetWorkingQuality, QCAddHistory

from cnodc.units import UnitConverter


class GraphPane(BasePane):

    local_db: LocalDatabase = None
    converter: UnitConverter = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._chart_frame: t.Optional[ttk.Frame] = None
        self._figure: t.Optional[mplf.Figure] = None
        self._canvas: t.Optional[mpltk.FigureCanvasTkAgg] = None
        self._axes: t.Optional[mpla.Axes] = None
        self._current_coordinate: t.Optional[str] = None
        self._current_parameter: t.Optional[str] = None
        self._current_recordset_id: t.Optional[str] = None
        self._current_recordset: t.Optional[ocproc2.RecordSet] = None
        self._combo_recordset: t.Optional[ttk.Combobox] = None
        self._combo_independent: t.Optional[ttk.Combobox] = None
        self._combo_dependent: t.Optional[ttk.Combobox] = None

    def on_init(self):
        self._chart_frame = ttk.Frame(self.app.middle_right)
        self._chart_frame.grid(row=0, column=0, sticky='NSEW')
        self._chart_frame.rowconfigure(0, weight=1)
        self._chart_frame.columnconfigure(0, weight=1)
        self._figure = mplf.Figure(dpi=100, figsize=(1, 1))
        self._canvas = mpltk.FigureCanvasTkAgg(self._figure, master=self._chart_frame)
        self._canvas.get_tk_widget().configure(scrollregion=(0, 0, 600, 600))
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

    def _on_click(self, event: mplbb.MouseEvent):
        if event.button != 3 or self._current_coordinate is None or self._current_parameter is None or self._current_recordset is None:
            return
        if event.ydata is None:
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

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD | DisplayChange.BATCH):
            self._clear_graph()
            if app_state.record is not None and 'PROFILE' in app_state.record.subrecords:
                self._update_boxes(app_state.record)
        elif change_type & DisplayChange.ACTION:
            if self._axes is not None:
                self._reload_recordset()

    def _reload_recordset(self):
        self._clear_graph()
        self._current_recordset_id = None
        self._update_variables(self._current_combobox_value(self._combo_recordset))

    def _update_boxes(self, record: ocproc2.DataRecord):
        rs_choices = []
        for srt in record.subrecords:
            for rs_idx in record.subrecords[srt]:
                rs_choices.append(f"{srt}#{rs_idx}")
        self._combo_recordset.configure(values=rs_choices)
        self._current_recordset_id = None
        if rs_choices:
            self._combo_recordset.current(0)
            self._update_variables(rs_choices[0])
        else:
            self._update_variables(None)

    def _rs_change(self, e):
        self._update_variables(self._current_combobox_value(self._combo_recordset))

    def _update_variables(self, record_set_name: t.Optional[str]):
        if record_set_name == self._current_recordset_id:
            return
        if record_set_name is None:
            self._combo_independent.configure(values=[])
            self._combo_dependent.configure(values=[])
            self._current_recordset_id = None
            self._current_recordset = None
            self._clear_graph()
            return
        self._current_recordset_id = record_set_name
        last_ind = self._current_combobox_value(self._combo_independent)
        last_dep = self._current_combobox_value(self._combo_dependent)
        srt, rs_idx = record_set_name.split('#', maxsplit=1)
        path = f'subrecords/{srt}/{rs_idx}'
        self._current_recordset: ocproc2.RecordSet = self.app.app_state.record.find_child(path)
        dep_vars = set()
        ind_vars = set()
        for r in self._current_recordset.records:
            dep_vars.update(r.parameters.keys())
            ind_vars.update(r.coordinates.keys())
        dep_vars = list(dep_vars)
        dep_vars.sort()
        ind_vars = list(ind_vars)
        ind_vars.sort()
        self._combo_independent.configure(values=ind_vars)
        self._combo_dependent.configure(values=dep_vars)
        self._current_parameter = None
        self._current_coordinate = None
        if ind_vars:
            self._combo_independent.current(ind_vars.index(last_ind) if last_ind and last_ind in ind_vars else 0)
        if dep_vars:
            self._combo_dependent.current(dep_vars.index(last_dep) if last_dep and last_dep in dep_vars else 0)
        if ind_vars and dep_vars:
            self._update_graph()
        else:
            self._clear_graph()

    def _var_change(self, e):
        self._update_graph()

    def _current_combobox_value(self, cb: ttk.Combobox):
        curr = cb.current()
        if curr > -1:
            return cb['values'][curr]
        return None

    def _update_graph(self):
        p = self._current_combobox_value(self._combo_dependent)
        c = self._current_combobox_value(self._combo_independent)
        if p != self._current_parameter or c != self._current_coordinate:
            if p is None or c is None:
                self._clear_graph()
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

    def _show_graph(self, record_set: ocproc2.RecordSet, x_name: str, y_name: str):
        self._clear_graph()
        unit_map = {}
        x_qc_values = [self._extract_value(r.coordinates, x_name, unit_map) for r in record_set.records]
        y_qc_values = [self._extract_value(r.parameters, y_name, unit_map) for r in record_set.records]
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
        self._axes.set_ylabel(f'{y_name} [{unit_map[y_name]}]' if y_name in unit_map else y_name)
        self._axes.set_xlabel(f'{x_name} [{unit_map[x_name]}]' if x_name in unit_map else x_name)
        self._canvas.draw()

    def _extract_value(self, map_: ocproc2.ValueMap, value_name: str, unit_map: dict) -> tuple[t.Optional[float], int]:
        if not map_.has_value(value_name):
            return None, 9
        val = map_[value_name]
        if not val.is_numeric():
            return None, 9
        raw_value = val.to_float()
        wq = int(val.metadata.best_value('WorkingQuality', 0))
        if val.metadata.has_value('Units'):
            units = val.metadata.best_value('Units')
            if value_name not in unit_map:
                unit_map[value_name] = units
            if unit_map[value_name] != units:
                return float(self.converter.convert(raw_value, units, unit_map[value_name])), wq
        return raw_value, wq
