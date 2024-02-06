import datetime

from cnodc.desktop.gui.base_pane import BasePane
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import cnodc.desktop.translations as i18n
import cnodc.ocproc2.structures as ocproc2
import cnodc.ocproc2.operations as ops
import typing as t
import tkinter as tk


class ParameterContextMenu:

    def __init__(self, app, target_path):
        self._app = app
        self._target_path = target_path
        self._menu = tk.Menu(app.root, tearoff=0)
        self._menu.add_command(
            label=i18n.get_text('parameter_context_edit'),
            command=self._edit_value
        )
        self._menu.add_command(
            label=i18n.get_text('parameter_context_flag_good'),
            command=self._flag_good
        )
        self._menu.add_command(
            label=i18n.get_text('parameter_context_flag_probably_good'),
            command=self._flag_probably_good
        )
        self._menu.add_command(
            label=i18n.get_text('parameter_context_flag_dubious'),
            command=self._flag_dubious
        )
        self._menu.add_command(
            label=i18n.get_text('parameter_context_flag_erroneous'),
            command=self._flag_erroneous
        )
        self._menu.add_command(
            label=i18n.get_text('parameter_context_flag_missing'),
            command=self._flag_missing
        )

    def _edit_value(self):
        print(self._target_path, '5')

    def _flag_dubious(self):
        self._app.record_operator_action(ops.QCSetValue(f'{self._target_path}/metadata/WorkingQuality', 3))

    def _flag_erroneous(self):
        self._app.record_operator_action(ops.QCSetValue(f'{self._target_path}/metadata/WorkingQuality', 4))

    def _flag_missing(self):
        self._app.record_operator_action(ops.QCSetValue(f'{self._target_path}/metadata/WorkingQuality', 9))

    def _flag_good(self):
        self._app.record_operator_action(ops.QCSetValue(f'{self._target_path}/metadata/WorkingQuality', 1))

    def _flag_probably_good(self):
        self._app.record_operator_action(ops.QCSetValue(f'{self._target_path}/metadata/WorkingQuality', 2))

    def handle_popup_click(self, e):
        try:
            self._menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            self._menu.grab_release()


class ParameterPane(BasePane):

    TAG_MAP = {
        1: 'good',
        2: 'probably_good',
        3: 'dubious',
        4: 'erroneous',
        9: 'missing',
        12: 'recommend_probably_good',
        13: 'recommend-dubious',
        14: 'recommend-erroneous',
        19: 'recommend-missing',
        20: 'invalid',
        21: 'invalid',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parameter_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        self.app.right_frame.rowconfigure(0, weight=1)
        self.app.right_frame.columnconfigure(0, weight=1)
        self._parameter_list = ScrollableTreeview(
            parent=self.app.right_frame,
            selectmode='browse',
            show="tree",
            headers=[
                i18n.get_text('parameter_list_name'),
                i18n.get_text('parameter_list_value'),
            ],
            on_right_click=self._on_parameter_right_click,
            displaycolumns=(1, 2,)
        )
        self._parameter_list.tag_configure('recommend-dubious', foreground='blue')
        self._parameter_list.tag_configure('recommend-erroneous', foreground='red')
        self._parameter_list.tag_configure('recommend-missing', foreground='orange')
        self._parameter_list.grid(row=0, column=0, sticky='NSEW')
        self._parameter_list.table.column('#0', width=40, stretch=False)
        self._parameter_list.table.column('#1', minwidth=60, anchor='w')
        self._parameter_list.table.column('#2', anchor='e')

    def show_record(self, record: ocproc2.DataRecord, path: str):
        self._parameter_list.clear_items()
        if record.metadata:
            m_path = f'{path}/metadata' if path else 'metadata'
            self._parameter_list.table.insert('', 'end', open=True, iid=m_path, text='', values=[m_path, 'Metadata', ''])
            for k in record.metadata.keys():
                self._create_parameter_entry(record.metadata[k], m_path, k)
        if record.coordinates:
            c_path = f'{path}/coordinates' if path else 'coordinates'
            self._parameter_list.table.insert('', 'end', open=True, iid=c_path, text='', values=[c_path, 'Coordinates', ''])
            for k in record.coordinates.keys():
                self._create_parameter_entry(record.coordinates[k], c_path, k)
        if record.parameters:
            p_path = f'{path}/parameters' if path else 'parameters'
            self._parameter_list.table.insert('', 'end', open=True, iid=p_path, text='', values=[p_path, 'Coordinates', ''])
            for k in record.parameters.keys():
                self._create_parameter_entry(record.parameters[k], p_path, k)

    def show_recordset(self, record_set: ocproc2.RecordSet, path: str):
        self._parameter_list.clear_items()
        if record_set.metadata:
            m_path = f'{path}/metadata'
            self._parameter_list.table.insert('', 'end', open=True, iid=m_path, text='', values=[m_path, 'Metadata', ''])
            for k in record_set.metadata.keys():
                self._create_parameter_entry(record_set.metadata[k], m_path, k)

    def _create_parameter_entry(self, v: ocproc2.AbstractValue, parent_path: str, key: str, depth: int = 1):
        if isinstance(v, ocproc2.MultiValue):
            for idx, subv in v.values():
                self._create_parameter_entry(subv, f'{parent_path}/{key}/{idx}', str(idx), depth + 1)
        else:
            self._create_parameter_list_item(v, parent_path, key, depth)
        if v.metadata:
            for k in v.metadata:
                if k in ('Units', 'Quality', 'WorkingQuality') and v.is_numeric():
                    continue
                self._create_parameter_entry(v.metadata[k], f'{parent_path}/{key}', k, depth + 1)

    def _create_parameter_list_item(self, v: ocproc2.AbstractValue, parent_path: str, key: str, depth: int):
        path = f'{parent_path}/{key}'
        dv, tags = self._parameter_display_value(v)
        self._parameter_list.table.insert(parent_path, 'end', iid=path, text='', values=[path, f'{"  " * depth}{key}', dv], tags=tags)

    def _parameter_display_value(self, v: ocproc2.AbstractValue):
        tags = []
        wq = v.metadata.best_value('WorkingQuality', None)
        if wq is not None and not isinstance(wq, int):
            try:
                wq = int(wq)
            except ValueError:
                wq = None
        if wq is not None and wq in ParameterPane.TAG_MAP:
            tags.append(ParameterPane.TAG_MAP[wq])
        if v.is_empty():
            return '|empty|', tags
        if v.is_iso_datetime():
            dt_utc = datetime.datetime.fromtimestamp(v.to_datetime().timestamp(), datetime.timezone.utc)
            return dt_utc.strftime('%Y-%m-%d %H:%M:%S [UTC]'), tags
        if v.is_numeric():
            val = v.to_float() if not v.is_integer() else v.to_int()
            units = v.metadata.best_value('Units', None)
            if units is not None:
                val = f"{str(val)} {v.metadata.best_value('Units')}"
            else:
                val = str(val)
            return val, tags
        return v.to_string(), tags

    def _on_parameter_right_click(self, item, event):
        pcm = ParameterContextMenu(self.app, item['values'][0])
        pcm.handle_popup_click(event)


