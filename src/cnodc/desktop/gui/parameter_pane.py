import datetime
import tkinter.simpledialog
import tkcalendar as tkc
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange
from cnodc.desktop.gui.choice_dialog import ask_choice
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import cnodc.desktop.translations as i18n
import cnodc.ocproc2.structures as ocproc2
import cnodc.ocproc2.operations as ops
import tkinter.messagebox as tkmb
import typing as t
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd

from cnodc.desktop import VERSION
from cnodc.ocproc2.validation import OCProc2Ontology, OCProc2ElementInfo
from cnodc.desktop.gui.date_time_dialog import ask_date, ask_time, ask_datetime
from autoinject import injector


class ParameterContextMenu:

    def __init__(self,
                 app,
                 target_path,
                 element_info: t.Optional[OCProc2ElementInfo],
                 current_user: str,
                 current_units: t.Optional[str] = None,
                 current_value: t.Optional = None):
        self._current_value: ocproc2.Value = current_value
        self._current_user = current_user
        self._app = app
        self._element_info = element_info
        self._current_units = current_units or (element_info.preferred_unit if element_info else '')
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
        new_value = self._edit_choice()
        if new_value is not None:
            self._app.save_operations([
                ops.QCSetValue(self._target_path, new_value, children=[
                    ops.QCSetWorkingQuality(self._target_path, 5),
                    ops.QCAddHistory(
                        f"CHANGE [{self._target_path}] FROM [{self._current_value.to_string()}] TO [{str(new_value)}]",
                        "operator_qc",
                        VERSION,
                        self._current_user,
                        message_type=ocproc2.MessageType.INFO.value
                    )
                ])
            ])

    def _edit_choice(self):
        title = ''
        prompt = ''
        unit_str = '' if self._current_units is None else f' [{self._current_units}]'
        if self._element_info is not None:
            title = self._element_info.label(i18n.current_language())
            prompt = [
                f"{self._element_info.documentation(i18n.current_language())}{unit_str}"
            ]
            if self._element_info.max_value is not None or self._element_info.min_value is not None:
                if self._element_info.max_value is None:
                    prompt.append(i18n.get_text('prompt_min_value', min=str(self._element_info.min_value)))
                elif self._element_info.min_value is None:
                    prompt.append(i18n.get_text('prompt_max_value', max=str(self._element_info.max_value)))
                else:
                    prompt.append(i18n.get_text('prompt_range', min=str(self._element_info.min_value), max=str(self._element_info.max_value)))
            prompt = "\n".join(prompt)
            if self._element_info.allowed_values:
                return ask_choice(
                    title=title,
                    prompt=prompt,
                    default=self._current_value.value,
                    parent=self._app.root,
                    options={x: str(x) for x in self._element_info.allowed_values}
                )
        return self._prompt_for_data_type(title, prompt, self._element_info.data_type if self._element_info else None)

    def _prompt_for_data_type(self, title, prompt, data_type):
        if data_type == 'decimal':
            return tksd.askfloat(
                title=title,
                prompt=prompt,
                initialvalue=self._current_value.to_float() if self._current_value.is_numeric() else None,
                minvalue=self._element_info.min_value if self._element_info is not None else None,
                maxvalue=self._element_info.max_value if self._element_info is not None else None
            )
        elif data_type == 'integer':
            return tksd.askinteger(
                title=title,
                prompt=prompt,
                initialvalue=self._current_value.to_int() if self._current_value.is_integer() else None,
                minvalue=self._element_info.min_value if self._element_info is not None else None,
                maxvalue=self._element_info.max_value if self._element_info is not None else None
            )
        elif data_type == 'string':
            return tksd.askstring(
                title=title,
                initialvalue=self._current_value.to_string(),
                prompt=prompt
            )
        elif data_type == 'dateTimeStamp':
            return ask_datetime(
                parent=self._app.root,
                default=self._current_value.to_datetime() if self._current_value.is_iso_datetime() else None,
                title=title,
                prompt=prompt
            )
        elif data_type == 'date':
            return ask_date(
                parent=self._app.root,
                default=self._current_value.to_datetime() if self._current_value.is_iso_datetime() else None,
                title=title,
                prompt=prompt
            )
        elif data_type is None:
            data_type = ask_choice(
                title=i18n.get_text('data_type_choice_title'),
                prompt=i18n.get_text('data_type_choice_prompt'),
                parent=self._app.root,
                options={
                    'string': i18n.get_text('data_type_string'),
                    'integer': i18n.get_text('data_type_integer'),
                    'dateTimeStamp': i18n.get_text('data_type_datetime'),
                    'date': i18n.get_text('data_type_date'),
                    'decimal': i18n.get_text('data_type_decimal')
                }
            )
            if data_type is not None:
                # We need for tk to process the focus event from ask_choice
                # before we give it another prompt.
                self._app.root.update()
                return self._prompt_for_data_type(title, prompt, data_type)
            else:
                return None
        else:
            tkmb.showwarning(
                title=i18n.get_text('data_type_not_supported_title'),
                message=i18n.get_text('data_type_not_supported_message', data_type=data_type)
            )

    def _flag_dubious(self):
        self._set_working_quality_flag(3)

    def _flag_erroneous(self):
        self._set_working_quality_flag(4)

    def _flag_missing(self):
        self._set_working_quality_flag(9)

    def _flag_good(self):
        self._set_working_quality_flag(1)

    def _flag_probably_good(self):
        self._set_working_quality_flag(2)

    def _set_working_quality_flag(self, flag_no: int):
        cwq = self._current_value.metadata.best_value('WorkingQuality', 0)
        if int(cwq) != flag_no:
            self._app.save_operations([
                self._app.create_flag_operator(self._target_path, flag_no)
            ])

    def handle_popup_click(self, e):
        try:
            self._menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            self._menu.grab_release()


class ParameterPane(BasePane):

    ontology: OCProc2Ontology = None

    TAG_MAP = {
        1: 'good',
        2: 'probably-good',
        3: 'dubious',
        4: 'erroneous',
        9: 'missing',
        12: 'recommend-probably-good',
        13: 'recommend-dubious',
        14: 'recommend-erroneous',
        19: 'recommend-missing',
        20: 'invalid',
        21: 'invalid',
    }

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parameter_list: t.Optional[ScrollableTreeview] = None
        self._value_lookup: dict[str, ocproc2.Value] = {}

    def on_init(self):
        param_frame = ttk.Frame(self.app.bottom_notebook)
        param_frame.rowconfigure(0, weight=1)
        param_frame.columnconfigure(0, weight=1)
        self._parameter_list = ScrollableTreeview(
            parent=param_frame,
            selectmode='browse',
            show="tree headings",
            headers=[
                '',
                i18n.get_text('parameter_list_name'),
                i18n.get_text('parameter_list_value'),
                i18n.get_text('parameter_list_units'),
                i18n.get_text('parameter_list_quality'),
            ],
            on_right_click=self._on_parameter_right_click,
            displaycolumns=(1, 2, 3, 4)
        )
        self._parameter_list.tag_configure('header', background='#000000', foreground='#FFFFFF')
        self._parameter_list.tag_configure('alt', background='#EEEEEE')
        self._parameter_list.tag_configure('invalid', foreground='red')
        self._parameter_list.tag_configure('good', foreground=self.app.quality_color(1))
        self._parameter_list.tag_configure('probably-good', foreground=self.app.quality_color(2))
        self._parameter_list.tag_configure('dubious', foreground=self.app.quality_color(3))
        self._parameter_list.tag_configure('erroneous', foreground=self.app.quality_color(4))
        self._parameter_list.tag_configure('missing', foreground=self.app.quality_color(9))
        self._parameter_list.tag_configure('recommend-probably-good', foreground=self.app.quality_color(12))
        self._parameter_list.tag_configure('recommend-dubious', foreground=self.app.quality_color(13))
        self._parameter_list.tag_configure('recommend-erroneous', foreground=self.app.quality_color(14))
        self._parameter_list.tag_configure('recommend-missing', foreground=self.app.quality_color(19))
        self._parameter_list.tag_configure('invalid', foreground=self.app.quality_color(20))
        self._parameter_list.grid(row=0, column=0, sticky='NSEW')
        self._parameter_list.table.column('#0', width=35, stretch=tk.NO)
        self._parameter_list.table.column('#1', width=150, anchor='w')
        self._parameter_list.table.column('#2', width=150, anchor='w')
        self._parameter_list.table.column('#3', width=75, anchor='e')
        self._parameter_list.table.column('#4', width=22, stretch=tk.NO)
        self.app.bottom_notebook.add(param_frame, text='Properties', sticky='NSEW')

    def on_language_change(self):
        # TODO: parameter list headings
        self._rebuild_parameter_list(self.app.app_state)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD_CHILD | DisplayChange.RECORD):
            self._rebuild_parameter_list(app_state)

    def _rebuild_parameter_list(self, app_state):
        if app_state.child_recordset is not None:
            self.show_recordset(app_state.child_recordset, app_state.subrecord_path)
        elif app_state.child_record is not None:
            self.show_record(app_state.child_record, app_state.subrecord_path)
        elif app_state.record is not None:
            self.show_record(app_state.record, app_state.subrecord_path)
        else:
            self._parameter_list.clear_items()

    def show_record(self, record: ocproc2.DataRecord, path: str):
        self._parameter_list.clear_items()
        self._value_lookup = {}
        if record.metadata:
            m_path = f'{path}/metadata' if path else 'metadata'
            self._parameter_list.table.insert('', 'end', open=True, iid=m_path, text='', values=['', i18n.get_text('metadata'), '', '', ''], tags=['header'])
            is_alt = False
            for k in record.metadata.keys():
                self._create_parameter_entry(record.metadata[k], m_path, k, is_alt=is_alt)
                is_alt = not is_alt
        if record.coordinates:
            c_path = f'{path}/coordinates' if path else 'coordinates'
            self._parameter_list.table.insert('', 'end', open=True, iid=c_path, text='', values=['', i18n.get_text('coordinates'), '', '', ''], tags=['header'])
            is_alt = False
            for k in record.coordinates.keys():
                self._create_parameter_entry(record.coordinates[k], c_path, k, is_alt=is_alt)
                is_alt = not is_alt
        if record.parameters:
            p_path = f'{path}/parameters' if path else 'parameters'
            self._parameter_list.table.insert('', 'end', open=True, iid=p_path, text='', values=['', i18n.get_text('parameters'), '', '', ''], tags=['header'])
            is_alt = False
            for k in record.parameters.keys():
                self._create_parameter_entry(record.parameters[k], p_path, k, is_alt=is_alt)
                is_alt = not is_alt

    def show_recordset(self, record_set: ocproc2.RecordSet, path: str):
        self._parameter_list.clear_items()
        if record_set.metadata:
            m_path = f'{path}/metadata'
            is_alt = False
            self._parameter_list.table.insert('', 'end', open=True, iid=m_path, text='', values=['', i18n.get_text('metadata'), '', '', ''], tags=['header'])
            for k in record_set.metadata.keys():
                self._create_parameter_entry(record_set.metadata[k], m_path, k, is_alt=is_alt)
                is_alt = not is_alt

    def _create_parameter_entry(self, v: ocproc2.AbstractValue, parent_path: str, key: str, depth: int = 1, is_alt: bool = False):
        if isinstance(v, ocproc2.MultiValue):
            is_alt = False
            # TODO: need a heading here (with translations)
            for idx, subv in v.values():
                self._create_parameter_entry(subv, f'{parent_path}/{key}/{idx}', str(idx), depth + 1, is_alt)
                is_alt = not is_alt
        elif isinstance(v, ocproc2.Value):
            self._create_parameter_list_item(v, parent_path, key, depth, is_alt)
        if v.metadata:
            is_alt = False
            for k in v.metadata:
                if k in ('Units', 'Quality', 'WorkingQuality'):
                    continue
                self._create_parameter_entry(v.metadata[k], f'{parent_path}/{key}', k, depth + 1, is_alt)
                is_alt = not is_alt

    def _create_parameter_list_item(self, v: ocproc2.Value, parent_path: str, key: str, depth: int, is_alt: bool):
        path = f'{parent_path}/{key}'
        dv, tags = self._parameter_display_value(v)
        if is_alt:
            tags.append('alt')
        self._value_lookup[path] = v
        # TODO: instead of using key, use a lookup of key (if not a number)
        self._parameter_list.table.insert(parent_path, 'end', iid=path, text='', values=[path, f'{"  " * depth}{key}', *dv], tags=tags)

    def _parameter_display_value(self, v: ocproc2.AbstractValue) -> tuple[tuple, list]:
        tags = []
        wq = v.metadata.best_value('WorkingQuality', 0)
        if wq is not None and not isinstance(wq, int):
            try:
                wq = int(wq)
            except ValueError:
                wq = 0
        if wq is not None and wq in ParameterPane.TAG_MAP:
            tags.append(ParameterPane.TAG_MAP[wq])
        if v.is_empty():
            return ('', '', wq), tags,
        if v.is_iso_datetime():
            dt_utc = datetime.datetime.fromtimestamp(v.to_datetime().timestamp(), datetime.timezone.utc)
            return (dt_utc.strftime('%Y-%m-%d %H:%M:%S'), 'UTC', wq), tags
        if v.is_numeric():
            val = v.to_float() if not v.is_integer() else v.to_int()
            units = v.metadata.best_value('Units', None)
            if units is not None:
                return (str(val), v.metadata.best_value('Units'), wq), tags
            else:
                return (str(val), '', wq), tags
        return (v.to_string(), '', wq), tags

    def _on_parameter_right_click(self, item, event):
        if item['values'][0] != '':
            pcm = ParameterContextMenu(
                self.app,
                item['values'][0],
                self._get_element_info(item['values'][0]),
                self.app.app_state.username,
                item['values'][3],
                self._value_lookup[item['values'][0]]
            )
            pcm.handle_popup_click(event)

    def _get_element_info(self, path: str) -> t.Optional[OCProc2ElementInfo]:
        elements = path.split('/')
        while elements[-1].isdigit():
            elements = elements[:-1]
        return self.ontology.element_info(elements[-1])

