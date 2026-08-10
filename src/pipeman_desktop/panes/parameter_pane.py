from medsutil.ocproc2 import ElementMap
from pipeman_desktop.components.ocproc_data_entry import ask_ocproc2, InputType
from pipeman_desktop.i18n import OCProc2Translator
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.choice_dialog import ask_choice
from pipeman_desktop.components.scrollable import ScrollableTreeview
import gcapp.i18n.base as i18n
import typing as t
import tkinter as tk
import tkinter.ttk as ttk

import medsutil.ocproc2 as ocproc2
from autoinject import injector

from pipeman_desktop.util import quality_color

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop


class ParameterContextMenu:

    ocproc_translator: OCProc2Translator = None

    @injector.construct
    def __init__(self,
                 app: PipemanDesktop,
                 target_path,
                 element_info: t.Optional[ocproc2.OCProc2ElementInfo],
                 element_name: str,
                 current_units: str | None = None,
                 current_value: ocproc2.SingleElement | None = None):
        self._current_value: ocproc2.SingleElement | None = current_value
        self._element_name = element_name
        self._app = app
        self._element_info = element_info
        self._current_units = current_units or (element_info.preferred_unit if element_info else '')
        self._target_path = target_path
        self._menu = tk.Menu(app.root, tearoff=0)
        if self._element_info is not None and self._element_info.data_type in {"string", "integer", "date", "decimal", "Duration", "dateTimeStamp"}:
            self._menu.add_command(
                label=i18n.tr('context_menu.parameter.edit'),
                command=self._edit_value
            )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_unchecked'),
            command=self._flag_unchecked
        )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_good'),
            command=self._flag_good
        )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_probably_good'),
            command=self._flag_probably_good
        )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_dubious'),
            command=self._flag_dubious
        )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_erroneous'),
            command=self._flag_erroneous
        )
        self._menu.add_command(
            label=i18n.tr('context_menu.parameter.flag_missing'),
            command=self._flag_missing
        )

    def _edit_value(self):
        new_value = self._edit_choice()
        if new_value is not None:
            self._app.state.add_action(
                ocproc2.ChangeValue(
                    path=self._target_path,
                    new_value=new_value,
                    test_protocol=self._app.state.test_protocol
                )
            )

    def _edit_choice(self) -> InputType:
        data_type = self._element_info.data_type if self._element_info is not None else None
        if data_type is None:
            data_type = ask_choice(
                title=i18n.tr('dialog.choose_data_type.title'),
                prompt=i18n.tr('dialog.choose_data_type.message'),
                parent=self._app.root,
                options={
                    'string': i18n.tr('data_type.string'),
                    'integer': i18n.tr('data_type.integer'),
                    'dateTimeStamp': i18n.tr('data_type.datetime'),
                    'date': i18n.tr('data_type.date'),
                    'decimal': i18n.tr('data_type.decimal'),
                    'Duration': i18n.tr('data_type.duration'),
                }
            )
            if data_type is not None:
                # We need for tk to process the focus event from ask_choice
                # before we give it another prompt.
                self._app.root.update()
            else:
                return None
        return ask_ocproc2(
            parent=self._app.root,
            element_name=self._element_name,
            data_type=data_type,
            min_value=self._element_info.min_value if self._element_info is not None else None,
            max_value=self._element_info.max_value if self._element_info is not None else None,
            allowed_values=self._element_info.allowed_values if self._element_info is not None else None,
            current_element=self._current_value,
        )

    def _flag_unchecked(self):
        self._set_working_quality_flag(0)

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
        self._app.state.add_action(ocproc2.ChangeQuality(
            path=self._target_path,
            new_flag=flag_no,
            test_protocol=self._app.state.test_protocol
        ))

    def handle_popup_click(self, e):
        try:
            self._menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            self._menu.grab_release()


class ParameterPane(BasePane):

    ontology: ocproc2.OCProc2Ontology = None
    ocproc_translator: OCProc2Translator = None

    HIDE_ELEMENTS = {
        "WorkingQuality",
        "Units",
        "CNODCPlatformCandidates",
    }

    READ_ONLY_ELEMENTS = {
        "CNODCPlatform",
    }

    TAG_MAP = {
        -1: 'invalid',
        1: 'good',
        2: 'probably-good',
        3: 'dubious',
        4: 'erroneous',
        5: 'modified',
        9: 'missing',
    }

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parameter_list: t.Optional[ScrollableTreeview] = None
        self._value_lookup: dict[str, ocproc2.SingleElement] = {}
        self._parameter_name_lookup: dict[str, str] = {}

    def on_init(self):
        param_frame = ttk.Frame(self.app.batch_right)
        param_frame.rowconfigure(0, weight=1)
        param_frame.columnconfigure(0, weight=1)
        param_frame.grid(row=0, column=0, sticky='NSEW')
        self._parameter_list = ScrollableTreeview(
            parent=param_frame,
            selectmode='browse',
            show="tree headings",
            columns=["name", "value", "units", "quality"],
            on_right_click=self._on_parameter_right_click
        )
        self._parameter_list.set_header_text("name", i18n.tr('tree.parameter_list.name'))
        self._parameter_list.set_header_text("value", i18n.tr('tree.parameter_list.value'))
        self._parameter_list.set_header_text("units", i18n.tr('tree.parameter_list.units'))
        self._parameter_list.set_header_text("quality", i18n.tr('tree.parameter_list.quality'))
        self._parameter_list.tag_configure('header', background='#000000', foreground='#FFFFFF')
        self._parameter_list.tag_configure('alt', background='#EEEEEE')
        self._parameter_list.tag_configure('invalid', foreground='red')
        self._parameter_list.tag_configure('good', foreground=quality_color(1))
        self._parameter_list.tag_configure('probably-good', foreground=quality_color(2))
        self._parameter_list.tag_configure('dubious', foreground=quality_color(3))
        self._parameter_list.tag_configure('erroneous', foreground=quality_color(4))
        self._parameter_list.tag_configure('modified', foreground=quality_color(5))
        self._parameter_list.tag_configure('missing', foreground=quality_color(9))
        self._parameter_list.tag_configure('invalid', foreground=quality_color(-1))
        self._parameter_list.grid(row=0, column=0, sticky='NSEW')
        self._parameter_list.table.heading("#1", anchor="w")
        self._parameter_list.table.heading("#2", anchor="e")
        self._parameter_list.table.heading("#3", anchor="w")
        self._parameter_list.table.column('#0', width=50, stretch=tk.NO)
        self._parameter_list.table.column('#1', width=150, anchor='w')
        self._parameter_list.table.column('#2', width=150, anchor='e')
        self._parameter_list.table.column('#3', width=75, anchor='w')
        self._parameter_list.table.column('#4', width=25, stretch=tk.NO)

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD_CHILD | DisplayChange.RECORD):
            self._rebuild_parameter_list()
        if change_type & DisplayChange.LANGUAGE:
            if self._parameter_list is not None:
                self._parameter_list.set_header_text("name", i18n.tr('tree.parameter_list.name'))
                self._parameter_list.set_header_text("value", i18n.tr('tree.parameter_list.value'))
                self._parameter_list.set_header_text("units", i18n.tr('tree.parameter_list.units'))
                self._parameter_list.set_header_text("quality", i18n.tr('tree.parameter_list.quality'))
                self._rebuild_parameter_list()

    def _rebuild_parameter_list(self):
        self._value_lookup.clear()
        self._parameter_name_lookup.clear()
        if self._parameter_list is not None:
            self._parameter_list.clear_items()
            if self.app.state.current_recordset is not None:
                self.show_recordset(self.app.state.current_recordset, self.app.state.current_child_path)
            elif self.app.state.current_record is not None:
                self.show_record(self.app.state.current_record, self.app.state.current_child_path)

    def show_record(self, record: ocproc2.BaseRecord, path: str):
        self._build_from_element_map(record.coordinates, path, "coordinates", True)
        self._build_from_element_map(record.parameters, path, "parameters", True)
        self._build_from_element_map(record.metadata, path, "metadata", True)

    def show_recordset(self, record_set: ocproc2.RecordSet, path: str):
        self._build_from_element_map(record_set.metadata, path, "metadata", True)

    def _build_from_element_map(self, element_map: ElementMap, path: str, map_name: str, open_header: bool = False):
        keys = [x for x in element_map.keys() if x not in self.HIDE_ELEMENTS]
        if keys:
            map_path = self._create_parameter_header(path, map_name)
            is_alt = False
            for k in keys:
                self._create_parameter_entry(element_map[k], map_path, k, is_alt=is_alt)
                is_alt = not is_alt
            if open_header:
                self._parameter_list.open_item(map_path)

    def _create_parameter_header(self, path: str, header_name: str) -> str:
        m_path = f'{path}/{header_name}' if path else header_name
        self._parameter_list.append_item(
            iid=m_path,
            values=(self.ocproc_translator.translate_element_type(header_name), '', '', ''),
            tags=('header',)
        )
        return m_path

    def _create_parameter_entry(self, v: ocproc2.AbstractElement, parent_path: str, key: str, depth: int = 1, param_name: str = None, is_alt: bool = False):
        if param_name is None:
            param_name = key
        my_path = f'{parent_path}/{key}'
        label = f"#{key}" if key.isdigit() else self.ocproc_translator.translate_element_name(key)
        if isinstance(v, ocproc2.MultiElement):
            is_alt = False
            self._parameter_list.append_item(
                iid=my_path,
                values=(label, '', '', ''),
                tags=('header',)
            )
            for idx, subv in v.values():
                self._create_parameter_entry(subv, my_path, str(idx), depth + 1, param_name, is_alt)
                is_alt = not is_alt
        elif isinstance(v, ocproc2.SingleElement):
            self._create_parameter_list_item(v, parent_path, key, label, depth, param_name, is_alt)

    def _create_parameter_list_item(self, v: ocproc2.SingleElement, parent_path: str, key: str, label: str, depth: int, parameter_name: str, is_alt: bool):
        path = f'{parent_path}/{key}'
        dv, tags = self._parameter_display_value(v)
        if is_alt:
            tags.append('alt')
        self._parameter_list.append_item(
            parent=parent_path,
            iid=path,
            values=(f'{"  " * depth}{label}', *dv),
            tags=tuple(tags)
        )
        self._value_lookup[path] = v
        self._parameter_name_lookup[path] = parameter_name
        is_alt = False
        for m_name, md in v.metadata.items():
            if m_name not in self.HIDE_ELEMENTS:
                self._create_parameter_entry(md, path, m_name, depth + 1, is_alt=is_alt)
                is_alt = not is_alt

    def _parameter_display_value(self, v: ocproc2.AbstractElement) -> tuple[tuple, list]:
        tags = []
        wq = v.metadata.best('WorkingQuality', default=0, coerce=int)
        if wq is not None and wq in ParameterPane.TAG_MAP:
            tags.append(ParameterPane.TAG_MAP[wq])
        if v.is_empty():
            return ('', '', wq), tags,
        elif v.is_iso_datetime():
            dt_utc = v.to_datetime().astimezone("Etc/UTC")
            precision = v.metadata.best('DatePrecision', default=None, coerce=str)
            dt_format = "%Y-%m-%d %H:%M:%S"
            if precision == "minute":
                dt_format = "%Y-%m-%d %H:%M"
            elif precision == "hour":
                dt_format = "%Y-%m-%d %H"
            elif precision == "day":
                dt_format = "%Y-%m-%d"
            return (dt_utc.strftime(dt_format), 'UTC', wq), tags
        elif v.is_numeric():
            if v.is_integer():
                val = str(v.to_int())
            elif v.metadata.has_value("Uncertainty"):
                sn = v.to_scinum()
                val = sn.to_places_as_string(sn.significant_digits() + 1)
            else:
                val = f"{v.to_float():.9f}"
                if "." in val:
                    val = val.rstrip("0")
                    if val.endswith("."):
                        val = val + "0"
            units = v.metadata.best('Units', None, coerce=str)
            if units is not None:
                return (val, v.metadata.best('Units'), wq), tags
            else:
                return (val, '', wq), tags
        else:
            return (v.to_string(), '', wq), tags

    def _on_parameter_right_click(self, item, event):
        if item['iid'] not in self._parameter_name_lookup:
            return
        parameter_name = self._parameter_name_lookup[item['iid']]
        if parameter_name in self.READ_ONLY_ELEMENTS:
            return
        pcm = ParameterContextMenu(
            self.app,
            item['iid'],
            self._get_element_info(parameter_name),
            self.app.state.username,
            item['values'][3],
            self._value_lookup[item['iid']]
        )
        pcm.handle_popup_click(event)

    def _get_element_info(self, parameter_name: str) -> t.Optional[ocproc2.OCProc2ElementInfo]:
        return self.ontology.info(parameter_name)

