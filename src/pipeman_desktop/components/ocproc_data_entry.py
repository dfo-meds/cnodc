import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd
import typing as t
from medsutil import ocproc2
import gcapp.i18n.base as i18n
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.iso_duration import ISODuration
from pipeman_desktop.i18n import OCProc2Translator

InputType = int | AwareDateTime | str | None

def ask_ocproc2(*args, **kwargs) -> InputType:
    dialog = OCProc2Entry(*args, **kwargs)
    res = dialog.result
    return res


class OCProc2Entry(tksd.Dialog):

    ocproc_translator: OCProc2Translator = None

    @injector.construct
    def __init__(self,
                 parent,
                 element_name: str,
                 data_type: str,
                 min_value: float | int | None = None,
                 max_value: float | int | None = None,
                 allowed_values: list[str | int] | None = None,
                 current_element: ocproc2.SingleElement | None = None,
                 button_label: t.Optional[str] = None):
        self.button_label = button_label or i18n.tr('choice_dialog_ok')
        self.element_name = element_name
        self.data_type = data_type
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values
        self.current_element = current_element
        self.result_var = tk.StringVar(value=self._default_value())
        self.result: InputType = None
        title = self.ocproc_translator.translate_element_name(element_name)
        super().__init__(parent=parent, title=title)
        self.parent = None

    def _default_value(self) -> str:
        if self.current_element is None or self.current_element.is_empty():
            return ""
        elif self.data_type == "dateTimeStamp":
            return self.current_element.to_datetime().isoformat()
        elif self.data_type == "date":
            return self.current_element.to_date().isoformat()
        elif self.data_type == "integer":
            return str(self.current_element.to_int())
        else:
            return self.current_element.to_string()

    def _build_prompt_info(self) -> str:
        prompt = []
        desc = self.ocproc_translator.translate_element_description(self.element_name)
        if desc:
            prompt.append(desc)
        units = self.current_element.units()
        if units:
            prompt.append(i18n.tr("ocproc2_entry_units", units=units))
        if self.data_type in {"dateTimeStamp", "date"}:
            prompt.append(i18n.tr("ocproc2_entry_time_prompt"))
        if self.min_value is not None and self.max_value is not None:
            prompt.append(i18n.tr("ocproc2_entry_range", min_value=self.min_value, max_value=self.max_value))
        elif self.min_value is not None:
            prompt.append(i18n.tr("ocproc2_entry_min", min_value=self.min_value))
        elif self.max_value is not None:
            prompt.append(i18n.tr("ocproc2_entry_max", max_value=self.max_value))
        return "\n".join(prompt)

    def _build_control(self, parent):
        if self.allowed_values is not None:
            return ttk.Combobox(
                parent,
                textvariable=self.result_var,
                values=[str(x) for x in self.allowed_values]
            )
        else:
            return ttk.Entry(
                parent,
                textvariable=self.result_var,
            )

    def body(self, parent):
        prompt = self._build_prompt_info()
        if prompt:
            label = ttk.Label(parent, text=prompt, wraplength=300, justify="left")
            label.pack(expand=True, fill="both", padx=2, pady=2)
        control = self._build_control(parent)
        control.pack(expand=True, fill="both", padx=2, pady=2)
        return control

    def validate(self) -> bool:
        result = self.result_var.get()
        if self.allowed_values is not None:
            if result in self.allowed_values:
                return True
            if result.isdigit() and int(result) in self.allowed_values:
                return True
            return False
        elif self.data_type == "integer":
            return result.isdigit()
        elif self.data_type in {"dateTimeStamp", "date"}:
            try:
                _ = AwareDateTime.fromisoformat(result)
                return True
            except ValueError:
                return False
        elif self.data_type == "decimal":
            try:
                _ = float(result)
                return True
            except ValueError:
                return False
        elif self.data_type == "Duration":
            try:
                _ = ISODuration.from_iso_format(result)
                return True
            except ValueError:
                return False
        else:
            return True

    def _get_value(self) -> str | int | AwareDateTime | None:
        result = self.result_var.get()
        if self.allowed_values is not None:
            if result in self.allowed_values:
                return result
            if result.isdigit() and int(result) in self.allowed_values:
                return int(result)
            return None
        elif self.data_type == "integer":
            return int(result)
        elif self.data_type in {"date", "dateTimeStamp"}:
            return AwareDateTime.fromisoformat(result).isoformat()
        else:
            # note: we return floats as strings to preserve the user's intention on precision here
            # empty strings become None again
            return result or None

    def apply(self):
        self.result = self._get_value()
