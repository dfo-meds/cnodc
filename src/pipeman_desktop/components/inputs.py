import datetime
import tkinter
import tkinter.ttk as ttk
import typing as t
from tkinter.simpledialog import Dialog

from gcapp import i18n
from medsutil.awaretime import AwareDateTime
from pipeman_desktop.components.tooltip import Tooltip


class Validator:

    def __call__(self, value: t.Any) -> str | None:
        ...


class Required(Validator):

    def __call__(self, value: t.Any) -> str | None:
        if not value:
            return i18n.tr("error.validation.required")
        return None


class DateTimeValidator(Validator):

    def __call__(self, value: t.Any) -> str | None:
        try:
            if value:
                _ = AwareDateTime.fromisoformat(value)
            return None
        except (KeyError, ValueError, TypeError):
            return i18n.tr("error.validation.datetime")

class RangeValidator(Validator):

    def __init__(self,
                 min_value: int | float | None = None,
                 max_value: int | float | None = None):
        self._min_value = min_value
        self._max_value = max_value

    def __call__(self, value: t.Any) -> str | None:
        try:
            if value:
                x = float(value)
                if self._min_value is not None and x < self._min_value:
                    return i18n.tr("error.validation.too_small", min_value=self._min_value)
                if self._max_value is not None and x > self._max_value:
                    return i18n.tr("error.validation.too_large", max_value=self._max_value)
            return None
        except (KeyError, ValueError, TypeError):
            return i18n.tr("error.validation.float")


class IntegerValidator(Validator):

    def __call__(self, value: t.Any) -> str | None:
        try:
            if value:
                _ = int(value)
                return None
        except (KeyError, ValueError, TypeError):
            return i18n.tr("error.validation.integer")


class FloatValidator(Validator):

    def __call__(self, value: t.Any) -> str | None:
        try:
            if value:
                _ = int(value)
                return None
        except (KeyError, ValueError, TypeError):
            return i18n.tr("error.validation.float")


class LengthValidator(Validator):

    def __init__(self,
                 min_length: int | None = None,
                 max_length: int | None = None):
        self._min_length = min_length
        self._max_length = max_length

    def __call__(self, value: t.Any) -> str | None:
        str_len = len(str(value))
        if self._min_length is not None and str_len < self._min_length:
            return i18n.tr("error.validation.too_short", min_length=self._min_length)
        if self._max_length is not None and str_len > self._max_length:
            return i18n.tr("error.validation.too_long", max_length=self._max_length)
        return None


class InputField:

    def __init__(self, *, same_row_as: str | None = None, order: int | None = None, default: t.Any = None, validators: list[Validator] | None = None):
        self.default = None
        self._validators = validators or []
        self.order = order
        self.same_row_as = same_row_as
        if default is not None:
            self.set_value(default)

    def set_value(self, d: t.Any):
        self.default = d

    def build(self, parent, row: int):
        raise NotImplementedError

    def validate(self) -> list[str]:
        errors = []
        value = self.raw_value()
        for validator in self._validators:
            result = validator(value)
            if result is not None:
                errors.append(result)
        self.mark_errored(len(errors) > 0)
        return errors

    def mark_errored(self, has_errors: bool):
        pass

    def raw_value(self) -> t.Any:
        raise NotImplementedError

    def value(self) -> t.Any:
        return self.raw_value()


class FieldWithLabel(InputField):

    def __init__(self,
                 *,
                 label_name: str | None = None,
                 tooltip_name: str | None = None,
                 col_offset: int = 0,
                 control_colspan: int = 2,
                 no_label: bool = False,
                 **kwargs):
        super().__init__(**kwargs)
        self._col_offset = col_offset
        self._tooltip_name = tooltip_name
        self._label_name = label_name
        self._label = None
        self._control = None
        self._no_label = no_label
        self._colspan = control_colspan
        self._tooltip = None

    def build(self, parent, row: int):
        offset = 0
        if self._label_name is not None and not self._no_label:
            self._label = ttk.Label(parent, text=i18n.tr(self._label_name))
            self._label.grid(row=row, column=self._col_offset, sticky="EW")
            offset += 1
        self._control = self.control(parent)
        self._control.grid(row=row, column=self._col_offset + offset, sticky="EW", columnspan=self._colspan)
        if self._tooltip_name is not None:
            self._tooltip = Tooltip(self._control, i18n.tr(self._tooltip_name))

    def mark_errored(self, has_errors: bool):
        if isinstance(self._control, BorderedControl):
            self._control.set_errored(has_errors)

    def control(self, parent) -> tkinter.Frame | tkinter.ttk.Frame:
        raise NotImplementedError


class CheckboxField(FieldWithLabel):

    def __init__(self, no_label: bool = True, **kwargs):
        super().__init__(**kwargs, no_label=no_label)
        self.intvar = tkinter.IntVar(value=bool(self.default))

    def raw_value(self) -> bool:
        return bool(self.intvar.get())

    def control(self, parent):
        return BorderedCheckbox(parent, text=i18n.tr(self._label_name), variable=self.intvar, onvalue=1, offvalue=0)


class StringField(FieldWithLabel):

    def __init__(self, **kwargs):
        self.textvar = tkinter.StringVar()
        super().__init__(**kwargs)

    def set_value(self, d: t.Any):
        self.textvar.set(str(d) if d is not None else "")

    def raw_value(self) -> str | None:
        return self.textvar.get() or None

    def control(self, parent):
        return BorderedEntry(parent, textvariable=self.textvar)


class IntegerField(StringField):

    def __init__(self, **kwargs):
        if "validators" not in kwargs:
            kwargs["validators"] = []
        kwargs["validators"].append(IntegerValidator())
        super().__init__(**kwargs)

    def value(self) -> int | None:
        x = self.raw_value()
        if x:
            return int(x)
        return None


class FloatField(StringField):

    def __init__(self, **kwargs):
        if "validators" not in kwargs:
            kwargs["validators"] = []
        kwargs["validators"].append(FloatValidator())
        super().__init__(**kwargs)

    def value(self) -> float | None:
        x = self.raw_value()
        if x:
            return float(x)
        return None


class DateTimeField(StringField):

    def __init__(self, **kwargs):
        if "validators" not in kwargs:
            kwargs["validators"] = []
        kwargs["validators"].append(DateTimeValidator())
        super().__init__(**kwargs)

    def set_value(self, d: t.Any):
        if isinstance(d, datetime.date):
            super().set_value(d.isoformat())
        else:
            super().set_value(d)

    def value(self) -> AwareDateTime | None:
        x = self.raw_value()
        if x:
            return AwareDateTime.fromisoformat(x, tzinfo="Etc/UTC")
        return None


class SelectField(StringField):

    def __init__(self, *, options: dict[str, str] | list[str], **kwargs):
        all_options = [(k, v) for k, v in options.items()] if isinstance(options, dict) else [(k, k) for k in options]
        all_options.sort(key=lambda x: x[1])
        self._display_options = [x[1] for x in all_options]
        self._values = [x[0] for x in all_options]
        super().__init__(**kwargs)

    def set_value(self, d: t.Any):
        if d in self._display_options:
            super().set_value(d)
        elif d in self._values:
            super().set_value(self._display_options[self._values.index(d)])
        else:
            super().set_value("")

    def value(self) -> str | None:
        value = super().value()
        if value is not None and value in self._display_options:
            return self._values[self._display_options.index(value)]
        return None

    def control(self, parent):
        return BorderedChoice(parent, values=self._display_options, textvariable=self.textvar)


class BorderedControl(ttk.Frame):

    def __init__(self, parent):
        super().__init__(parent, style='BorderedEntry.TFrame')
        self.entry = None

    def set_widget(self, widget):
        self.entry = widget
        self.entry.pack(padx=2, pady=2, sticky="EW")

    def set_errored(self, is_error: bool):
        if is_error:
            self.configure(style='Errored.BorderedEntry.TFrame')
        else:
            self.configure(style='BorderedEntry.TFrame')



class BorderedEntry(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent)
        self.set_widget(ttk.Entry(self, *args, **kwargs))


class BorderedChoice(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent)
        self.set_widget(ttk.Combobox(self, *args, **kwargs))


class BorderedCheckbox(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent)
        self.set_widget(ttk.Checkbutton(self, *args, **kwargs))


class FormDialog(Dialog):

    def __init__(self,
                 parent,
                 title: str,
                 defaults: dict[str, t.Any] | None = None,
                 readonly: bool = False):
        self.result = None
        self._defaults = defaults or {}
        self._fields: dict[str, InputField] = {}
        self._readonly = readonly
        self.fields()
        self._error_label: ttk.Label | None = None
        self._error_label_text = tkinter.StringVar(parent, value="")
        super().__init__(parent, title)

    def value(self, field_name):
        return self._fields[field_name].value()

    def body(self, parent):
        later = []
        fields = [(field.order, key) for key, field in self._fields.items()]
        fields.sort(key=lambda x: x[0])
        row = 0
        row_map = {}
        for _, field_name in fields:
            row_map[field_name] = row
            field = self._fields[field_name]
            if field.same_row_as:
                later.append(field_name)
            else:
                field.build(parent, row)
                row += 1
        for field_name in later:
            field = self._fields[field_name]
            field.build(parent, row_map[field.same_row_as])
        self._error_label = ttk.Label(parent, textvariable=self._error_label_text)

    def validate(self):
        errors = []
        for field in self._fields.values():
            errors.extend(field.validate())
        res = self.custom_validation()
        if res:
            errors.extend(res)
        if errors:
            self._error_label_text.set("\n".join(errors))
            return False
        else:
            self._error_label_text.set("")
            return True

    def custom_validation(self) -> list[str] | None:
        return None

    def add_field(self, name: str, field: InputField):
        if field.default is None and name in self._defaults:
            field.set_value(self._defaults[name])
        if field.order is None:
            field.order = max(f.order or 0 for f in self._fields.values()) if self._fields else 0
        self._fields[name] = field

    def apply(self):
        data = {
            name: field.value()
            for name, field in self._fields.items()
        }
        self.alter_data(data)
        self.result = data

    def alter_data(self, data: dict):
        pass

    def fields(self):
        raise NotImplementedError
