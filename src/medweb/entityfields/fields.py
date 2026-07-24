import decimal
import typing as t
import datetime

import wtforms
from markupsafe import Markup, escape

from gcapp.i18n import TString, MLString, tr, format_date
from gcflask.widgets import FlatPickrWidget, Select2Widget
from medweb.entityfields.base import Field, NumberMixin, StringMixin


class BooleanField(Field):
    DATA_TYPE = "boolean"
    CONTROL_CLASS = wtforms.BooleanField

    def _display(self, v: bool | None) -> Markup:
        if v is None:
            return escape(tr("gcflask.common.na"))
        elif not v:
            return escape(tr("gcflask.common.no"))
        else:
            return escape(tr("gcflask.common.yes"))

    def _sanitize_value(self, value: str | bool | int | None) -> bool | None:
        if value is None:
            return None
        return not not value


class DateField(Field):
    DATA_TYPE = "date"
    CONTROL_CLASS = wtforms.DateField

    def __init__(self, *args, with_time: bool = False, with_calendar: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self._with_time = with_time
        self._with_calendar = with_calendar
        if self._with_time and self._with_calendar:
            self._storage_format = "%Y-%m-%dT%H:%M:%S"
        elif self._with_time:
            self._storage_format = "%H:%M"
        elif self._with_calendar:
            self._storage_format = "%Y-%m-%d"
        else:
            raise TypeError("Must specify with_time or with_calendar or both")

    def _display(self, v: datetime.date | None) -> Markup:
        return escape(format_date(v) if v is not None else tr("gcflask.common.na"))

    def _sanitize_value(self, value: str | datetime.date | None) -> datetime.date | None:
        if isinstance(value, str):
            if self._with_time:
                return datetime.datetime.strptime(value, self._storage_format)
            else:
                return datetime.date.strptime(value, self._storage_format)
        return value

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        return {
            "format": self._storage_format,
            "widget": FlatPickrWidget(
                with_time=self._with_time,
                with_calendar=True,
                placeholder=TString("gcflask.common.placeholder"),
            )
        }

    def _serialize(self, value: datetime.date | None) -> t.Any:
        if value is None:
            return None
        return value.isoformat()


class DateTimeField(DateField):
    DATA_TYPE = "datetime"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, with_time=True)


class TimeField(DateField):
    DATA_TYPE = "time"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, with_calendar=False, with_time=True)


class DecimalField(NumberMixin, Field):
    DATA_TYPE = "decimal"
    CONTROL_CLASS = wtforms.DecimalField

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        kwargs = {
            'places': self._config.get("places", 10)
        }
        rounding: str | None = self._config.get("rounding", None)
        if rounding and hasattr(decimal, rounding):
            kwargs["rounding"] = getattr(decimal, rounding)
        return kwargs

    def _sanitize_value(self, value: decimal.Decimal | str | None | int | float) -> decimal.Decimal | None:
        if value is None or value == "":
            return None
        v = decimal.Decimal(value) if isinstance(value, (str, int, float)) else value
        return t.cast(decimal.Decimal, round(v, self._config.get("places", 10)))

    def _display(self, v: decimal.Decimal | None) -> Markup:
        str_v = str(v) if v is not None else ''
        if "." in str_v:
            str_v = str_v.rstrip("0")
        elif str_v.startswith("0E"):
            str_v = "0"
        return escape(str_v)

    def _data_value(self, value: decimal.Decimal | None, **kwargs) -> t.Any:
        return self._display(value)


class EmailField(StringMixin, Field):
    DATA_TYPE = "email"
    CONTROL_CLASS = wtforms.EmailField


class FloatField(NumberMixin, Field):
    DATA_TYPE = "float"
    CONTROL_CLASS = wtforms.FloatField


class IntegerField(NumberMixin, Field):
    DATA_TYPE = "float"
    CONTROL_CLASS = wtforms.IntegerField


class StringField(StringMixin, Field):
    DATA_TYPE = "text"
    CONTROL_CLASS = wtforms.StringField


class TextAreaField(StringField):
    DATA_TYPE = "multitext"
    CONTROL_CLASS = wtforms.TextAreaField


class TelephoneField(StringMixin, Field):
    DATA_TYPE = "telephone"
    CONTROL_CLASS = wtforms.TelField


class URLField(StringMixin, Field):
    DATA_TYPE = "url"
    CONTROL_CLASS = wtforms.URLField


class ChoiceField[X](Field):
    DATA_TYPE = "choice"

    def __init__(self, *args, coerce_form: type[X] = str, **kwargs):
        super().__init__(*args, **kwargs)
        self._coerce_form: type = coerce_form
        self._values = None
        self._use_default_repeatable = False

    def _control_class(self) -> t.Callable:
        if self.is_repeatable:
            return wtforms.SelectMultipleField
        return wtforms.SelectField

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        return {
            'choices': self.choices,
            'coerce': self._coerce_form,
            'widget': Select2Widget(
                allow_multiple=self.is_repeatable,
                placeholder=TString("gcflask.common.placeholder")
            ) if self._allow_javascript_controls() else None
        }

    def _find_choice(self, value: X | None) -> tuple[X | None, str | MLString | TString | None]:
        if value is None or value == "":
            return None, None
        for short, long in self.choices():
            if short == value:
                return short, long
        return None, None

    def choices(self) -> list[tuple[X, str | MLString | TString]]:
        if self._values is None:
            self._values = self._build_choices()
        return self._values

    def _build_choices(self) -> list[tuple[X, str | TString | MLString]]:
        v: list[tuple[str, str | TString | MLString]] = [("", TString("gcflask.common.placeholder"))]
        for x in self._config["values"]:
            disp = self._config["values"][x]
            if isinstance(disp, dict):
                v.append((x, MLString(disp)))
            else:
                v.append((x, str(disp)))
        return v

    def _sanitize_value(self, value) -> X | None:
        return self._find_choice(value)[0]

    def _display(self, v: X | None) -> Markup:
        return escape(self._find_choice(v)[1])

    def _data_value(self, value: X | None, **kwargs) -> t.Any:
        return self._find_choice(value)[0]


