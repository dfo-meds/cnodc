import typing as t

import wtforms as wtf
import wtforms.validators as wtfv
from flask_wtf import FlaskForm
from flask_wtf.file import FileField

from gcflask.i18n import TString


CONTROL_LIST = [
    *[chr(x) for x in range(0, 9)],
    chr(11),
    chr(12),
    *[chr(x) for x in range(14, 32)],
    chr(127)
]


def _delayed_string_item(kwargs: dict[str, t.Any], _default: str | None = None, _field="label"):
    dlabel = kwargs.pop(f"delayed_{_field}", _default)
    if dlabel and not (_field in kwargs and kwargs[_field]):
        kwargs[_field] = TString(dlabel)


class NoControlCharacters:
    """Ensure there are no control characters"""

    def __init__(self, exceptions: list[str] | None = None, message: str | TString | None = None):
        self.message: str | TString = message or TString("gcflask.error.control_char_in_str")
        self.exceptions = exceptions or []

    def __call__(self, form, field, message: str | TString | None = None):
        txt = field.data or ''
        for cchar in CONTROL_LIST:
            if cchar in txt and not cchar in self.exceptions:
                raise wtfv.ValidationError(message or self.message)


class InputRequired(wtfv.InputRequired):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _default="gcflask.error.required_field", _field="message")
        super().__init__(**kwargs)


class StringField(wtf.StringField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class PasswordField(wtf.StringField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class SubmitField(wtf.SubmitField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _default="gcflask.common.submit", _field="label")
        super().__init__(**kwargs)


class GCFlaskForm(FlaskForm):

    def __init__(self, *args, **kwargs):
        self._with_file_upload = False
        super().__init__(*args, **kwargs)
        for name in self._fields:
            if isinstance(self._fields[name], FileField):
                self._with_file_upload = True
                break

    def validate_on_submit(self, extra_validators=None):
        if super().validate_on_submit(extra_validators):
            return True
        elif self.errors:
            ...
        return False
