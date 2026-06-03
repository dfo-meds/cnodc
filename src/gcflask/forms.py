import typing as t

import wtforms as wtf
import wtforms.form as wtff
import wtforms.validators as wtfv
from autoinject import injector
from flask_wtf import FlaskForm
from flask_wtf.file import FileField
from wtforms.fields.core import UnboundField

from gcflask.i18n import TString, TranslationManager, LanguageDetector
from gcflask.widgets import BetterTableWidget, TabbedFieldFormWidget

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


class Length(wtfv.Length):

    def __init__(self, min_value: int | None = None, max_value: int | None = None):
        if min_value is not None and max_value is not None:
            super().__init__(min_value, max_value, TString("gcflask.error.length_between", "Length must be between %(min) and %(max)"))
        elif min_value is not None:
            super().__init__(min_value, message=TString("gcflask.error.length_less_than_min", "Length must be at least %(min)"))
        elif max_value is not None:
            super().__init__(max=max_value, message=TString("gcflask.error.length_greater_than_max", "Length must be at most %(max)"))
        else:
            raise TypeError("Both min_value and max_value cannot be None")


class NumberRange(wtfv.NumberRange):

    def __init__(self, min_value: int | float | None = None, max_value: int | float | None = None):
        if min_value is not None and max_value is not None:
            super().__init__(min_value, max_value, TString("gcflask.error.range_between", "Number must be between %(min) and %(max)"))
        elif min_value is not None:
            super().__init__(min_value, message=TString("gcflask.error.range_less_than_min", "Number must be at least %(min)"))
        elif max_value is not None:
            super().__init__(max=max_value, message=TString("gcflask.error.range_greater_than_max", "Number must be at most %(max)"))
        else:
            raise TypeError("Both min_value and max_value cannot be None")


class InputRequired(wtfv.InputRequired):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _default="gcflask.error.required_field", _field="message")
        super().__init__(**kwargs)


class StringField(wtf.StringField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class BooleanField(wtf.BooleanField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class SelectField(wtf.SelectField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class PasswordField(wtf.PasswordField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _field="label")
        super().__init__(**kwargs)


class SubmitField(wtf.SubmitField):

    def __init__(self, **kwargs):
        _delayed_string_item(kwargs, _default="gcflask.common.submit", _field="label")
        super().__init__(**kwargs)


class DynamicFormField(wtf.FormField):

    def __init__(self, fields: dict[str, UnboundField | wtf.Field], *args, widget=None, **kwargs):
        self._field_list = fields
        self.form: wtff.BaseForm | None = None
        if widget is None:
            widget = BetterTableWidget()
        super().__init__(self._generate_form, *args, widget=widget, **kwargs)

    def __getattr__(self, item):
        if self.form is not None:
            return getattr(self._form, item)
        return None

    def _generate_form(self, formdata=None, obj=None, **kwargs) -> wtff.BaseForm:
        defaults = {}
        for key in self._field_list:
            if key in kwargs:
                defaults[key] = kwargs.pop(key)
        form = wtff.BaseForm(self._field_list, **kwargs)
        form.process(formdata, obj, data=defaults)
        return form


class TranslatableField(DynamicFormField):

    tm: TranslationManager = None
    ld: LanguageDetector = None

    @injector.construct
    def __init__(self,
                 template_field: t.Callable[[...], UnboundField | wtf.Field],
                 field_kwargs: dict[str, t.Any] | None = None,
                 *args,
                 use_undefined: bool = True,
                 allow_translation_requests: bool = False,
                 allow_js_widget: bool = True,
                 use_metadata_languages: bool = False,
                 **kwargs):
        self._template_field: t.Callable[[...], UnboundField | wtf.Field] = template_field
        self._template_kwargs: dict[str, t.Any] = field_kwargs or {}
        self._template_kwargs.pop("label", "")
        self._use_undefined: bool = use_undefined
        self._allow_translation_requests: bool = allow_translation_requests
        self._supported_languages: list[str] = self.tm.supported_languages("metadata" if use_metadata_languages else "interface")

        default_language = "und"
        if 'default' in kwargs and kwargs['default']:
            default_language = self._sanitize_defaults(kwargs['default'])

        if "widget" not in kwargs and allow_js_widget:
            kwargs['widget'] = TabbedFieldFormWidget(
                ['_translation_request'],
                for_txt_input=True,
                default_tab=self._supported_languages.index(default_language)
            )
        super().__init__(self._build_field_list(), *args, **kwargs)

    def _sanitize_defaults(self, defaults: dict[str, t.Any]):
        found_languages = []
        for key in list(defaults.keys()):
            if key not in self._supported_languages:
                del defaults[key]
            elif defaults[key]:
                found_languages.append(key)
        return self.ld.detect_language(found_languages)

    def __call__(self, **kwargs):
        if self.allow_translation_requests:
            if self.form.data['_translation_request']:
                for fn in self.form._fields:
                    if fn != 'und':
                        if self.form._fields[fn].render_kw:
                            self.form._fields[fn].render_kw['disabled'] = True
                        else:
                            self.form._fields[fn].render_kw = {
                                'disabled': True
                            }
        return super().__call__(**kwargs)

    def get_language_keys(self) -> list[str]:
        keys = []
        if self.use_undefined:
            keys.append('und')
        keys.extend(self._supported_languages)
        return keys

    def _build_field_list(self) -> dict[str, UnboundField | wtf.Field]:
        fields: dict[str, UnboundField | wtf.Field] = {
            key: self._template_field(label=TString(f"languages.short.{key.lower()}"), **self._template_kwargs)
            for key in self.get_language_keys()
        }
        if self.allow_translation_requests:
            fields["_translation_request"] = BooleanField(delayed_label="pipeman.common.open_translation_request")
        # gettext('languages.short.und')
        # gettext('languages.short.en')
        # gettext('languages.short.fr')
        return fields


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
