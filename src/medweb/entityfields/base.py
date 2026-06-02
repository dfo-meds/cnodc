import itertools
import typing as t

from markupsafe import Markup, escape
from wtforms.fields.core import UnboundField
from wtforms.validators import Optional

from gcflask.forms import InputRequired, TranslatableField, NumberRange, Length, NoControlCharacters
from gcflask.i18n import MLString
from gcflask.widgets import HtmlList, MultilingualList
from medweb.entityfields.keywords import Keyword

if t.TYPE_CHECKING:
    import wtforms as wtf

class Container:

    def data(self, field_name: str):
        ...

    @property
    def container_id(self) -> int:
        ...

    @property
    def container_type(self) -> str:
        ...

    @property
    def supports_select2(self) -> bool:
        ...


class Field[AcceptType, ActualType]:

    ACCEPT_TYPES = None | AcceptType | list[AcceptType] | set[AcceptType] | tuple[AcceptType] | dict[str, AcceptType]
    ACTUAL_TYPES = None | ActualType | list[ActualType] | dict[str, ActualType]

    def __init__(self, name: str, config: dict[str, t.Any], parent_container: Container) -> None:
        self._name: str = name
        self._config: dict[str, t.Any] = config
        self._parent: Container = parent_container
        self._value = None

    @property
    def display_group(self) -> str:
        return self._config['display_group'] if 'display_group' in self._config and self._config['display_group'] else ''

    @property
    def parent_id(self) -> int | None:
        return self._parent.container_id if self._parent is not None else None

    @property
    def parent_type(self) -> str | None:
        return self._parent.container_type if self._parent is not None else None

    @property
    def is_repeatable(self):
        return 'repeatable' in self._config and self._config['repeatable']

    @property
    def is_multilingual(self):
        return 'multilingual' in self._config and self._config['multilingual']

    @property
    def value(self) -> ACTUAL_TYPES:
        return self._value

    @value.setter
    def value(self, value: ACCEPT_TYPES) -> None:
        self._value = self._sanitize_value_entry(value)

    def _sanitize_value_entry(self, value: ACCEPT_TYPES) -> ACTUAL_TYPES:
        if self.is_repeatable:
            return self._sanitize_repeatable_value(value)
        elif self.is_multilingual:
            return self._sanitize_multilingual_value(value)
        else:
            return self._sanitize_value(value)

    def _sanitize_repeatable_value(self, value: ACCEPT_TYPES) -> list[ActualType | dict[str, ActualType] | None] | None:
        if value is None:
            return None
        if isinstance(value, str):
            separator = self._config.get("separator", None)
            if separator:
                value = value.split(separator)
        # this happens in some form controls
        elif isinstance(value, list) and value and isinstance(value[0], list):
            value = value[0]
        if self.is_multilingual:
            return [
                self._sanitize_multilingual_value(v, idx)
                for idx, v in enumerate((value if isinstance(value, (list, tuple, set)) else [value]))
            ]
        else:
            return [
                self._sanitize_value(v)
                for v in (value if isinstance(value, (list, tuple, set)) else [value])
            ]

    def _sanitize_multilingual_value(self, value: ACCEPT_TYPES, idx: int | None = None) -> dict[str, ActualType] | None:
        if value is None:
            return None
        if isinstance(value, dict):
            if '_translation_request' in value and value['_translation_request']:
                self._file_translation_request(value, idx)
            return {k: self._sanitize_value(v) for k, v in value.items()}
        else:
            return {'und': self._sanitize_value(value)}

    def _file_translation_request(self, value: dict[str, t.Any], index: int | None = None):
        # TODO: return to this
        ...


    def _complete_translation_request(self, translations: dict[str, t.Any], index: int | None = None):
        # TODO
        ...

    def serialize(self):
        if self.is_repeatable:
            if self.is_multilingual:
                return [self._serialize_multilingual(v) for v in self._value]
            else:
                return [self._serialize(v) for v in self._value]
        elif self.is_multilingual:
            return self._serialize_multilingual(self._value)
        else:
            return self._serialize(self._value)

    def _serialize_multilingual(self, value: dict[str, ActualType]) -> dict[str, t.Any]:
        return {k: self._serialize(v) for k, v in value.items()}

    def _serialize(self, value: ActualType) -> t.Any:
        return value

    def form_control(self) -> wtf.Field | UnboundField:
        if self.is_repeatable:
            return wtf.FieldList(
                self._form_control_with_translation_check(parent=False),
                min_entries=1,
                **self._top_level_control_kwargs()
            )
        else:
            return self._form_control_with_translation_check()

    def _form_control_with_translation_check(self, parent: bool = True):
        if self.is_multilingual:
            tf_kwargs: dict[str, t.Any] = {
                'use_metadata_languages': True,
                'field_kwargs': self._field_level_control_kwargs(),
                'allow_translation_requests': self._allow_translation_requests(),
                'allow_js_widget': self._allow_javascript_controls(),
            }
            if parent:
                tf_kwargs.update(self._top_level_control_kwargs())
            else:
                tf_kwargs['label'] = ''
            return TranslatableField(self._control_class(), **tf_kwargs)
        elif not parent:
            return self._control_class()(**self._field_level_control_kwargs())
        else:
            return self._control_class()(**self._top_level_control_kwargs(), **self._field_level_control_kwargs())

    def _top_level_control_kwargs(self) -> dict[str, t.Any]:
        return {
            'label': self.label(),
            'description': self.description(),
            'default': self._default_value(),
        }

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        return {
            'filters': self._filters(),
            'validators': self._validators(),
        }

    def _allow_translation_requests(self) -> bool:
        if self.parent_id is None:
            return False
        return self._config.get("allow_translation_requests", self.is_multilingual)

    def _allow_javascript_controls(self) -> bool:
        return True

    def label(self, clean: bool = True) -> MLString:
        return MLString(self._config.get("label" ,""))

    def description(self) -> MLString:
        return MLString(self._config.get("description" ,""))

    def display(self) -> HtmlList | MultilingualList | Markup:
        if self.is_repeatable:
            return HtmlList([
                self._display_multilingual_check(x)
                for x in self._value
            ])
        else:
            return self._display_multilingual_check(self._value)

    def _display_multilingual_check(self, v: t.Any):
        if self.is_multilingual:
            return MultilingualList({
                k: self._display(v) for k, v in v.items()
            })
        else:
            return self._display(v)

    def _default_value(self):
        if self._value is None:
            if self.is_repeatable:
                return []
            elif self.is_multilingual:
                return {}
            else:
                return None
        return self._value

    def _clean_multilingual_for_form(self, value: dict[str, t.Any]) -> dict[str, t.Any]:
        # TODO: need to verify that all the metadata languages are valid here
        return value

    def _filters(self) -> list:
        return []

    def _validators(self) -> list:
        validators = []
        if self._config.get("is_required", False):
            validators.append(InputRequired())
        else:
            validators.append(Optional())
        return validators

    def get_keywords(self) -> set[Keyword]:
        if not self._value:
            return set()
        kw_config = self._config.get("keyword_config", None)
        if not (kw_config and isinstance(kw_config, dict) and 'is_keyword' in kw_config and kw_config['is_keyword']):
            return set()
        return self._extract_keywords()

    def _extract_keywords(self) -> set[Keyword]:
        result = set()
        if self.is_repeatable:
            for v in self._value:
                result.update(self._extract_keyword(v))
        else:
            result.update(self._extract_keyword(self._value))
        return result

    def _extract_keyword(self, value: t.Any) -> t.Iterable[Keyword]:
        yield Keyword(
            str(value),
            str(value),
            self._display(value),
            self._build_thesaurus(),
            self._keyword_mode()
        )

    def _keyword_mode(self) -> str:
        method = "value"
        cfg = self._config.get("keyword_config", {})
        method = cfg.get("extraction_method", default=method)
        method = cfg.get("mode", default=method)
        if method not in ("value", "translate", "both"):
            method = "value"
        return method

    def _get_default_thesaurus(self):
        return self._config.get("keyword_config", {}).get("thesaurus", None)

    def _build_thesaurus(self, loaded_obj: Container | None = None) -> dict:
        thesaurus = None
        if loaded_obj is not None:
            thesaurus_field = self._config.get("keyword_config", {}).get("thesaurus_field", None)
            if thesaurus_field:
                thesaurus = loaded_obj.data(thesaurus_field)
        return thesaurus or self._get_default_thesaurus()

    def data(self, lang: str | None = None, index: int | None = None, **kwargs) -> t.Any:
        if self._value is None:
            return self._data_value(None)
        if self.is_repeatable:
            if index is not None:
                return self._data_value_multilingual_check(self._value[index] if index < len(self._value) else None, lang=lang, **kwargs)
            else:
                return [
                    self._data_value_multilingual_check(v, lang=lang, **kwargs)
                    for v in self._value
                ]
        else:
            return self._data_value_multilingual_check(self._value, lang=lang, **kwargs)

    def _data_value_multilingual_check(self, value, lang: str | None = None, **kwargs):
        if self.is_multilingual:
            if lang is not None:
                return self._data_value(value[lang] if lang in value else None, **kwargs)
            else:
                return {
                    k: self._data_value(value[k], **kwargs)
                    for k in value
                }
        else:
            return self._data_value(value, **kwargs)

    def _data_value(self, value: ActualType | None, **kwargs) -> t.Any:
        return value

    def _sanitize_value(self, value: AcceptType) -> ActualType | None:
        return value

    def _control_class(self) -> t.Callable[[...], wtf.Field | UnboundField]:
        return getattr(self, 'CONTROL_CLASS')

    def _display(self, v: t.Any) -> Markup:
        if v is None:
            return escape('')
        return escape(v)


class NumberMixin(Field):

    def _validators(self) -> list:
        validators = super()._validators()
        min_value = self._config.get("min_value", None)
        max_value = self._config.get("max_value", None)
        if min_value is not None or max_value is not None:
            validators.append(NumberRange(min_value, max_value))
        return validators


class StringMixin(Field):

    def _validators(self) -> list:
        validators = super()._validators()
        validators.append(NoControlCharacters())
        min_length = self._config.get("min_length", None)
        max_length = self._config.get("max_length", None)
        if min_length is not None and max_length is not None:
            validators.append(Length(min_length, max_length))
        return validators

    def _sanitize_value(self, value: t.Any) -> str | None:
        if value == "" or value is None:
            return None
        if isinstance(value, dict):
            for key in itertools.chain(("und", "en"), value.keys()):
                if key in value and value[key]:
                    return self._sanitize_value(value[key])
        if isinstance(value, (tuple, list)):
            value = self._config.get("separator", ",").join(value)
        return str(value)

    def _display(self, v: t.Any) -> Markup:
        if v is None:
            v = ""
        return escape(v.replace("\n", "<br />"))

