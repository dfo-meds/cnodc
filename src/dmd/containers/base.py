import itertools
import typing as t

from autoinject import injector
from markupsafe import Markup, escape
from wtforms.fields.core import UnboundField
from wtforms.validators import Optional

from gcapp import i18n as i18n
from gcflask.forms import InputRequired, TranslatableField, NumberRange, Length, NoControlCharacters
from gcapp.i18n.base import MLString
from gcflask.widgets import HtmlList, MultilingualList, InfoTable, HtmlContent
from dmd.containers.keywords import Keyword

if t.TYPE_CHECKING:
    import wtforms as wtf


class FieldValidator:

    def __call__(self,
                 obj_path: list[str | MLString],
                 field: Field,
                 memo: set[tuple[str, int | None]]) -> t.Iterable[ValidationResult]:
        raise NotImplementedError


class ContainerValidator:

    def __call__(self,
                 obj_path: list[str | MLString],
                 container: Container,
                 memo: set[tuple[str, int | None]]) -> t.Iterable[ValidationResult]:
        raise NotImplementedError


class ValidationResult:
    ...


class Container:

    FIELD_TYPE_REGISTRY: dict[str, type] = {}

    def __init__(self,
                 container_type: str,
                 display_names: dict[str, str],
                 fields: dict[str, dict[str, t.Any]],
                 field_values: dict[str, t.Any]):
        self._container_type = container_type
        self._fields: dict[str, Field] = {}
        self._load_fields(fields, field_values)
        self._display_names = display_names
        self._field_validators: dict[str, list[FieldValidator]] = {}
        self._container_validators: list[ContainerValidator] = []

    def container_link(self) -> Markup:
        raise NotImplementedError

    def _load_fields(self, fields: dict[str, dict[str, t.Any]], values: dict[str, t.Any]):
        for field_name, field_config in fields.items():
            self._fields[field_name] = self.build_field(
                field_name,
                field_config,
                self
            )
            self._fields[field_name].value = values.get(field_name, None)

    def add_field_validator(self, field_name: str, validator: FieldValidator):
        if field_name not in self._field_validators:
            self._field_validators[field_name] = []
        self._field_validators[field_name].append(validator)

    def add_container_validator(self, validator: ContainerValidator):
        self._container_validators.append(validator)

    def validate_metadata(self,
                          parent_path: list[str] | None = None,
                          _memo: set[tuple[str, int | None]] | None = None) -> list[ValidationResult]:
        memo = set() if _memo is None else _memo

        my_id = (self.container_type, self.container_id)
        if my_id in memo:
            return []
        memo.add(my_id)

        pp = [] if parent_path is None else parent_path
        my_name = self.display()

        results = []
        for fn in sorted(self._fields.keys()):
            field = self._fields[fn]
            obj_path = [*pp, my_name, field.label(True)]
            for validator in self._field_validators.get(fn, []):
                results.extend(validator(obj_path, field, memo))
            results.extend(field.validate_metadata(obj_path, memo))

        for validator in self._container_validators:
            results.extend(validator([*pp, my_name], self, memo))

        return results

    def __contains__(self, name: str) -> bool:
        return name in self._fields

    def __getitem__(self, name: str) -> t.Any:
        return self.data(name) or ""

    def info_table(self, display_group: str | None = None) -> InfoTable:
        return InfoTable(
            rows=[x for x in self.display_values(display_group)],
            table_classes=["field-list-table"]
        )

    def display_values(self,
                       display_group: str | None = None) -> t.Generator[tuple[list[str] | None, str | MLString | Markup | HtmlContent, str | MLString | Markup | HtmlContent], None, None]:
        for field_name in self.ordered_field_names(display_group):
            field = self._fields[field_name]
            label = field.label()
            content = field.display()
            classes: list[str] = []
            yield classes, label, content

    def all_empty(self) -> bool:
        for field in self._fields.values():
            if not field.is_empty():
                return False
        return True

    def serialize(self) -> dict[str, t.Any]:
        return {
            fn: field.serialize()
            for fn, field in self._fields.items()
        }

    def unserialize(self, data: dict[str, t.Any]):
        for key, value in data.items():
            if key in self._fields:
                self._fields[key].value = value

    def field_label(self, field_name: str, clean: bool = True) -> MLString:
        return self._fields[field_name].label(clean)

    def display(self):
        return MLString(self._display_names)

    def data(self, field_name: str, **kwargs) -> t.Any:
        if field_name in self._fields:
            return self._fields[field_name].data(**kwargs)
        return None

    def controls(self, display_group: str | None = None):
        return {
            field_name: field.form_control()
            for field_name, field in self._fields.items()
            if display_group is None or display_group == field.display_group
        }

    def ordered_field_names(self, display_group: str | None = None):
        field_names = [
            (field.order, fn)
            for fn, field in self._fields.items()
            if display_group is None or field.display_group == display_group
        ]
        field_names.sort()
        for _, field_name in field_names:
            yield field_name

    @property
    def container_id(self) -> int | None:
        raise NotImplementedError

    @property
    def container_type(self) -> str:
        return self._container_type

    @property
    def supports_select2(self) -> bool:
        raise NotImplementedError

    @staticmethod
    def register_type(field_type: type) -> type:
        Container.FIELD_TYPE_REGISTRY[getattr(field_type, "DATA_TYPE")] = field_type
        return field_type

    @staticmethod
    def build_field(field_name: str, config: dict[str, t.Any], container: Container) -> Field:
        return Container.FIELD_TYPE_REGISTRY[config["data_type"]](field_name, config, container)


class Field[AcceptType, ActualType]:

    ACCEPT_TYPES = None | AcceptType | list[AcceptType] | set[AcceptType] | tuple[AcceptType] | dict[str, AcceptType]
    ACTUAL_TYPES = None | ActualType | list[ActualType] | dict[str, ActualType]

    def __init__(self, name: str, config: dict[str, t.Any], parent_container: Container) -> None:
        self._name: str = name
        self._config: dict[str, t.Any] = config
        self._parent: Container = parent_container
        self._value = None

    @property
    def order(self) -> int:
        return self._config.get("order", 0)

    @property
    def display_group(self) -> str:
        return self._config.get('display_group', '')

    @property
    def parent_id(self) -> int | None:
        return self._parent.container_id if self._parent is not None else None

    @property
    def parent_type(self) -> str | None:
        return self._parent.container_type if self._parent is not None else None

    @property
    def is_repeatable(self) -> bool:
        return bool(self._config.get("repeatable", False))

    @property
    def is_multilingual(self):
        return bool(self._config.get("multilingual", False))

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
            values = []
            for idx, v in enumerate((value if isinstance(value, (list, tuple, set)) else [value])):
                value = self._sanitize_multilingual_value(v, idx)
                if value is not None:
                    values.append(value)
            return values or None
        else:
            values = []
            for v in (value if isinstance(value, (list, tuple, set)) else [value]):
                value = self._sanitize_value(v)
                if value is not None:
                    values.append(value)
            return values or None

    def _sanitize_multilingual_value(self, value: ACCEPT_TYPES, idx: int | None = None) -> dict[str, ActualType] | None:
        if value is None:
            return None
        values = {}
        if isinstance(value, dict):
            if '_translation_request' in value and value['_translation_request']:
                self._file_translation_request(value, idx)
            for k, v in value.items():
                value = self._sanitize_value(v)
                if value is not None:
                    values[k] = value
        else:
            value = self._sanitize_value(value)
            if value is not None:
                values["und"] = value
        return values or None

    def is_empty(self) -> bool:
        return not self._value

    def _file_translation_request(self, value: dict[str, t.Any], index: int | None = None):
        # TODO: return to this
        ...


    def _complete_translation_request(self, translations: dict[str, t.Any], index: int | None = None):
        # TODO
        ...

    def validate_metadata(self,
                          obj_path: list[str | MLString],
                          memo: set[tuple[str, int | None]]) -> t.Iterable[ValidationResult]:
        return []

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

    def display(self) -> Markup | HtmlContent:
        if self.is_repeatable:
            return HtmlList([
                self._display_multilingual_check(x)
                for x in self._value
            ])
        else:
            return self._display_multilingual_check(self._value)

    def _display_multilingual_check(self, v: t.Any) -> Markup | HtmlContent:
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
            self._keyword_value(value),
            self._build_thesaurus(),
            self._keyword_mode()
        )

    def _keyword_value(self, value: t.Any) -> str:
        return str(value) if value else ""

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

    def _display(self, v: t.Any) -> Markup | HtmlContent:
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

    def _display(self, v: t.Any) -> Markup | HtmlContent:
        if v is None:
            v = ""
        return escape(v.replace("\n", "<br />"))


@injector.injectable
class ContainerLoader:

    def build_container(self, container_type: str, values: dict[str, t.Any] | None = None) -> Container:
        raise NotImplementedError

    def list_containers(self, container_type: str) -> t.Iterable[tuple[int | str, i18n.MLString]]:
        raise NotImplementedError

    def load_container(self, container_type: str, container_id: int) -> Container | None:
        raise NotImplementedError

    def search_containers(self,
                          container_type: str,
                          filter_ids: list[int] | None = None,
                          name_like: str | None = None) -> t.Iterable[Container]:
        raise NotImplementedError

    def search_for_select_callback(self, container_type: str) -> str:
        raise NotImplementedError
