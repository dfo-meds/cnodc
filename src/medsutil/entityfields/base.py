import typing as t

from gcflask.i18n import MLString

if t.TYPE_CHECKING:
    import wtforms as wtf

class Container:

    @property
    def container_id(self) -> int:
        ...

    @property
    def container_type(self) -> str:
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
        sanitize: t.Callable[[t.Any], ActualType | dict[str, ActualType] | None] = self._sanitize_multilingual_value if self.is_multilingual else self._sanitize_value
        return [
            sanitize(v)
            for v in (value if isinstance(value, (list, tuple, set)) else [value])
        ]

    def _sanitize_multilingual_value(self, value: ACCEPT_TYPES) -> dict[str, ActualType] | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}
        else:
            return {'und': self._sanitize_value(value)}

    def _sanitize_value(self, value: ACCEPT_TYPES) -> ActualType | None:
        return value

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

    def form_control(self) -> wtf.Field:
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

    def _control_class(self) -> t.Callable[[...], wtf.Field]:
        raise NotImplemented

    def _allow_translation_requests(self) -> bool:
        return False

    def _allow_javascript_controls(self) -> bool:
        return False

    def label(self) -> MLString:
        return MLString(self._config.get("label" ,""))

    def description(self) -> MLString:
        return MLString(self._config.get("description" ,""))

    def _default_value(self):
        ...

    def _filters(self) -> list:
        return []

    def _validators(self) -> list:
        validators = []
        if self._config.get("is_required", False):

            validators.append()
        return validators


