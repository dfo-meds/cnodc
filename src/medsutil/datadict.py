"""
    The DataDictObject class provides a base object that stores values in
    a dictionary in a JSON-ready format.
"""
import datetime
import decimal
import functools
import typing as t
from contextlib import contextmanager
from types import EllipsisType

from medsutil.sanitize import unnumpy, coerce, require
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_name, dynamic_object
from medsutil.delayed import _DelayedDefaultValue, newdict, newlist, resolve_delayed, newset
import medsutil.types as ct
import medsutil.json as json

type AcceptAsObjectType = type[DataDictObject]
type _ManagedNameSanitizer[StoreType, ExportType] = t.Callable[[StoreType], ExportType]
type _SetCoercer[AcceptType, StoreType] = t.Callable[[AcceptType], StoreType]
type AcceptAsCoercer[AcceptType, StoreType] = _SetCoercer[AcceptType, StoreType] | type[StoreType]
type _GetCoercer[GetType, StoreType] = t.Callable[[StoreType], GetType]
type _ManagedNameValidator[GetType] = t.Callable[[t.Any, GetType], t.NoReturn]

class _ManagedNameGetter[GetType](t.Protocol):
    def __call__(self, *, managed_name: str) -> GetType: ...

class _ManagedNameSetter[AcceptType](t.Protocol):
    def __call__(self, value: AcceptType, *, managed_name: str): ...

class _ManagedNameDeleter(t.Protocol):
    def __call__(self, *, managed_name: str): ...


class _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType](property):
    """ A property that also stores a name and a default value. """

    AllAcceptTypes = AcceptType | GetType | StoreType | ExportType | None | _DelayedDefaultValue[AcceptType | GetType]

    # noinspection PyTypeChecker
    def __init__(self,
                 fget: t.Optional[_ManagedNameGetter[GetType]] = None,
                 fset: t.Optional[_ManagedNameSetter[AllAcceptTypes]] = None,
                 fdel: t.Optional[_ManagedNameDeleter] = None,
                 doc: t.Optional[str] = None,
                 default: AllAcceptTypes | EllipsisType = None,
                 managed_name: t.Optional[str] = None,
                 sanitizer: t.Optional[_ManagedNameSanitizer[StoreType, ExportType]] = None):
        self.managed_name: str = managed_name
        self.property_name: str = None
        self.default = default
        self.sanitizer = sanitizer
        super().__init__(
            fget=functools.partial(fget, managed_prop=self),
            fset=functools.partial(fset, managed_prop=self),
            fdel=functools.partial(fdel, managed_prop=self),
            doc=doc
        )

    def sanitize(self, value: GetType) -> ExportType:
        if value is not None and self.sanitizer is not None:
            return self.sanitizer(value)
        return value

    def __set_name__(self, cls, name: str):
        self.property_name = name
        if self.managed_name is None:
            self.managed_name = name
        if not hasattr(cls, '_datadict_props_'):
            cls._datadict_props_ = {}
        if cls not in cls._datadict_props_:
            cls._datadict_props_[cls] = {}
        cls._datadict_props_[cls][self.managed_name] = self

type _SimpleProperty[AcceptType, ExportType] = _ManagedNameProperty[AcceptType, ExportType, ExportType, ExportType]
type _ExportedProperty[AcceptType, GetType, ExportType] = _ManagedNameProperty[AcceptType, GetType, GetType, ExportType]

def ddo_property[AcceptType, GetType, StoreType, ExportType](
        *,
        managed_name: str | None = None,
        coerce_set: AcceptAsCoercer[AcceptType, StoreType] = None,
        coerce_get: _GetCoercer[GetType, StoreType] = None,
        required: bool = False,
        default: GetType | AcceptType | EllipsisType | _DelayedDefaultValue[AcceptType | GetType] | None = None,
        doc: str | None = None,
        validators: list[_ManagedNameValidator[GetType]] | None = None,
        readonly: bool = False,
        sanitizer: _ManagedNameSanitizer[StoreType, ExportType] | None = None) -> _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType]:
    if required:
        default = ...
    return _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType](
        fget=functools.partial(DataDictObject.get_data, coerce_get=coerce_get),
        fset=functools.partial(DataDictObject.set_data, coerce_set=coerce_set, readonly=readonly, validators=validators),
        fdel=functools.partial(DataDictObject.del_data, readonly=readonly),
        doc=doc,
        managed_name=managed_name,
        default=default,
        sanitizer=sanitizer
    )


class DataDictObject:

    def __init__(self, *args, _cls_=None, **kwargs):
        self._data = {}
        self._allow_readonly_access: bool = True
        self._in_init: bool = True
        self._after_init: list[t.Callable] = []
        with self.readonly_access():
            for prop in self._datadict_props():
                if prop.property_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.property_name))
                    if prop.managed_name:
                        kwargs.pop(prop.managed_name, None)
                elif prop.managed_name and prop.managed_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.managed_name))
                elif prop.default is Ellipsis:
                    raise ValueError(f'Missing argument [{prop.property_name}] for [{self.__class__.__name__}]')
                else:
                    setattr(self, prop.property_name, prop.default)
        self._in_init = False
        for x in self._after_init:
            x()
        del self._after_init
        super().__init__(*args, **kwargs)

    def __repr__(self):
        s = f'<{self.__class__.__name__} ' + '{'
        s += ', '.join(f"{repr(x)}: {repr(self._data[x])}" for x in self._data)
        s += '}>'
        return s

    def __str__(self):
        s = '{'
        s += ', '.join(f"{repr(x)}: {repr(self._data[x])}" for x in self._data)
        s += '}'
        return s

    @contextmanager
    def readonly_access(self) -> t.Generator[t.Self, t.Any, None]:
        try:
            self._allow_readonly_access = True
            yield self
        finally:
            self._allow_readonly_access = False

    def to_map(self) -> dict[str, t.Any]:
        return self._data

    def export(self) -> dict[str, ct.SupportsExtendedJson]:
        map_ = {'_cls_': dynamic_name(self)}
        for prop in self._datadict_props():
            map_[prop.managed_name] = prop.sanitize(getattr(self, prop.property_name))
        return map_

    def get_sanitized_data(self, managed_name: str):
        prop = self._find_datadict_prop(managed_name)
        if not prop:
            raise KeyError(f'Missing managed name {managed_name}')
        return prop.sanitize(getattr(self, prop.property_name))

    def get_data(self, *, managed_prop: _ManagedNameProperty, coerce_get: t.Callable = None):
        managed_name = managed_prop.managed_name
        if managed_name in self._data:
            value = self._data[managed_name]
            return coerce_get(value) if coerce_get is not None and value is not None else value
        raise KeyError(f'Missing {managed_name}')

    def del_data(self, *, managed_prop: _ManagedNameProperty, readonly: bool = False):
        managed_name = managed_prop.managed_name
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")
        if managed_name in self._data:
            del self._data[managed_name]

    @resolve_delayed
    def set_data(self, value: t.Any, *, managed_prop: _ManagedNameProperty, coerce_set: t.Callable = None, readonly: bool = False, validators: list[t.Callable] | None = None):
        managed_name = managed_prop.managed_name
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")
        if value is not None and coerce_set is not None:
            value = coerce_set(value)
        if value is not None and validators:
            for validator in validators:
                validator(value)
        original = self._data[managed_name] if managed_name in self._data else None
        self._data[managed_name] = value
        if self._in_init:
            self._after_init.append(functools.partial(self.after_set, managed_name, value, original))
        else:
            self.after_set(managed_name, value, original)

    def set_from_managed_name(self, value: t.Any, name: str):
        prop = self._find_datadict_prop(name)
        setattr(self, prop.property_name, value)

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        pass

    def _find_datadict_prop(self, name: str) -> _ManagedNameProperty:
        for prop in self._datadict_props():
            if prop.managed_name == name or prop.property_name == name:
                return prop
        raise KeyError(f'Missing managed name {name}')

    def _datadict_props(self) -> t.Iterable[_ManagedNameProperty]:
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, '_datadict_props_'):
                if cls in cls._datadict_props_:
                    yield from cls._datadict_props_[cls].values()

    @classmethod
    def from_map(cls, data: t.Mapping[str, t.Any]):
        if '_cls' in data:
            return dynamic_object(data['_cls_'])(**data)
        return cls(**data)

def p_int(**kwargs) -> _SimpleProperty[ct.AcceptAsInteger, int]:
    return ddo_property(**kwargs, coerce_set=int)

def p_float(**kwargs) -> _SimpleProperty[ct.AcceptAsFloat, float]:
    return ddo_property(**kwargs, coerce_set=float)

def p_bool(**kwargs) -> _SimpleProperty[ct.SupportsBool, bool]:
    return ddo_property(**kwargs, coerce_set=bool)

def p_str(**kwargs) -> _SimpleProperty[ct.SupportsString, str]:
    return ddo_property(**kwargs, coerce_set=str)

def p_date(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, datetime.date, str]:
    return ddo_property(
        coerce_set=coerce.as_date,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_datetime(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, datetime.datetime, str]:
    return ddo_property(
        coerce_set=coerce.as_datetime,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_awaretime(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, AwareDateTime, str]:
    return ddo_property(
        coerce_set=coerce.as_awaretime,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_enum[X](enum_type: type[X], **kwargs) -> _ExportedProperty[ct.AcceptAsEnum[X], X, ct.SupportsNativeJson]:
    if 'validators' not in kwargs:
        kwargs['validators'] = []
    kwargs['validators'].append(functools.partial(require.type_is, required_type=enum_type))
    return ddo_property(
        coerce_set=functools.partial(coerce.as_enum, enum_type=enum_type),
        sanitizer=coerce.enum_as_value,
        **kwargs
    )

def p_dict[AcceptKey, AcceptValue, StoreKey, StoreValue, ExportType](
        default: dict[StoreKey, StoreValue] = newdict,
        str_coerce: t.Callable[[str], dict[AcceptKey, AcceptValue]] = None,
        value_coerce: t.Callable[[AcceptValue], StoreValue] = None,
        key_coerce: t.Callable[[AcceptKey], StoreKey] = None,
        key_validators: list[t.Callable[[StoreKey], None]] = None,
        value_validators: list[t.Callable[[StoreValue], None]] = None,
        sanitizer: _ManagedNameSanitizer[dict[StoreKey, StoreValue], ExportType] = None,
        **kwargs) -> _ExportedProperty[t.Mapping[AcceptKey, AcceptValue], dict[StoreKey, StoreValue], ExportType]:
    if value_validators or key_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(require.mapping_meets_all, value_validators=value_validators, key_validators=key_validators))
    return ddo_property(
        default=default,
        coerce_set=functools.partial(coerce.as_dict, key_coerce=key_coerce, str_coerce=str_coerce, value_coerce=value_coerce),
        sanitizer=sanitizer,
        **kwargs
    )

def p_list[AcceptItemType, StoreItemType, ExportType](
        default: list[StoreItemType] = newlist,
        str_coerce: t.Callable[[str], list[AcceptItemType]] = None,
        value_coerce: t.Callable[[AcceptItemType], StoreItemType] = None,
        value_validators: list[_ManagedNameValidator[StoreItemType]] = None,
        sanitizer: _ManagedNameSanitizer[list[StoreItemType], ExportType] = None,
        **kwargs) -> _ExportedProperty[t.Iterable[AcceptItemType], list[StoreItemType], ExportType]:
    if value_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(require.iterable_meets_all, validators=value_validators))
    return ddo_property(
        coerce_set=functools.partial(
            coerce.as_list,
            value_coerce=value_coerce,
            str_coerce=str_coerce
        ),
        sanitizer=sanitizer,
        default=default,
        **kwargs
    )

def p_set[AcceptItemType, StoreItemType, ExportItemType](
        default: set[StoreItemType] = newset,
        str_coerce: t.Callable[[str], t.Iterable[AcceptItemType]]=None,
        value_coerce: t.Callable[[AcceptItemType], StoreItemType]=None,
        value_validators: list[_ManagedNameValidator[StoreItemType]]=None,
        sanitizer: _ManagedNameSanitizer[set[StoreItemType], ExportItemType]=None,
        **kwargs) -> _ExportedProperty[t.Iterable[AcceptItemType], set[StoreItemType], ExportItemType]:
    if value_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(require.iterable_meets_all, validators=value_validators))
    return ddo_property(
        coerce_set=functools.partial(
            coerce.as_set,
            value_coerce=value_coerce,
            str_coerce=str_coerce
        ),
        sanitizer=sanitizer,
        default=default,
        **kwargs
    )

def _coerce_ddo(v: DataDictObject | t.Mapping | str, str_coerce: t.Callable[[str], t.Mapping | DataDictObject] = None, required_type: type[DataDictObject] = None) -> DataDictObject:
    if isinstance(v, str) and str_coerce is not None:
        v = str_coerce(v)
    if isinstance(v, DataDictObject):
        return v
    elif isinstance(v, t.Mapping):
        if required_type is not None:
            return required_type.from_map(v)
        return DataDictObject.from_map(v)
    else:
        raise TypeError('Invalid type for DDO')

def p_ddo(required_type: type[DataDictObject] = None,
          **kwargs) -> _ExportedProperty[dict[str, t.Any] | DataDictObject, DataDictObject, dict[str, ct.SupportsNativeJson]]:
    if required_type is not None:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(require.type_is, required_type=required_type))
    return ddo_property(
        coerce_set=functools.partial(_coerce_ddo, str_coerce=None, required_type=required_type),
        sanitizer=lambda x: x.export(),
        **kwargs
    )

def p_json_dict[AcceptType, GetType](value_coerce: t.Callable[[AcceptType], GetType] = None, **kwargs) -> _ExportedProperty[t.Mapping[str, AcceptType] | str, dict[str, GetType], str]:
    return p_dict(str_coerce=json.load_dict, sanitizer=coerce.as_json_string, value_coerce=value_coerce, **kwargs)

def p_json_list[AcceptType, GetType](value_coerce: t.Callable[[AcceptType], GetType] = None, **kwargs) -> _ExportedProperty[t.Iterable[AcceptType] | str, list[GetType], str]:
    return p_list(str_coerce=json.load_list, sanitizer=coerce.as_json_string, value_coerce=value_coerce, **kwargs)

def p_json_set[X](**kwargs) -> _ExportedProperty[ct.AcceptAsJsonSet, set[X], str]:
    return p_set(str_coerce=json.load_set, sanitizer=coerce.as_json_string, **kwargs)

def p_json_str_list(**kwargs) -> _ExportedProperty[ct.AcceptAsJsonList, list[str], str]:
    return p_json_list(value_coerce=str, **kwargs)

def p_json_str_set(**kwargs) -> _ExportedProperty[ct.AcceptAsJsonList, set[str], str]:
    return p_json_set(value_coerce=str, **kwargs)

def p_json_object(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[dict[str, t.Any] | DataDictObject | ct.JsonDictString, DataDictObject, str]:
    if required_type is not None:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(require.type_is, required_type=required_type))
    return ddo_property(
        coerce_set=functools.partial(_coerce_ddo, str_coerce=json.load_dict, required_type=required_type),
        sanitizer=lambda x: json.dumps(x.export()),
        **kwargs
    )

def p_json_object_list(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[t.Iterable[DataDictObject | t.Mapping | str] | ct.JsonListString, list[DataDictObject], str]:
    if required_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(require.type_is, required_type=required_type))
    return p_json_list(
        value_coerce=functools.partial(_coerce_ddo, required_type=required_type),
        sanitizer=lambda x: json.dumps([y.export() for y in x]),
        **kwargs
    )

def p_json_object_dict(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[t.Mapping[str, DataDictObject | t.Mapping | str] | str, dict[str, DataDictObject], str]:
    if required_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(require.type_is, required_type=required_type))
    return p_json_dict(
        value_coerce=functools.partial(_coerce_ddo, required_type=required_type),
        sanitizer=lambda x: json.dumps({str(y): x[y].export() for y in x}),
        **kwargs
    )

def p_json_enum_set[X](enum_type: type[X], **kwargs) -> _ExportedProperty[t.Iterable[ct.AcceptAsEnum[X]], set[X], str]:
    if 'value_validators' not in kwargs:
        kwargs['value_validators'] = []
    kwargs['value_validators'].append(functools.partial(require.type_is, required_type=enum_type))
    return p_json_set(
        value_coerce=functools.partial(coerce.as_enum, enum_type=enum_type),
        **kwargs
    )

def p_nonumpy(**kwargs) -> _SimpleProperty[ct.NumpyNumberLike, ct.NumberLike]:
    if 'validators' not in kwargs:
        kwargs['validators'] = []
    kwargs['validators'].append(functools.partial(require.type_is, required_type=(int, float, decimal.Decimal)))
    return ddo_property(
        coerce_set=unnumpy,
        **kwargs
    )

def p_i18n_text(**kwargs) -> _SimpleProperty[ct.AcceptAsLanguageDict, ct.LanguageDict]:
    return ddo_property(
        coerce_set=coerce.as_i18n_text,
        **kwargs
    )

def p_bytearray(**kwargs) -> _SimpleProperty[t.ByteString, bytearray]:
    return ddo_property(
        coerce_set=coerce.as_bytearray,
        **kwargs
    )

def p_bytes(**kwargs) -> _SimpleProperty[t.ByteString, bytes]:
    return ddo_property(
        coerce_set=coerce.as_bytes,
        **kwargs
    )

def p_dynamic_object(**kwargs) -> _ExportedProperty[t.Any, t.Any, str]:
    return ddo_property(
        coerce_set=lambda x: dynamic_object(x) if isinstance(x, str) else x,
        sanitizer=dynamic_name,
        **kwargs
    )

def _dynamic_callable(x: t.Any) -> t.Callable:
    if isinstance(x, str):
        x = dynamic_object(x)
    if callable(x):
        return x
    raise ValueError('Invalid object, not callable')

def p_dynamic_callable(**kwargs) -> _ExportedProperty[t.Any, t.Callable, str]:
    return ddo_property(
        coerce_set=_dynamic_callable,
        sanitizer=dynamic_name,
        **kwargs
    )
