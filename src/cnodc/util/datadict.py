"""
    The DataDictObject class provides a base object that stores values in
    a dictionary in a JSON-ready format.
"""
from __future__ import annotations
import datetime
import decimal
import enum
import functools
import inspect
import typing as t
import uuid
from contextlib import contextmanager

import numpy as np

from cnodc.util import unnumpy
from cnodc.util.awaretime import AwareDateTime
from cnodc.util.dynamic import dynamic_name, dynamic_object
from cnodc.util.types import *
import cnodc.util.json as json

if t.TYPE_CHECKING:

    type AcceptAsJsonDict = t.Mapping[str, SupportsExtendedJson] | JsonDictString | None | _DelayedDefaultValue[dict]
    type AcceptAsJsonList = t.Iterable[SupportsExtendedJson] | JsonListString | None | _DelayedDefaultValue[list]
    type AcceptAsJsonSet = t.Iterable[SupportsExtendedJson] | JsonListString | None | _DelayedDefaultValue[set]
    type AcceptAsDateTime = datetime.datetime | datetime.date | str
    type AcceptAsEnum[EnumType: enum.Enum] = EnumType | str | int



    class _ManagedNameGetter[GetType](t.Protocol):
        def __call__(self, *, managed_name: str) -> GetType: ...

    class _ManagedNameSetter[AcceptType](t.Protocol):
        def __call__(self, value: AcceptType, *, managed_name: str): ...

    class _ManagedNameDeleter(t.Protocol):
        def __call__(self, *, managed_name: str): ...

    class _SetCoercer[AcceptType, StoreType](t.Protocol):
        def __call__(self, value: AcceptType) -> StoreType: ...

    class _GetCoercer[GetType, StoreType](t.Protocol):
        def __call__(self, value: StoreType) -> GetType: ...

    class _ManagedNameValidator[StoreType](t.Protocol):
        def __call__(self, obj: object, value: StoreType) -> t.NoReturn: ...



class _DelayedDefaultValue[T]:
    """ Represents a default value that needs to be built to avoid
        pass-by-reference errors when a new object is built (like a
        dictionary). """

    def __init__(self, cb: t.Callable[[], T]):
        self._cb = cb

    def __call__(self) -> T:
        return self._cb()


newdict = _DelayedDefaultValue[dict](lambda: {})
newlist = _DelayedDefaultValue[list](lambda: [])
newset = _DelayedDefaultValue[set](lambda: set())
newuuid = _DelayedDefaultValue[str](lambda: str(uuid.uuid4()))


class _ManagedNameProperty[AcceptType, GetType, StoreType](property):
    """ A property that also stores a name and a default value. """

    AllAcceptTypes = AcceptType | GetType | StoreType | None | _DelayedDefaultValue[AcceptType | GetType]

    def __init__(self,
                 fget: t.Optional[_ManagedNameGetter[GetType]] = None,
                 fset: t.Optional[_ManagedNameSetter[AllAcceptTypes]] = None,
                 fdel: t.Optional[_ManagedNameDeleter] = None,
                 doc: t.Optional[str] = None,
                 default: AllAcceptTypes | Ellipsis = None,
                 managed_name: t.Optional[str] = None):
        # note: this ensures if the name is updated afterwards that the fget/fset get the right value
        self.managed_name = managed_name
        self.property_name = None
        self.default = default
        super().__init__(
            fget=functools.partial(fget, managed_prop=self),
            fset=functools.partial(fset, managed_prop=self),
            fdel=functools.partial(fdel, managed_prop=self),
            doc=doc
        )

    def __set_name__(self, cls, name: str):
        if not hasattr(cls, '_datadict_props_'):
            cls._datadict_props_ = {}
        if cls not in cls._datadict_props_:
            cls._datadict_props_[cls] = []
        cls._datadict_props_[cls].append(self)
        if self.managed_name is None:
            self.managed_name = name
        self.property_name = name


def ddo_property[AcceptType, GetType, StoreType](
        *,
        managed_name: str | None = None,
        coerce: _SetCoercer[AcceptType, StoreType] = None,
        coerce_get: _GetCoercer[GetType, StoreType] = None,
        required: bool = False,
        default: GetType | AcceptType | Ellipsis | _DelayedDefaultValue[t.Union[AcceptType, GetType]] | None = None,
        doc: str | None = None,
        validators: list[_ManagedNameValidator[StoreType]] | None = None,
        readonly: bool = False) -> _ManagedNameProperty[AcceptType, GetType, StoreType]:
    if required:
        default = ...
    return _ManagedNameProperty[AcceptType, GetType, StoreType](
        fget=functools.partial(DataDictObject.get_data, coerce=coerce_get),
        fset=functools.partial(DataDictObject.set_data, coerce=coerce, readonly=readonly, validators=validators),
        fdel=functools.partial(DataDictObject.del_data, readonly=readonly),
        doc=doc,
        managed_name=managed_name,
        default=default
    )

def _datetime_coerce(x: AcceptAsDateTime) -> datetime.datetime:
    if isinstance(x, datetime.datetime):
        return x
    elif isinstance(x, datetime.date):
        return datetime.datetime(x.year, x.month, x.day)
    else:
        return datetime.datetime.fromisoformat(x)

def _awaretime_coerce(x: AcceptAsDateTime) -> AwareDateTime:
    if isinstance(x, datetime.datetime):
        return AwareDateTime.from_datetime(x)
    elif isinstance(x, datetime.date):
        return AwareDateTime(x.year, x.month, x.day)
    else:
        return AwareDateTime.fromisoformat(x)


def _date_coerce(x: AcceptAsDateTime) -> datetime.date:
    if isinstance(x, datetime.datetime):
        return x.date()
    elif isinstance(x, datetime.date):
        return x
    else:
        return datetime.date.fromisoformat(x)

def _coerce_ddo(obj, str_coerce=None):
    if isinstance(obj, DataDictObject):
        return obj
    if str_coerce and isinstance(obj, str):
        obj = str_coerce(obj)
    return DataDictObject.from_map(obj)

def _coerce_iterable(obj, iterable_cls: type, value_coerce=None, str_coerce=None):
    if str_coerce is not None and isinstance(obj, str):
        obj = str_coerce(obj)
    if value_coerce is None:
        if isinstance(obj, iterable_cls):
            return obj
        return iterable_cls(x for x in obj)
    return iterable_cls(value_coerce(x) for x in obj)

def _coerce_dict(obj, key_coerce=None, value_coerce=None, str_coerce=None):
    if str_coerce is not None and isinstance(obj, str):
        obj = str_coerce(obj)
    if key_coerce is None and value_coerce is None:
        if isinstance(obj, dict):
            return obj
        return {x: obj[x] for x in obj}
    elif key_coerce is None:
        return {x: value_coerce(obj[x]) for x in obj}
    elif value_coerce is None:
        return {key_coerce(x): obj[x] for x in obj}
    return {key_coerce(x): value_coerce(obj[x]) for x in obj}

def _coerce_enum(obj, enum_type: type):
    if obj is None or obj == '':
        return None
    if isinstance(obj, enum_type):
        return obj
    return enum_type(obj)

def _coerce_multilingual_text(v: AcceptAsLanguageDict | None) -> LanguageDict | None:
    if v is None:
        return None
    if isinstance(v, str):
        return {'und': v}
    return v

def _ensure_type[X](parent, v: X, require_type: AcceptAsObjectType):
    if not isinstance(v, require_type):
        raise ValueError(f'Invalid object type, expecting [{require_type.__name__}] found [{v.__class__.__name__}]')

def _ensure_for_all_in_mapping(parent, obj: t.Mapping, key_validators=None, value_validators=None):
    for key in obj:
        if key_validators:
            for key_validator in key_validators:
                key_validator(parent, key)
        if value_validators:
            value = obj[key]
            for value_validator in value_validators:
                value_validator(parent, value)

def _ensure_for_all_in_iterable(parent, obj: t.Iterable, validators: list[_ManagedNameValidator]):
    for item in obj:
        for validator in validators:
            validator(parent, item)

def _ensure_number(parent, v):
    if not isinstance(v, (int, float, decimal.Decimal)):
        raise ValueError('Expected a number')


class DataDictObject:

    def __init__(self, _cls_=None, **kwargs):
        self._data = {}
        self._allow_readonly_access: bool = True
        self._in_init: bool = True
        self._after_init: list[t.Callable] = []
        with self.readonly_access():
            for prop in self._datadict_props():
                if prop.property_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.property_name))
                    kwargs.pop(prop.managed_name, None)
                elif prop.managed_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.managed_name))
                elif prop.default is Ellipsis:
                    raise ValueError(f'Missing argument [{prop.property_name}] for [{self.__class__.__name__}]')
                elif isinstance(prop.default, _DelayedDefaultValue):
                    setattr(self, prop.property_name, prop.default())
                else:
                    setattr(self, prop.property_name, prop.default)
            if kwargs:
                raise AttributeError(f'Unhandled keyword arguments [{','.join(kwargs)}] for [{self.__class__.__name__}]')
        self._in_init = False
        for x in self._after_init:
            x()
        del self._after_init


    @contextmanager
    def readonly_access(self):
        try:
            self._allow_readonly_access = True
            yield self
        finally:
            self._allow_readonly_access = False

    def to_json_map(self) -> dict:
        map_ = self._clean_map(self._data)
        map_['_cls_'] = dynamic_name(self)
        return map_

    @classmethod
    def _clean_map(cls, obj):
        if isinstance(obj, DataDictObject):
            return obj.to_json_map()
        elif isinstance(obj, list):
            return [cls._clean_map(x) for x in obj]
        elif isinstance(obj, set):
            return [cls._clean_map(x) for x in obj]
        elif isinstance(obj, dict):
            return {x: cls._clean_map(obj[x]) for x in obj}
        elif isinstance(obj, enum.Enum):
            return obj.value
        return obj

    def get_data(self, *, managed_prop: _ManagedNameProperty, coerce: t.Callable = None):
        managed_name = managed_prop.managed_name
        if managed_name in self._data:
            value = self._data[managed_name]
            return coerce(value) if coerce is not None and value is not None else value
        raise KeyError(f'Missing {managed_name}')

    def del_data(self, *, managed_prop: _ManagedNameProperty, readonly: bool = False):
        managed_name = managed_prop.managed_name
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")
        if managed_name in self._data:
            del self._data[managed_name]

    def set_data(self, value: t.Any, *, managed_prop: _ManagedNameProperty, coerce: t.Callable = None, readonly: bool = False, validators: list[t.Callable] | None = None):
        managed_name = managed_prop.managed_name
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")
        if value is not None and coerce is not None:
            value = coerce(value)
        if value is not None and validators:
            for validator in validators:
                validator(self, value)
        self._data[managed_name] = value
        if self._in_init:
            self._after_init.append(functools.partial(self.after_set, managed_name, value))
        else:
            self.after_set(managed_name, value)

    def after_set(self, managed_name: str, value: t.Any):
        pass

    def _datadict_props(self) -> t.Iterable[_ManagedNameProperty]:
        for cls in reversed(self.__class__.__mro__):
            if hasattr(cls, '_datadict_props_'):
                if cls in cls._datadict_props_:
                    yield from cls._datadict_props_[cls]

    @staticmethod
    def from_map(data: dict):
        cls = dynamic_object(data['_cls_'])
        return cls(**data)

if t.TYPE_CHECKING:
    AcceptAsObjectType = t.Optional[type[DataDictObject] | t.Self]

def p_int(**kwargs) -> _ManagedNameProperty[t.SupportsInt, int, int]:
    return ddo_property(**kwargs, coerce=int)

def p_float(**kwargs) -> _ManagedNameProperty[t.SupportsFloat, float, float]:
    return ddo_property(**kwargs, coerce=float)

def p_bool(**kwargs) -> _ManagedNameProperty[SupportsBool, bool, bool]:
    return ddo_property(**kwargs, coerce=bool)

def p_str(**kwargs) -> _ManagedNameProperty[SupportsString, str, str]:
    return ddo_property(**kwargs, coerce=str)

def p_date(**kwargs) -> _ManagedNameProperty[AcceptAsDateTime, datetime.date, datetime.date]:
    return ddo_property(
        coerce=_date_coerce,
        **kwargs
    )

def p_datetime(**kwargs) -> _ManagedNameProperty[AcceptAsDateTime, datetime.datetime, datetime.datetime]:
    return ddo_property(
        coerce=_datetime_coerce,
        **kwargs
    )

def p_awaretime(**kwargs) -> _ManagedNameProperty[AcceptAsDateTime, AwareDateTime, AwareDateTime]:
    return ddo_property(
        coerce=_awaretime_coerce,
        **kwargs
    )

def p_enum[X: enum.Enum](enum_type: type[X], **kwargs) -> _ManagedNameProperty[AcceptAsEnum[X], X, X]:
    if 'validators' not in kwargs:
        kwargs['validators'] = []
    kwargs['validators'].append(functools.partial(_ensure_type, require_type=enum_type))
    return ddo_property(
        coerce=functools.partial(_coerce_enum, enum_type=enum_type),
        **kwargs
    )

def p_dict[T](default: t.Mapping = newdict,
              str_coerce: t.Callable[[str], dict] = None,
              value_coerce: t.Callable[[t.Any], T] = None,
              key_validators: t.Callable[[str], None] = None,
              value_validators: list[t.Callable[[T], None]] = None,
              **kwargs) -> _ManagedNameProperty[t.Mapping[str, T], dict[str, T], dict[str, T]]:
    if value_validators or key_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(_ensure_for_all_in_mapping, value_validators=value_validators, key_validators=key_validators))
    return ddo_property(
        coerce=functools.partial(_coerce_dict, key_coerce=str, str_coerce=str_coerce, value_coerce=value_coerce),
        default=default,
        **kwargs
    )

def p_list[T](default: t.Iterable = newlist,
              str_coerce: t.Callable[[str], list[T]]=None,
              value_coerce: t.Callable[[t.Any], T]=None,
              value_validators: list[_ManagedNameValidator[T]]=None,
              **kwargs) -> _ManagedNameProperty[t.Iterable[T], list[T], list[T]]:
    if value_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(_ensure_for_all_in_iterable, validators=value_validators))
    return ddo_property(
        coerce=functools.partial(
            _coerce_iterable,
            iterable_cls=list,
            value_coerce=value_coerce,
            str_coerce=str_coerce
        ),
        default=default,
        **kwargs
    )

def p_set[T](default: t.Iterable = newlist,
             str_coerce: t.Callable[[str], set[T]]=None,
             value_coerce: t.Callable[[t.Any], T]=None,
             value_validators: list[_ManagedNameValidator[T]]=None,
             **kwargs) -> _ManagedNameProperty[t.Iterable[T], set[T], set[T]]:
    if value_validators:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(_ensure_for_all_in_iterable, validators=value_validators))
    return ddo_property(
        coerce=functools.partial(
            _coerce_iterable,
            iterable_cls=set,
            value_coerce=value_coerce,
            str_coerce=str_coerce
        ),
        default=default,
        **kwargs
    )

def p_ddo(require_type: AcceptAsObjectType = None,
          str_coerce=None,
          **kwargs) -> _ManagedNameProperty[dict | DataDictObject, DataDictObject, DataDictObject]:
    if require_type is not None:
        if 'validators' not in kwargs:
            kwargs['validators'] = []
        kwargs['validators'].append(functools.partial(_ensure_type, require_type=require_type))
    return ddo_property(
        coerce=functools.partial(_coerce_ddo, str_coerce=str_coerce),
        **kwargs
    )

def p_json_dict(**kwargs) -> _ManagedNameProperty[AcceptAsJsonDict, t.Mapping[str, SupportsExtendedJson], t.Mapping[str, SupportsExtendedJson]]:
    return p_dict(str_coerce=json.load_dict, **kwargs)

def p_json_list(**kwargs) -> _ManagedNameProperty[AcceptAsJsonList, list[SupportsExtendedJson], list[SupportsExtendedJson]]:
    return p_list(str_coerce=json.load_list, **kwargs)

def p_json_set(**kwargs) -> _ManagedNameProperty[AcceptAsJsonSet, set[SupportsExtendedJson], set[SupportsExtendedJson]]:
    return p_set(str_coerce=json.load_set, **kwargs)

def p_json_str_list(**kwargs) -> _ManagedNameProperty[AcceptAsJsonList, list[str], list[str]]:
    return p_json_list(value_coerce=str, **kwargs)

def p_json_str_set(**kwargs) -> _ManagedNameProperty[AcceptAsJsonList, set[str], set[str]]:
    return p_json_set(value_coerce=str, **kwargs)

def p_json_object(require_type: AcceptAsObjectType = None, **kwargs) -> _ManagedNameProperty[AcceptAsJsonDict | DataDictObject, DataDictObject, DataDictObject]:
    return p_ddo(
        require_type,
        str_coerce=json.load_dict,
        **kwargs
    )

def p_json_object_list(require_type: AcceptAsObjectType = None, **kwargs) -> _ManagedNameProperty[AcceptAsJsonList | t.Iterable[DataDictObject], list[DataDictObject], list[DataDictObject]]:
    if require_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(_ensure_type, require_type=require_type))
    return p_json_list(
        value_coerce=_coerce_ddo,
        **kwargs
    )

def p_json_object_dict(require_type: AcceptAsObjectType = None, **kwargs) -> _ManagedNameProperty[AcceptAsJsonDict | t.Mapping[str, DataDictObject], t.Mapping[str, DataDictObject], t.Mapping[str, DataDictObject]]:
    if require_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(_ensure_type, require_type=require_type))
    return p_json_dict(
        value_coerce=_coerce_ddo,
        **kwargs
    )

def p_json_enum_set[X: enum.Enum](enum_type: type[X], **kwargs) -> _ManagedNameProperty[t.Iterable[AcceptAsEnum[X]], set[X], set[X]]:
    if 'value_validators' not in kwargs:
        kwargs['value_validators'] = []
    kwargs['value_validators'].append(functools.partial(_ensure_type, require_type=enum_type))
    return p_json_set(
        value_coerce=functools.partial(_coerce_enum, enum_type=enum_type),
        **kwargs
    )

def p_nonumpy(**kwargs) -> _ManagedNameProperty[NumpyNumberLike, NumberLike, NumberLike]:
    if 'validators' not in kwargs:
        kwargs['validators'] = []
    kwargs['validators'].append(_ensure_number)
    return ddo_property(
        coerce=unnumpy,
        **kwargs
    )

def p_i18n_text(**kwargs) -> _ManagedNameProperty[AcceptAsLanguageDict, LanguageDict, LanguageDict]:
    return ddo_property(
        coerce=_coerce_multilingual_text,
        **kwargs
    )