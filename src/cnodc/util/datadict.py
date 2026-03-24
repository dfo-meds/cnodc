import datetime
import enum
import functools
import inspect
import typing as t
import uuid
from contextlib import contextmanager
from uuid import uuid4

from cnodc.util.awaretime import AwareDateTime


class _ManagedProperty(property):

    def __init__(self, *args, **kwargs):
        self.managed_name = kwargs.pop('managed_name', None)
        self.default = kwargs.pop('default', None)
        super().__init__(*args, **kwargs)


class _DelayedDefaultValue:

    def __init__(self, cb: t.Callable):
        self._cb = cb

    def __call__(self):
        return self._cb()


newdict = _DelayedDefaultValue(lambda: {})
newlist = _DelayedDefaultValue(lambda: [])
newset = _DelayedDefaultValue(lambda: {})
newuuid = _DelayedDefaultValue(lambda: str(uuid.uuid4()))


def ddo_property(
        dict_key: str,
        *,
        coerce: t.Callable = None,
        coerce_get: t.Callable = None,
        default=...,
        readonly: bool = False):
    return _ManagedProperty(
        functools.partial(DataDictObject.get_data, key_name=dict_key, coerce=coerce_get),
        functools.partial(DataDictObject.set_data, key_name=dict_key, coerce=coerce, readonly=readonly),
        managed_name=dict_key,
        default=default
    )


def ddo_int(dict_key: str, **kwargs):
    return ddo_property(dict_key, **kwargs, coerce=int)

def ddo_float(dict_key: str, **kwargs):
    return ddo_property(dict_key, **kwargs, coerce=float)

def ddo_bool(dict_key: str, **kwargs):
    return ddo_property(dict_key, **kwargs, coerce=bool)

def ddo_str(dict_key: str, **kwargs):
    return ddo_property(dict_key, **kwargs, coerce=str)

def ddo_datetime(dict_key: str, **kwargs):
    return ddo_property(
        dict_key=dict_key,
        coerce=lambda x: x if isinstance(x, str) else x.isoformat(),
        coerce_get=lambda x: datetime.datetime.fromisoformat(x),
        **kwargs
    )

def ddo_awaredatetime(dict_key: str, **kwargs):
    return ddo_property(
        dict_key=dict_key,
        coerce=lambda x: x if isinstance(x, str) else x.isoformat(),
        coerce_get=lambda x: AwareDateTime.fromisoformat(x),
        **kwargs
    )

def ddo_date(dict_key: str, **kwargs):
    return ddo_property(
        dict_key=dict_key,
        coerce=lambda x: x if isinstance(x, str) else x.isoformat(),
        coerce_get=lambda x: datetime.date.fromisoformat(x),
        **kwargs
    )

def ddo_enum(dict_key: str, enum_type: type[enum.Enum], **kwargs):
    return ddo_property(
        dict_key=dict_key,
        coerce=lambda x: x.value,
        coerce_get=lambda x: enum_type(x),
        **kwargs
    )


class DataDictObject:

    _cache = None

    def __init__(self, **kwargs):
        self._data = {}
        self._allow_readonly_access: bool = False
        defaults = self.get_defaults()
        for key in defaults:
            if key in kwargs:
                setattr(self, key, kwargs.pop(key))
            elif defaults[key] is not Ellipsis:
                if isinstance(defaults[key], _DelayedDefaultValue):
                    setattr(self, key, defaults[key]())
                else:
                    setattr(self, key, defaults[key])
            else:
                raise ValueError(f'Missing argument [{key}] for [{self.__class__.__name__}]')
        if kwargs:
            raise AttributeError(f'Unhandled keyword arguments [{','.join(kwargs)}] for [{self.__class__.__name__}]')

    @contextmanager
    def readonly_access(self):
        try:
            self._allow_readonly_access = True
            yield self
        finally:
            self._allow_readonly_access = False

    def get_data(self, key_name: str, default=None, *, coerce: t.Callable = None):
        if key_name in self._data:
            value = self._data[key_name]
            return coerce(value) if coerce is not None and value is not None else value
        return default

    def set_data(self, value: t.Any, *, key_name: str, coerce: t.Callable = None, readonly: bool = False):
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{key_name} is read-only")
        self._data[key_name] = coerce(value) if coerce is not None and value is not None else value

    @classmethod
    def get_defaults(cls) -> dict[str, t.Any]:
        if DataDictObject._cache is None:
            DataDictObject._cache = {}
        if cls not in DataDictObject._cache:
            DataDictObject._cache[cls] = {}
            attrs = dir(cls)
            for attr_name in attrs:
                if not attr_name[0] == '_':
                    attr = inspect.getattr_static(cls, attr_name)
                    if isinstance(attr, _ManagedProperty):
                        DataDictObject._cache[cls][attr_name] = attr.default
        return DataDictObject._cache[cls]
