"""
    Provides a dataclass-like base object that offers properties which store their values in a dictionary on the
    class. Also comes with import and export functions to JSON.

    The data dict object basically maintains a dictionary of values. The property functions in this class allow
    us to define a property on the class that we can access like any other property. They store the value in the
    dictionary. They can also clean values before being stored, alter the value returned, and change how a value
    is exported.

    Data dict objects can take keyword arguments to set properties when the object is instantiated, much like a
    dataclass does.
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
from medsutil.delayed import _DelayedDefaultValue, newdict, newlist, newset
import medsutil.types as ct
import medsutil.json as json

type AcceptAsObjectType = type[DataDictObject]
type _ManagedNameSanitizer[StoreType, ExportType] = t.Callable[[StoreType], ExportType]
type _SetCoercer[AcceptType, StoreType] = t.Callable[[AcceptType], StoreType]
type AcceptAsCoercer[AcceptType, StoreType] = _SetCoercer[AcceptType, StoreType] | type[StoreType]
type _GetCoercer[GetType, StoreType] = t.Callable[[StoreType], GetType]
type _ManagedNameValidator[GetType] = t.Callable[[t.Any, GetType], t.NoReturn]


class _ManagedNameGetter[GetType](t.Protocol):
    """ Defines what a getter function looks like. """
    def __call__(self, *, managed_name: str) -> GetType: ...


class _ManagedNameSetter[AcceptType](t.Protocol):
    """ Defines what a setter function looks like. """
    def __call__(self, value: AcceptType, *, managed_name: str): ...


class _ManagedNameDeleter(t.Protocol):
    """ Defines what a deleter function looks like. """
    def __call__(self, *, managed_name: str): ...


class _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType](property):
    """ A property that also stores a name and a default value. """

    AllAcceptTypes = AcceptType | GetType | StoreType | ExportType | None | _DelayedDefaultValue[AcceptType | GetType]
    """ These are all the types we can accept for storage. """

    # noinspection PyTypeChecker
    def __init__(self,
                 fget: t.Optional[_ManagedNameGetter[GetType]] = None,
                 fset: t.Optional[_ManagedNameSetter[AllAcceptTypes]] = None,
                 fdel: t.Optional[_ManagedNameDeleter] = None,
                 doc: t.Optional[str] = None,
                 default: AllAcceptTypes | EllipsisType = None,
                 managed_name: t.Optional[str] = None,
                 sanitizer: t.Optional[_ManagedNameSanitizer[StoreType, ExportType]] = None,
                 order: int = 0):
        """
        :param fget: A function to get the value
        :param fset: A function to set the value
        :param fdel: A function to delete the value
        :param doc: Documentation of the property
        :param default: The default value of the property
        :param managed_name: The name the property is stored as in the dictionary.
        :param sanitizer: A function to call to when exporting to an object.
        :param order: The order of this property.
        """
        self.order = order
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
        """ Sanitizes the value for export. """
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


# helper types
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
        sanitizer: _ManagedNameSanitizer[StoreType, ExportType] | None = None,
        order: int = 0) -> _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType]:
    """
        Builds a managed property for a data dictionary object.
        :param managed_name: The key to store the value in the dictionary.
        :param coerce_set: A function to call when setting the value
        :param coerce_get: A function to call on the value retrieved before it is returned
        :param required: True if this value is required
        :param default: The default value of the property
        :param doc: A docstring for the property
        :param validators: A list of validators for the property
        :param readonly: Whether the property is readonly or not
        :param sanitizer: A function to call when exporting the value
        :param order: The order to check this property in the data dict object.
    """
    if required:
        default = ...
    return _ManagedNameProperty[AcceptType, GetType, StoreType, ExportType](
        fget=functools.partial(DataDictObject.get_data, coerce_get=coerce_get),
        fset=functools.partial(DataDictObject.set_data, coerce_set=coerce_set, readonly=readonly, validators=validators),
        fdel=functools.partial(DataDictObject.del_data, readonly=readonly),
        doc=doc,
        managed_name=managed_name,
        default=default,
        sanitizer=sanitizer,
        order=order
    )


class DataDictObject(object):

    def __init__(self, *args, _cls_=None, **kwargs):
        """
        :param args: Arguments for the next parent class.
        :param _cls_: Used only when building from an exported object.
        :param kwargs: Either properties to set or additional keyword arguments for the next parent class.
        """
        self._data = {}
        self._prop_cache = None
        self._allow_readonly_access: bool = True
        self._in_init: bool = True
        self._init_complete: bool = False
        self._after_init: list[t.Callable] = []
        with self.readonly_access():
            properties: list[_ManagedNameProperty] = [*self._datadict_props().values()]
            properties.sort(key=lambda x: x.order)
            for prop in properties:
                if prop.property_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.property_name))
                    if prop.managed_name:
                        kwargs.pop(prop.managed_name, None)
                elif prop.managed_name and prop.managed_name in kwargs:
                    setattr(self, prop.property_name, kwargs.pop(prop.managed_name))
                elif prop.default is Ellipsis:
                    raise ValueError(f'Missing argument [{prop.property_name}] for [{self.__class__.__name__}]')
                else:
                    self.set_data(prop.default, managed_prop=prop, as_default=True)
        self._in_init = False
        for x in self._after_init:
            x()
        self._init_complete = True
        try:
            super(DataDictObject, self).__init__(*args, **kwargs)
        except TypeError as ex:
            raise TypeError(f"Unexpected arguments to parent class [{len(args)}][{','.join(kwargs.keys())}]") from ex

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
        """ A context manager that allows modification of readonly properties while the context manager is active.
            .. attention::
                This function can bypass the normal validation rules for the data object. Use with create care.
        """
        try:
            self._allow_readonly_access = True
            yield self
        finally:
            self._allow_readonly_access = False

    def to_map(self) -> dict[str, t.Any]:
        """ Returns the underlying dictionary for this object.
            .. attention::
                This returns a reference to the actual dictionary. Take care when modifying it or make a copy.
        """
        return self._data

    def export(self) -> dict[str, ct.SupportsExtendedJson]:
        """ Exports this object to a dictionary. """
        map_ = {'_cls_': dynamic_name(self)}
        for prop in self._datadict_props().values():
            map_[prop.managed_name] = prop.sanitize(getattr(self, prop.property_name))
        return map_

    def get_sanitized_data(self, managed_name: str) -> t.Any:
        """ Retrieves the data for the specified managed property, sanitized for export. """
        prop = self._find_datadict_prop(managed_name)
        return prop.sanitize(getattr(self, prop.property_name))

    def get_data(self, *, managed_prop: _ManagedNameProperty, coerce_get: t.Callable = None) -> t.Any:
        """ Retrieves the data for the specified managed property.
            :raises KeyError: If the managed property doesn't exist.
        """
        managed_name = managed_prop.managed_name
        if managed_name in self._data:
            value = self._data[managed_name]
            return coerce_get(value) if coerce_get is not None and value is not None else value
        raise KeyError(f'Missing {managed_name}')

    def del_data(self, *, managed_prop: _ManagedNameProperty, readonly: bool = False):
        """ Deletes the data for the specified managed property.
            :raises AttributeError: If the managed property doesn't exist.
        """
        managed_name = managed_prop.managed_name
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")
        if managed_name in self._data:
            del self._data[managed_name]

    def set_data(self,
                 value: t.Any,
                 *,
                 managed_prop: _ManagedNameProperty,
                 coerce_set: t.Callable = None,
                 readonly: bool = False,
                 validators: list[t.Callable] | None = None,
                 as_default: bool = False):
        """ Sets the data for the specified managed property.
            :param value: The value to set
            :param managed_prop: The managed property to set it for
            :param coerce_set: A function to call on values before they're set
            :param readonly: Set to true if this property is readonly
            :param validators: A list of validator functions
            :param as_default: Set to true if this property is being set as the default value.
            :raises AttributeError: If the managed property is readonly.
        """
        managed_name = managed_prop.managed_name

        # check if readonly
        if readonly and not self._allow_readonly_access:
            raise AttributeError(f"{managed_name} is read-only")

        # resolve any default values
        if as_default and isinstance(value, _DelayedDefaultValue):
            value = value()

        # handle type coercion on setting
        if value is not None and coerce_set is not None:
            value = coerce_set(value)

        # handle validation
        if value is not None and validators:
            for validator in validators:
                validator(value)

        # track the change
        original = self._data[managed_name] if managed_name in self._data else None

        # set the value
        self._data[managed_name] = value

        # fire an event for the change.
        if not as_default:
            if self._in_init:
                self._after_init.append(functools.partial(self.after_set, managed_name, value, original))
            else:
                self.after_set(managed_name, value, original)

    def set_from_managed_name(self, value: t.Any, name: str):
        """ Sets the data for the specified managed property that is referenced by name. """
        prop = self._find_datadict_prop(name)
        setattr(self, prop.property_name, value)

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        """ Called after data is set for a managed property """
        pass

    @classmethod
    def _find_datadict_prop(cls, name: str) -> _ManagedNameProperty:
        """ Finds a managed property for the given name.
            :raises KeyError: If the managed property doesn't exist.
        """
        props = cls._datadict_props()
        if name in props:
            return props[name]
        else:
            for prop in props.values():
                if prop.property_name == name:
                    return prop
        raise KeyError(f'Missing managed or property name {name}')

    @classmethod
    def _datadict_props(cls) -> dict[str, _ManagedNameProperty]:
        """ Handles caching for _build_datadict_props(). """
        if not hasattr(cls, '_datadict_all_props_'):
            setattr(cls, '_datadict_all_props_', {})
        ddp_by_class = getattr(cls, '_datadict_all_props_')
        if cls not in ddp_by_class:
            ddp_by_class[cls] = cls._build_datadict_props()
        return ddp_by_class[cls]

    @classmethod
    def _build_datadict_props(cls) -> dict[str, _ManagedNameProperty]:
        """ Builds a dictionary of managed properties for this class. """
        props = {}
        for rcls in reversed(cls.mro()):
            if hasattr(rcls, '_datadict_props_'):
                if rcls in rcls._datadict_props_:
                    for prop in rcls._datadict_props_[rcls].values():
                        props[prop.managed_name] = prop
        return props

    @classmethod
    def import_from_map(cls, data: t.Mapping[str, t.Any]) -> DataDictObject:
        """ Builds an object from an export() map. """
        if '_cls_' in data:
            return dynamic_object(data['_cls_'])(**data)
        return cls(**data)

def p_any(**kwargs) -> _SimpleProperty[ct.SupportsExtendedJson, ct.SupportsExtendedJson]:
    """ Builds a property that accepts any type of data and performs no conversion."""
    return ddo_property(**kwargs)

def p_int(**kwargs) -> _SimpleProperty[ct.AcceptAsInteger, int]:
    """ Builds a property that accepts anything that is compatible with py:type:`int`. """
    return ddo_property(**kwargs, coerce_set=int)

def p_float(**kwargs) -> _SimpleProperty[ct.AcceptAsFloat, float]:
    """ Builds a property that accepts anything that is compatible with py:type:`float`. """
    return ddo_property(**kwargs, coerce_set=float)

def p_bool(**kwargs) -> _SimpleProperty[ct.SupportsBool, bool]:
    """ Builds a property that accepts anything that is compatible with py:type:`bool`. """
    return ddo_property(**kwargs, coerce_set=bool)

def p_str(**kwargs) -> _SimpleProperty[ct.SupportsString, str]:
    """ Builds a property that accepts anything that is compatible with py:type:`str`. """
    return ddo_property(**kwargs, coerce_set=str)

def p_date(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, datetime.date, str]:
    """ Builds a property that accepts anything that can be converted to a py:class:`datetime.date`.
        See `medsutil.sanitize.coerce.as_date` for details.
    """
    return ddo_property(
        coerce_set=coerce.as_date,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_datetime(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, datetime.datetime, str]:
    """ Builds a property that accepts anything that can be converted to a py:class:`datetime.datetime`.
        See `medsutil.sanitize.coerce.as_datetime` for details.
    """
    return ddo_property(
        coerce_set=coerce.as_datetime,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_awaretime(**kwargs) -> _ExportedProperty[ct.AcceptAsDateTime, AwareDateTime, str]:
    """ Builds a property that accepts anything that can be converted to a py:class:`medsutil.awaretime.AwareDateTime`.
        See `medsutil.sanitize.coerce.as_awaretime` for details.
    """
    return ddo_property(
        coerce_set=coerce.as_awaretime,
        sanitizer=coerce.date_as_iso_string,
        **kwargs
    )

def p_enum[X](enum_type: type[X], **kwargs) -> _ExportedProperty[ct.AcceptAsEnum[X], X, ct.SupportsNativeJson]:
    """ Builds a property that accepts an enum value or the raw value of one. """
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
    """ Builds a property that accepts a Python mapping.

        :param default: The default value (defaults to a new dictionary)
        :param str_coerce: How to handle parsing a string (or None to not handle it)
        :param value_coerce: How to coerce values of the dictionary.
        :param key_coerce: How to coerce keys of the dictionary.
        :param key_validators: Validators that operate on the keys of the dictionary.
        :param value_validators: Validators that operate on the values of the dictionary.
        :param sanitizer: A function to handle exporting the dictionary.
        :param kwargs: Additional keyword arguments for :py:func:`medsutil.datadict.ddo_property`.
    """
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
    """ Builds a property that accepts a Python list

        :param default: The default value (defaults to a new list)
        :param str_coerce: How to handle parsing a string (or None to not handle it)
        :param value_coerce: How to coerce values of the list.
        :param value_validators: Validators that operate on the values of the list.
        :param sanitizer: A function to handle exporting the list.
        :param kwargs: Additional keyword arguments for :py:func:`medsutil.datadict.ddo_property`.
    """
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
    """ Builds a property that accepts a Python set

        :param default: The default value (defaults to a new set)
        :param str_coerce: How to handle parsing a string (or None to not handle it)
        :param value_coerce: How to coerce values of the set.
        :param value_validators: Validators that operate on the values of the set.
        :param sanitizer: A function to handle exporting the set.
        :param kwargs: Additional keyword arguments for :py:func:`medsutil.datadict.ddo_property`.
    """
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

def _coerce_ddo(v: DataDictObject | t.Mapping | str,
                str_coerce: t.Callable[[str], t.Mapping | DataDictObject] = None,
                required_type: type[DataDictObject] = None) -> DataDictObject:
    """
        Coerces another object to a data dict object.

        - Data dict objects are passed through.
        - Strings may be passed through the provided coercion function.
        - Mappings are converted using py:func:`medsutil.datadict.DataDictObject.import_from_map`.

        :param v: The value to coerce
        :param str_coerce: A function to handle a string being provided
        :param required_type: A subclass of DataDictObject
    """
    if isinstance(v, str) and str_coerce is not None:
        v = str_coerce(v)
    if isinstance(v, DataDictObject):
        return v
    elif isinstance(v, t.Mapping):
        if required_type is not None:
            return required_type.import_from_map(v)
        return DataDictObject.import_from_map(v)
    else:
        raise TypeError('Invalid type for DDO')

def p_ddo(required_type: type[DataDictObject] = None,
          **kwargs) -> _ExportedProperty[dict[str, t.Any] | DataDictObject, DataDictObject, dict[str, ct.SupportsNativeJson]]:
    """ Builds a property that contains another data dict object as a value (exported as a dictionary). """
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
    """ Builds a dictionary property with JSON export and import. """
    if 'sanitizer' not in kwargs or not kwargs['sanitizer']:
        kwargs['sanitizer'] = coerce.as_json_string
    return p_dict(str_coerce=json.load_dict, value_coerce=value_coerce, **kwargs)

def p_json_list[AcceptType, GetType](value_coerce: t.Callable[[AcceptType], GetType] = None, **kwargs) -> _ExportedProperty[t.Iterable[AcceptType] | str, list[GetType], str]:
    """ Builds a list property with JSON export and import. """
    if 'sanitizer' not in kwargs or not kwargs['sanitizer']:
        kwargs['sanitizer'] = coerce.as_json_string
    return p_list(str_coerce=json.load_list, value_coerce=value_coerce, **kwargs)

def p_json_set[X](**kwargs) -> _ExportedProperty[ct.AcceptAsJsonSet, set[X], str]:
    """ Builds a set property with JSON export and import. """
    if 'sanitizer' not in kwargs or not kwargs['sanitizer']:
        kwargs['sanitizer'] = coerce.as_json_string
    return p_set(str_coerce=json.load_set, **kwargs)

def p_json_str_list(**kwargs) -> _ExportedProperty[ct.AcceptAsJsonList, list[str], str]:
    """ Builds a list property of strings with JSON export and import. """
    return p_json_list(value_coerce=str, **kwargs)

def p_json_str_set(**kwargs) -> _ExportedProperty[ct.AcceptAsJsonList, set[str], str]:
    """ Builds a set property of strings with JSON export and import. """
    return p_json_set(value_coerce=str, **kwargs)

def p_json_object(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[dict[str, t.Any] | DataDictObject | ct.JsonDictString, DataDictObject, str]:
    """ Builds a data dict object property with JSON export and import. """
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
    """ Builds a list of data dict objects property with JSON export and import. """
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
    """ Builds a dictionary of strings to data dict objects property with JSON export and import. """
    if required_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(require.type_is, required_type=required_type))
    return p_json_dict(
        value_coerce=functools.partial(_coerce_ddo, required_type=required_type),
        sanitizer=lambda x: json.dumps({str(y): x[y].export() for y in x}),
        **kwargs
    )

def p_object_list(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[t.Iterable[DataDictObject | t.Mapping | str] | ct.JsonListString, list[DataDictObject], list[dict]]:
    """ Builds a list of data dict objects property. """
    if required_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(require.type_is, required_type=required_type))
    return p_list(
        value_coerce=functools.partial(_coerce_ddo, required_type=required_type),
        sanitizer=lambda x: [y.export() for y in x],
        **kwargs
    )

def p_object_dict(required_type: type[DataDictObject] = None, **kwargs) -> _ExportedProperty[t.Mapping[str, DataDictObject | t.Mapping | str] | str, dict[str, DataDictObject], dict[str, dict]]:
    """ Builds a dictionary of strings to data dict objects property. """
    if required_type is not None:
        if 'value_validators' not in kwargs:
            kwargs['value_validators'] = []
        kwargs['value_validators'].append(functools.partial(require.type_is, required_type=required_type))
    return p_dict(
        value_coerce=functools.partial(_coerce_ddo, required_type=required_type),
        sanitizer=lambda x: {str(y): x[y].export() for y in x},
        **kwargs
    )

def p_enum_set[X](enum_type: type[X], **kwargs) -> _ExportedProperty[t.Iterable[ct.AcceptAsEnum[X]], set[X], str]:
    """ Builds a set of enums property. """
    if 'value_validators' not in kwargs:
        kwargs['value_validators'] = []
    kwargs['value_validators'].append(functools.partial(require.type_is, required_type=enum_type))
    return p_set(
        value_coerce=functools.partial(coerce.as_enum, enum_type=enum_type),
        **kwargs
    )

def p_json_enum_set[X](enum_type: type[X], **kwargs) -> _ExportedProperty[t.Iterable[ct.AcceptAsEnum[X]], set[X], str]:
    """ Builds a set of enums property with JSON import and export """
    if 'value_validators' not in kwargs:
        kwargs['value_validators'] = []
    kwargs['value_validators'].append(functools.partial(require.type_is, required_type=enum_type))
    return p_json_set(
        value_coerce=functools.partial(coerce.as_enum, enum_type=enum_type),
        **kwargs
    )

if t.TYPE_CHECKING:
    import numpy as _np
    NumpyNumberLike = ct.NumberLike | _np.int8 | _np.int16 | _np.int32 | _np.int64 | _np.float16 | _np.float32 | _np.float64


def p_nonumpy(**kwargs) -> _SimpleProperty[NumpyNumberLike, ct.NumberLike]:
    """ Builds a numeric property (float or int) and strips numpy-like classes. """
    if 'validators' not in kwargs:
        kwargs['validators'] = []
    kwargs['validators'].append(functools.partial(require.type_is, required_type=(int, float, decimal.Decimal)))
    return ddo_property(
        coerce_set=unnumpy,
        **kwargs
    )

def p_i18n_text(**kwargs) -> _SimpleProperty[ct.AcceptAsLanguageDict, ct.LanguageDict]:
    """ Builds a translatable text dictionary property. """
    return ddo_property(
        coerce_set=coerce.as_i18n_text,
        validators=[functools.partial(require.type_is, required_type=dict)],
        **kwargs
    )

def p_bytearray(**kwargs) -> _SimpleProperty[t.ByteString, bytearray]:
    """ Builds a bytearray property. """
    return ddo_property(
        coerce_set=coerce.as_bytearray,
        **kwargs
    )

def p_bytes(**kwargs) -> _SimpleProperty[t.ByteString, bytes]:
    """ Builds a bytes property. """
    return ddo_property(
        coerce_set=coerce.as_bytes,
        **kwargs
    )

def p_bytes_b64(**kwargs) -> _ExportedProperty[t.ByteString | str, bytes, str]:
    """ Builds a base-64 encoded bytes property."""
    return ddo_property(
        coerce_set=coerce.as_bytes_from_base64,
        sanitizer=coerce.as_base64,
        **kwargs
    )

def p_dynamic_object(**kwargs) -> _ExportedProperty[t.Any, t.Any, str]:
    """ Builds a property that refers to a Python function or class by fully-qualified name. """
    return ddo_property(
        coerce_set=lambda x: dynamic_object(x) if isinstance(x, str) else x,
        sanitizer=dynamic_name,
        **kwargs
    )

def _dynamic_callable(x: t.Any) -> t.Callable:
    """ Given a string or object, returns it if it is callable.
        :raises ValueError: If it is not callable.
    """
    if isinstance(x, str):
        x = dynamic_object(x)
    if callable(x):
        return x
    raise ValueError('Invalid object, not callable')

def p_dynamic_callable(**kwargs) -> _ExportedProperty[t.Any, t.Callable, str]:
    """ Builds a property that refers to a callable Python function or class by fully-qualified name. """
    return ddo_property(
        coerce_set=_dynamic_callable,
        sanitizer=dynamic_name,
        **kwargs
    )


class DataDictModifiedTracker(DataDictObject):
    """ A mix-in class that tracks when values are modified. """

    def __init__(self, is_new: bool = True, **kwargs):
        self._modified_values: set[str] = set()
        self._is_new = is_new
        super().__init__(**kwargs)

    @property
    def is_new(self) -> bool:
        """ Checks if this is a new object. """
        return self._is_new

    @is_new.setter
    def is_new(self, b: bool):
        """ Sets if this is a new object. """
        self._is_new = b

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        """ Handles updating the modified list. """
        super().after_set(managed_name, value, original)
        if (self.is_new or self._init_complete) and original != value:
            self.mark_modified(managed_name)

    @property
    def modified_values(self) -> set[str]:
        """ Retrieves a list of modified values. """
        return self._modified_values

    def mark_modified(self, item: str):
        """Marks an item as modified"""
        self._modified_values.add(item)

    def clear_modified(self):
        """Clears the set of modified values."""
        self._modified_values.clear()
