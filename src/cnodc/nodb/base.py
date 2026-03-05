from __future__ import annotations
import datetime
import enum
import functools
import json
import typing as t
from contextlib import contextmanager

from cnodc.util import CNODCError


def parse_received_date(rdate: t.Union[str, datetime.date]) -> datetime.date:
    """Convert a date string or date object into a date object."""
    if isinstance(rdate, str):
        try:
            return datetime.date.fromisoformat(rdate)
        except (TypeError, ValueError) as ex:
            raise CNODCError(f"Invalid received date [{rdate}]", "NODB", 1000) from ex
    else:
        return rdate


class NODBValidationError(CNODCError):
    """Base exception for validation issues."""

    def __init__(self, msg, num, **kwargs):  # pragma: no coverage
        super().__init__(msg, 'NODB_VALIDATION', num, **kwargs)


class NODBBaseObject:
    """Base class for all NODB objects.

        This provides a lot of tools for building database classes.
    """

    def __init__(self, *, is_new: bool = True, **kwargs):
        self._data = {}
        self.modified_values = set()
        self._allow_set_readonly = True
        self.is_new = is_new
        for x in kwargs:
            if hasattr(self, x):
                setattr(self, x, kwargs[x])
            else:
                self._data[x] = kwargs[x]
        self.loaded_values = set(x for x in kwargs)
        if not is_new:
            # Reset modified values if we loaded an original object
            # so we don't update all the values all the time.
            self.modified_values.clear()
        self._allow_set_readonly = False
        self._cache = {}

    def __str__(self):  # pragma: no coverage (debugging only)
        s = f"{self.__class__.__name__}: "
        s += "; ".join(f"{x}={self._data[x]}" for x in self._data)
        s += " [modified:"
        s += ";".join(self.modified_values)
        s += "]"
        return s

    @contextmanager
    def _readonly_access(self):
        """ Allows temporary read-only access. Not to be used unless you know what you're doing. """
        try:
            self._allow_set_readonly = True
            yield self
        finally:
            self._allow_set_readonly = False

    def _with_cache(self, key: str, cb: callable, *args, **kwargs) -> t.Any:
        """Check if the key exists in the cache."""
        if key not in self._cache:
            self._cache[key] = cb(*args, **kwargs)
        return self._cache[key]

    def clear_cache(self, key: t.Optional[str] = None):
        if key is not None:
            if key in self._cache:
                del self._cache[key]
        else:
            self._cache.clear()

    def get(self, item, default=None):
        """Get an item from the data dictionary."""
        if item in self._data and self._data[item] is not None:
            return self._data[item]
        return default

    def get_for_db(self, item, default=None):
        """Get an item from the data dictionary for insertion into the database."""
        retval = default
        if item in self._data and self._data[item] is not None:
            retval = self._data[item]
        if isinstance(retval, enum.Enum):
            retval = retval.value
        elif isinstance(retval, (list, tuple, set, dict)):
            return json.dumps(retval)
        return retval

    def set(self, value, item, coerce=None, readonly: bool = False):
        """Set a value on the data dictionary."""
        if readonly and not self._allow_set_readonly:
            raise AttributeError(f"{item} is read-only")
        if coerce is not None and value is not None:
            value = coerce(value)
        if not self._value_equal(item, value):
            self._data[item] = value
            self.mark_modified(item)

    def _value_equal(self, item, value) -> bool:
        """Check if the value of an item is equal to the given value."""
        # No item means can't be equal
        if item not in self._data:
            return False
        # Handle the none case for the current value
        if self._data[item] is None:
            return value is None
        # avoid checking None == self._data[item] by handling this case
        elif value is None:
            return False  # self._data[item] is not None, so not equal
        # Two non-none values
        else:
            return self._data[item] == value

    def mark_modified(self, item):
        """Mark an item as modified"""
        self.modified_values.add(item)

    def clear_modified(self):
        """Clear the set of modified values."""
        self.modified_values.clear()

    @classmethod
    def get_table_name(cls):
        """Get the name of the table."""
        if hasattr(cls, 'TABLE_NAME'):
            return cls.TABLE_NAME
        return cls.__name__

    @classmethod
    def get_primary_keys(cls) -> t.Sequence:
        """Get the list of primary keys."""
        if hasattr(cls, 'PRIMARY_KEYS'):
            return cls.PRIMARY_KEYS
        return tuple()

    @classmethod
    def make_property(cls, item: str, coerce=None, readonly: bool = False):
        """Create a property."""
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=coerce, readonly=readonly)
        )

    @classmethod
    def make_datetime_property(cls, item: str, readonly: bool = False):
        """Create a datetime property"""
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=NODBBaseObject.to_datetime, readonly=readonly)
        )

    @classmethod
    def make_date_property(cls, item: str, readonly: bool = False):
        """Create a date property."""
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=NODBBaseObject.to_date, readonly=readonly)
        )

    @classmethod
    def make_enum_property(cls, item: str, enum_cls: type, readonly: bool = False):
        """Create an enum property"""
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=NODBBaseObject.to_enum(enum_cls), readonly=readonly)
        )

    @classmethod
    def make_wkt_property(cls, item: str, readonly: bool = False):
        """Create a text property that will contain a WKT element."""
        # TODO: currently a string but could add better validation
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=str, readonly=readonly)
        )

    @classmethod
    def make_json_property(cls, item: str):
        """Create a property that will contain a JSON list or object."""
        return property(
            functools.partial(NODBBaseObject.get, item=item),
            functools.partial(NODBBaseObject.set, item=item, coerce=NODBBaseObject.to_json)
        )

    @staticmethod
    def to_json(x):
        """Convert a string to JSON."""
        if isinstance(x, str) and x[0] in ('[', '{'):
            return json.loads(x)
        return x

    @staticmethod
    def to_enum(enum_cls):
        """Convert a value to an enum."""
        def _coerce(x):
            if isinstance(x, str):
                return enum_cls(x)
            return x
        return _coerce

    @staticmethod
    def to_datetime(dt):
        """Convert a value to a datetime object."""
        if isinstance(dt, str):
            return datetime.datetime.fromisoformat(dt)
        else:
            return dt

    @staticmethod
    def to_date(dt):
        """Convert a value to a date object."""
        if isinstance(dt, str):
            return datetime.date.fromisoformat(dt)
        else:
            return dt

    @classmethod
    def find_all(cls, db, **kwargs):
        """Find all workflows."""
        yield from db.stream_objects(cls, **kwargs)


class MetadataMixin:

    def set_metadata(self, key, value):
        """Set a metadata property."""
        if self.metadata is None:
            self.metadata = {key: value}
            self.modified_values.add("metadata")
        else:
            self.metadata[key] = value
            self.modified_values.add("metadata")

    def clear_metadata(self, key):
        """Clear a metadata property."""
        if self.metadata is None:
            return
        if key not in self.metadata:
            return
        del self.metadata[key]
        self.modified_values.add("metadata")
        if not self.metadata:
            self.metadata = None

    def get_metadata(self, key, default=None):
        """Get a metadata property."""
        if self.metadata is None or key not in self.metadata:
            return default
        return self.metadata[key]

    def add_to_metadata(self, key, value):
        """Add a value to a metadata set if not in that set already."""
        if self.metadata is None:
            self.metadata = {key: [value]}
            self.modified_values.add("metadata")
        elif key not in self.metadata:
            self.metadata[key] = [value]
            self.modified_values.add("metadata")
        elif value not in self.metadata[key]:
            self.metadata[key].append(value)
            self.modified_values.add("metadata")


IntColumn = functools.partial(NODBBaseObject.make_property, coerce=int)
BooleanColumn = functools.partial(NODBBaseObject.make_property, coerce=bool)
StringColumn = functools.partial(NODBBaseObject.make_property, coerce=str)
FloatColumn = functools.partial(NODBBaseObject.make_property, coerce=float)
UUIDColumn = StringColumn
ByteColumn = NODBBaseObject.make_property
DateTimeColumn = NODBBaseObject.make_datetime_property
DateColumn = NODBBaseObject.make_date_property
EnumColumn = NODBBaseObject.make_enum_property
JsonColumn = NODBBaseObject.make_json_property
WKTColumn = NODBBaseObject.make_wkt_property
