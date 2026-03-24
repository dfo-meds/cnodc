"""Structural elements of OCPROC2 values."""
from __future__ import annotations
import decimal
import functools
import hashlib
import math
import typing as t
import datetime
from xml.dom.minidom import Element

from uncertainties import ufloat, UFloat

from cnodc.ocproc2.lazy_load import LazyLoadDict
from cnodc.science.units.units import convert
import cnodc.util.awaretime as awaretime


UNIFORM_CONVERSION_FACTOR = decimal.Decimal("0.57735026918963")

SupportedValue = t.Union[
    None,
    str,
    float,
    int,
    list,
    set,
    tuple,
    dict,
    bool,
    datetime.datetime,
    datetime.date
]


CONVERTERS = {
    'str': lambda x: x,
    'float': lambda x: x,
    'int': lambda x: x,
    'bool': lambda x: x,
    'NoneType': lambda x: x,
    'list': lambda x: [normalize_data_value(y) for y in x],
    'set':  lambda x: [normalize_data_value(y) for y in x],
    'tuple': lambda x: [normalize_data_value(y) for y in x],
    'dict': lambda x: {normalize_data_value(y): normalize_data_value(x[y]) for y in x},
    'datetime': lambda x: x.isoformat(),
    'date': lambda x: x.isoformat(),
    'AwareDateTime': lambda x: x.isoformat(),
    # for more datatypes, put the class name and then a lambda function to call that returns the noramlized value.
}

def normalize_data_value(dv: t.Any):
    """Convert a data value to its normalized form for OCPROC2."""
    try:
        return CONVERTERS[dv.__class__.__name__](dv)
    except KeyError:
        raise ValueError(f"Invalid OCPROC2 value: [{dv}]")


def duck_type_catch(cb: callable):
    """Wrapper to check using duck typing"""
    @functools.wraps(cb)
    def _inner(*args, **kwargs) -> bool:
        try:
            cb(*args, **kwargs)
            return True
        except (TypeError, ValueError):
            return False
    return _inner


class AbstractElement:
    """Base class for Value and MultiValue."""

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = ElementMap()
        return self._metadata

    def __repr__(self):  # pragma: no coverage
        s = f'{self.__class__.__name__}({str(self)})'
        if self.metadata:
            s += "("
            s += ';'.join(f"{x}={repr(self.metadata[x])}" for x in self.metadata)
            s += ")"
        return s

    def working_quality(self) -> int:
        """Retrieve the working quality of the value."""
        return self.ideal().metadata.best('WorkingQuality', default=0, coerce=int)

    def units(self) -> t.Optional[str]:
        """Retrieve the units of the value."""
        return self.ideal().metadata.best('Units')

    def best(self, coerce: t.Optional[callable] = None) -> t.Any:
        """Find the best value and coerce it if needed."""
        v = self.ideal().value
        if coerce is not None and v is not None:
            return coerce(v)
        return v

    @duck_type_catch
    def is_numeric(self):
        """Check if the value is a number."""
        self.to_float()

    @duck_type_catch
    def is_integer(self):
        """Check if the value is an integer."""
        self.to_int()

    @duck_type_catch
    def is_iso_datetime(self):
        """Check if the value is an ISO 8601 date-time."""
        self.to_datetime()

    def to_decimal(self, units: t.Optional[str] = None) -> decimal.Decimal:
        """Convert this value to a decimal number"""
        bv = self.ideal()
        try:
            if units:
                return convert(decimal.Decimal(bv.value), bv.units(), units)
            return decimal.Decimal(bv.value)
        except decimal.DecimalException as ex:
            raise ValueError(f"Invalid decimal number [{bv.value}]") from ex

    def to_ufloat(self, units: t.Optional[str] = None) -> t.Union[UFloat, float]:
        """Convert this value to a UFloat."""
        bv = self.ideal()
        value = bv.to_decimal(units)
        unc = None
        if bv.metadata.has_value('Uncertainty'):
            unc = abs(bv.metadata.best('Uncertainty', coerce=decimal.Decimal))
            if bv.metadata.best('UncertaintyType', 'normal') == 'uniform':
                unc = unc * UNIFORM_CONVERSION_FACTOR
        if unc is not None and unc > 0:
            return ufloat(value, convert(unc, bv.units(), units))
        else:
            return float(value)

    def to_float(self, units: t.Optional[str] = None) -> float:
        """Convert this value to a float."""
        return float(self.to_decimal(units))

    def to_int(self, units: t.Optional[str] = None, raise_error_on_precision_loss: bool = True) -> int:
        """Convert this value to an integer."""
        dv = self.to_decimal(units)
        if raise_error_on_precision_loss and "." in str(dv):
            raise ValueError('Loss of precision')
        return int(dv)

    def to_datetime(self) -> datetime.datetime:
        """Convert this value to a datetime."""
        return awaretime.utc_from_isoformat(self.ideal().value)

    def to_date(self) -> datetime.date:
        """ Convert this value to a date. """
        return datetime.date.fromisoformat(self.ideal().value)

    def to_string(self) -> str:
        return str(self.ideal().value)

    @staticmethod
    def build_from_mapping(map_: t.Any):
        type_name = type(map_).__name__
        if type_name == 'list':
            return MultiElement(
                AbstractElement.build_from_mapping(x) for x in map_
            )
        elif type_name != 'dict':
            return SingleElement(map_, _skip_normalization=True)
        else:
            try:
                element = SingleElement(map_['_value'], _skip_normalization=True)
            except KeyError:
                element = MultiElement(
                    (AbstractElement.build_from_mapping(x) for x in map_['_values']),
                    _skip_normalization=True
                )
            try:
                md = map_['_metadata']
                if md:
                    element.metadata.from_mapping(md)
            except KeyError:
                pass
            return element

    def update_hash(self, h):
        """Update a hash with the unique value of this value."""
        for v in self.all_values(True):
            if v.value is None:
                h.update(b'\x00')
            else:
                h.update(str(v.value).encode('utf-8', 'replace'))
            v.metadata.update_hash(h)

    def is_good(self, allow_dubious: bool = False, allow_empty: bool = False) -> bool:
        """Check if there is a non-erroneous (and optionally non-empty, non-dubious) value """
        for v in self.all_values():
            if v.value is None or v.value == '':
                if allow_empty:
                    return True
                else:
                    continue
            wq = v.metadata.best('WorkingQuality', 0, int)
            if wq in (0, 1, 2, 5):
                return True
            if wq == 3 and allow_dubious:
                return True
            elif wq == 9 and allow_empty:
                return True
        return False

    def passed_qc(self) -> bool:
        """Check if this value is good and has completed QC."""
        for v in self.all_values():
            wq = v.metadata.best('WorkingQuality', 0, int)
            if wq not in (1, 2, 5):
                return False
        return True

    def is_empty(self) -> bool:
        """Check if the value is empty."""
        for v in self.all_values():
            if v.value is not None and v.value != '':
                return False
        return True

    def _stable_sort_key(self):
        raise NotImplementedError  # pragma: no coverage

    def is_multivalue(self) -> bool:
        return False

    def ideal(self) -> SingleElement:
        """Find the ideal representation of this value."""
        raise NotImplementedError  # pragma: no coverage

    def all_values(self, srt: bool = False) -> t.Iterable:
        """Retrieve all possible values for this one."""
        raise NotImplementedError  # pragma: no coverage

    def to_mapping(self) -> dict:
        """Convert the value to a map."""
        raise NotImplementedError  # pragma: no coverage

    def from_mapping(self, map_: t.Any):
        """Rebuild the value from a map."""
        raise NotImplementedError  # pragma: no coverage

    def find_child(self, path: list[str]):
        """Find a child value within this value."""
        raise NotImplementedError  # pragma: no coverage

    @property
    def quality(self):
        if self.metadata.has_value('Quality'):
            return self.metadata.best('Quality')
        if self.metadata.has_value('WorkingQuality'):
            return self.metadata.best('WorkingQuality')
        if self.is_empty():
            return 9
        return 0

    @property
    def value(self):
        """Get the value associated with this entry."""
        raise NotImplementedError  # pragma: no coverage


class SingleElement(AbstractElement):
    """Represents a single value with a single set of metadata."""

    __slots__ = ('_metadata', '_value')

    def __init__(self, value: SupportedValue = None, _skip_normalization: bool = False, **kwargs):
        self._value = value if _skip_normalization else normalize_data_value(value)
        self._metadata = None
        if kwargs:
            self.metadata.update(kwargs)

    def __contains__(self, item):
        return item == self._value

    def __eq__(self, other: AbstractElement):
        try:
            return self.value == other.value and self.metadata == other.metadata
        except AttributeError:
            return False

    def __str__(self):
        return str(self._value)  # pragma: no coverage

    def find_child(self, path: list[str]):
        if not path:
            return self
        first_element = path.pop(0)
        if first_element == 0 or first_element == "0":
            return self.find_child(path)
        elif first_element == 'metadata':
            return self.metadata.find_child(path)
        else:
            return None

    def all_values(self, srt: bool = False) -> t.Iterable[AbstractElement]:
        yield self

    def ideal(self) -> SingleElement:
        return self

    @property
    def value(self) -> SupportedValue:
        return self._value

    @value.setter
    def value(self, value: SupportedValue):
        self._value = normalize_data_value(value)

    def _stable_sort_key(self):
        h = hashlib.new('sha256')
        if self._value is None:
            h.update(b"\x00")
        else:
            h.update(str(self._value).encode('utf-8', errors='replace'))
        self.metadata.update_hash(h)
        return h.digest()

    def to_mapping(self):
        md = self.metadata.to_mapping() if self._metadata else None
        if md:
            return {
                '_value': self._value,
                '_metadata': md
            }
        elif type(self._value).__name__ in ('dict', 'list'):
            return {
                '_value': self._value,
            }
        else:
            return self._value

    @staticmethod
    def build(v: t.Any, metadata: dict = None):
        v = SingleElement(v)
        if metadata is not None:
            v.metadata.update(metadata)
        return v


OCProcValue = t.Union[SupportedValue, AbstractElement]
DefaultValueDict = dict[str, OCProcValue]


class MultiElement(AbstractElement):
    """Represents a set of multiple values."""

    __slots__ = ('_value', '_metadata')

    def __init__(self, values = None, _skip_normalization: bool = False):
        self._value = [v if _skip_normalization or isinstance(v, AbstractElement) else SingleElement(v) for v in values] if values else []
        self._metadata = None

    def __str__(self):
        return '\n'.join(str(x) for x in self._value)  # pragma: no coverage

    def __len__(self) -> int:
        return len(self._value)

    def __getitem__(self, item) -> AbstractElement:
        return self._value[item]

    def __eq__(self, other):
        if isinstance(other, SingleElement):
            return any(x == other for x in self._value)
        elif isinstance(other, MultiElement):
            if len(other) != len(self):
                return False
            if self.metadata != other.metadata:
                return False
            if not all(any(x == y for y in other.values()) for x in self.values()):
                return False
            if not all(any(x == y for y in self.values()) for x in other.values()):
                return False
            return True
        else:
            return False

    def ideal(self) -> t.Optional[SingleElement]:
        # TODO: do we need to handle the case where there are no values? seems unlikely
        best_value, best_wq = None, 9
        for v in self.all_values():
            wq = v.quality
            if wq == 1 or wq == 5:
                return v
            if (    best_value is None
                    or (wq == 2 and best_wq not in (2, ))
                    or (wq == 3 and best_wq not in (2, 3))
                    or (wq == 4 and best_wq not in (2, 3, 4))
                    or (wq == 0 and best_wq not in (0, 2, 3))
                    or (wq == 9 and best_wq not in (0, 2, 3, 4))):
                best_value = v
                best_wq = wq
        return best_value

    def is_multivalue(self) -> bool:
        return True

    def find_child(self, path: list[str]):
        if not path:
            return self
        first_element = path.pop(0)
        if first_element == 'metadata':
            return self.metadata.find_child(path)
        try:
            idx = int(first_element)
            if 0 <= idx < len(self._value):
                return self._value[idx].find_child(path)
        except (ValueError, TypeError):
            pass
        return None

    def all_values(self, srt: bool = False) -> t.Iterable:
        if srt:
            items = [x for x in self.all_values(False)]
            yield from sorted(items, key=lambda x: x._stable_sort_key())
        else:
            for v in self._value:
                yield from v.all_values()

    def values(self):
        return self._value

    @property
    def value(self):
        return self._value

    def append(self, value: AbstractElement):
        self._value.append(value)

    def to_mapping(self):
        if self.metadata:
            return {
                '_values': [v.to_mapping() for v in self._value],
                '_metadata': self._metadata.to_mapping()
            }
        return [v.to_mapping() for v in self._value]


class ElementMap(LazyLoadDict[AbstractElement]):
    """Represents a map of element names to values"""

    __slots__ = ('map', '_lazy')

    def __init__(self):
        super().__init__(AbstractElement.build_from_mapping)

    def find_child(self, path: list[str]):
        """Locate an element using an OCPROC2 path expression."""
        if not path:
            return self
        try:
            first_element = path.pop(0)
            return self._load(first_element).find_child(path)
        except KeyError as ex:
            return None

    def update_hash(self, h):
        """Update a hash with all the values of this map"""
        for k in sorted(self.keys()):
            h.update(k.encode('utf-8', 'replace'))
            self._load(k).update_hash(h)

    def best(self, item, default=None, coerce=None):
        """Find the best value for the given element name, or the default if it is not set."""
        try:
            return self._load(item).best(coerce=coerce)
        except KeyError:
            return default

    def has_value(self, item: str):
        """Check if the item exists in the map and has a non-empty value."""
        if item not in self:
            return False
        return not self._load(item).is_empty()

    def append_to(self,
                  element_name: str,
                  value: OCProcValue,
                  metadata: t.Optional[DefaultValueDict] = None,
                  **kwargs):
        self.append_element_to(element_name, ElementMap.ensure_element(value, metadata, **kwargs))

    def append_element_to(self, element_name: str, value: AbstractElement):
        if element_name not in self:
            self.set_element(element_name, value)
        else:
            e = self._load(element_name)
            if isinstance(e, MultiElement):
                e.append(value)
            else:
                ne = MultiElement([e, value], _skip_normalization=True)
                super().__setitem__(element_name, ne)

    def set(self,
            element_name: str,
            value: OCProcValue,
            metadata: t.Optional[DefaultValueDict] = None,
            **kwargs: dict[str, OCProcValue]):
        """Set an element to the given value and metadata."""
        self.set_element(element_name, ElementMap.ensure_element(value, metadata, **kwargs))

    def set_element(self, element_name: str, value: AbstractElement):
        super().__setitem__(element_name, value)

    def __setitem__(self, key, value):
        self.set_element(key, ElementMap.ensure_element(value))

    def set_many(self,
                 element_name: str,
                 values: t.Sequence[OCProcValue],
                 common_metadata: t.Optional[DefaultValueDict] = None,
                 specific_metadata: t.Optional[t.Sequence[DefaultValueDict]] = None,
                 metadata: t.Optional[DefaultValueDict] = None):
        """Build a multi-valued element from the given values."""
        common_metadata = common_metadata or {}
        self.set_many_elements(
            element_name=element_name,
            values=(self.ensure_element(values[i], specific_metadata[i] if specific_metadata is not None else None, **common_metadata) for i in range(0, len(values))),
            metadata=metadata
        )

    def set_many_elements(self, element_name: str, values: t.Iterable[SingleElement], metadata: t.Optional[DefaultValueDict] = None):
        if element_name not in self:
            element = MultiElement(values, _skip_normalization=True)
            self.set_element(element_name, element)
        else:
            element = self.get(element_name)
            if isinstance(element, SingleElement):
                element = MultiElement([element], _skip_normalization=True)
                self.set_element(element_name, element)
            else:
                element.value.extend(values)
        if metadata:
            element.metadata.update(metadata)

    def update(self, map_: dict[str, OCProcValue] = None, **kwargs: dict[str, OCProcValue]):
        if map_ is not None:
            for key in map_:
                self.set(key, map_[key])
        if kwargs:
            for key in kwargs:
                self.set(key, kwargs[key])

    def update_elements(self, map_: dict[str, AbstractElement] = None, **kwargs: dict[str, AbstractElement]):
        if map_ is not None:
            for key in map_:
                self.set_element(key, map_[key])
        if kwargs:
            for key in map_:
                self.set_element(key, map_[key])

    @staticmethod
    def ensure_element(value: OCProcValue, metadata: t.Optional[DefaultValueDict] = None, **kwargs: dict[str, OCProcValue]):
        if not isinstance(value, AbstractElement):
            value = SingleElement(value)
        if metadata:
            value.metadata.update(metadata)
        if kwargs:
            value.metadata.update(kwargs)
        return value
