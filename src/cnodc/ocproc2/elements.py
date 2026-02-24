"""Structural elements of OCPROC2 values."""
from __future__ import annotations
import decimal
import functools
import typing as t
import datetime

from uncertainties import ufloat, UFloat

from cnodc.ocproc2.lazy_load import LazyLoadDict
from cnodc.science.units.units import convert


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


def normalize_data_value(dv: t.Any):
    """Convert a data value to its normalized form for OCPROC2."""
    cls_name = dv.__class__.__name__
    if cls_name == 'str' or cls_name == 'float' or cls_name == 'int' or cls_name == 'bool' or cls_name == 'NoneType':
        return dv
    elif cls_name == 'datetime' or cls_name == 'date':
        return dv.isoformat()
    elif cls_name == 'list' or cls_name == 'set' or cls_name == 'tuple':
        return [normalize_data_value(x) for x in dv]
    elif cls_name == 'dict':
        return {
            str(x): normalize_data_value(dv[x])
            for x in dv
        }
    else:
        raise ValueError(f'Invalid value [{cls_name}]')


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

    def __repr__(self):
        s = f'{self.__class__.__name__}({str(self)})'
        if self.metadata:
            s += "("
            s += ';'.join(f"{x}={repr(self.metadata[x])}" for x in self.metadata)
            s += ")"
        return s

    def working_quality(self) -> int:
        """Retrieve the working quality of the value."""
        return self.ideal_single_value().metadata.best_value('WorkingQuality', default=0, coerce=int)

    def units(self) -> t.Optional[str]:
        """Retrieve the units of the value."""
        return self.ideal_single_value().metadata.best_value('Units')

    def best_value(self, coerce: t.Optional[callable] = None) -> t.Any:
        """Find the best value and coerce it if needed."""
        v = self.ideal_single_value().value
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
        bv = self.ideal_single_value()
        if units:
            return convert(decimal.Decimal(bv.value), bv.units(), units)
        return decimal.Decimal(bv.value)

    def to_float_with_uncertainty(self, units: t.Optional[str] = None) -> t.Union[UFloat, float]:
        """Convert this value to a UFloat."""
        bv = self.ideal_single_value()
        if bv.metadata.has_value('Uncertainty'):
            # TODO: uncertainty type
            value = ufloat(float(bv.value), bv.metadata.best_value('Uncertainty', coerce=float))
        else:
            value = float(bv.value)
        if units:
            return convert(value, bv.units(), units)
        return value

    def to_float(self, units: t.Optional[str] = None) -> float:
        """Convert this value to a float."""
        bv = self.ideal_single_value()
        if units:
            return convert(float(bv.value), bv.units(), units)
        return float(bv.value)

    def to_int(self, units: t.Optional[str] = None) -> int:
        """Convert this value to an integer."""
        bv = self.ideal_single_value()
        if isinstance(bv.value, float) or (isinstance(bv.value, str) and '.' in bv.value):
            raise ValueError('Loss of precision')
        if units:
            return convert(int(bv.value), bv.units(), units)
        return int(bv.value)

    def to_datetime(self) -> datetime.datetime:
        """Convert this value to a datetime."""
        return datetime.datetime.fromisoformat(self.ideal_single_value().value)

    def to_string(self) -> str:
        return str(self.ideal_single_value().value)

    @staticmethod
    def build_from_mapping(map_: t.Any):
        if not isinstance(map_, dict):
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
        for v in self.all_values():
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
            wq = v.metadata.best_value('WorkingQuality', 0, int)
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
            wq = v.metadata.best_value('WorkingQuality', 0, int)
            if wq not in (1, 2, 5):
                return False
        return True

    def is_empty(self) -> bool:
        """Check if the value is empty."""
        for v in self.all_values():
            if v.value is not None and v.value != '':
                return False
        return True

    def is_multivalue(self) -> bool:
        return False

    def ideal_single_value(self) -> SingleElement:
        """Find the ideal representation of this value."""
        raise NotImplementedError  # pragma: no coverage

    def all_values(self) -> t.Iterable:
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
            return self.metadata.best_value('Quality')
        if self.metadata.has_value('WorkingQuality'):
            return self.metadata.best_value('WorkingQuality')
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
        return False

    def __eq__(self, other: AbstractElement):
        if other.is_multivalue():
            if len(other.value) == 1:
                return self.__eq__(other.value[0])
            return False
        else:
            return self.value == other.value and self.metadata == other.metadata

    def __str__(self):
        return str(self._value)

    def find_child(self, path: list[str]):
        if (not path) or path[0] == "0":
            return self
        elif path[0] == 'metadata':
            return self.metadata.find_child(path[1:])
        else:
            return None

    def all_values(self) -> t.Iterable:
        yield self

    def ideal_single_value(self) -> SingleElement:
        return self

    @property
    def value(self) -> SupportedValue:
        return self._value

    @value.setter
    def value(self, value: SupportedValue):
        self._value = normalize_data_value(value)

    def to_mapping(self):
        md = self._metadata.to_mapping() if self._metadata else None
        if md:
            return {
                '_value': self._value,
                '_metadata': md
            }
        elif isinstance(self._value, dict):
            return {
                '_value': self._value,
                '_metadata': {}
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
        return '\n'.join(str(x) for x in self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __getitem__(self, item) -> AbstractElement:
        return self._value[item]

    def __eq__(self, other):
        if isinstance(other, SingleElement):
            if len(self._value) == 1:
                return other == self._value[0]
            return False
        elif isinstance(other, MultiElement):
            return len(other) == len(self) and all(other[x] == self[x] for x in range(0, len(self)))
        else:
            return False

    def ideal_single_value(self) -> t.Optional[SingleElement]:
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
        elif path[0] == 'metadata':
            return self.metadata.find_child(path[1:])
        elif path[0].isdigit():
            idx = int(path[0])
            if 0 <= idx < len(self._value):
                return self._value[idx]
            return None
        else:
            return None

    def all_values(self) -> t.Iterable:
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
        return {
            '_values': [v.to_mapping() for v in self._value],
            '_metadata': self._metadata.to_mapping() if self._metadata else {}
        }


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
            return self.load(path[0]).find_child(path[1:])
        except KeyError:
            return None

    def update_hash(self, h):
        """Update a hash with all the values of this map"""
        for k in sorted(self.keys()):
            h.update(k.encode('utf-8', 'replace'))
            self.load(k).update_hash(h)

    def best_value(self, item, default=None, coerce=None):
        """Find the best value for the given element name, or the default if it is not set."""
        try:
            return self.load(item).best_value(coerce=coerce)
        except KeyError:
            return default

    def has_value(self, item: str):
        """Check if the item exists in the map and has a non-empty value."""
        if item not in self:
            return False
        return not self.load(item).is_empty()

    def add_element(self,
                    element_name: str,
                    value: OCProcValue,
                    metadata: t.Optional[DefaultValueDict] = None,
                    **kwargs):
        element = ElementMap.ensure_element(value, metadata, **kwargs)
        if element_name not in self:
            self.set(element_name, element, _check=False)
        else:
            e = self.load(element_name)
            if isinstance(e, MultiElement):
                e.append(element)
            elif e.value != element.value:
                ne = MultiElement((e, element))
                self.set(element_name, ne, _check=False)

    def set_element(self,
                    element_name: str,
                    value: OCProcValue,
                    metadata: t.Optional[DefaultValueDict] = None,
                    **kwargs):
        """Set an element to the given value and metadata."""
        self.set(element_name, ElementMap.ensure_element(value, metadata, **kwargs), _check=False)

    def set(self, item: str, value, _check: bool = True):
        if _check and not isinstance(value, AbstractElement):
            value = SingleElement(value)
        super().set(item, value)

    def set_multiple(self,
                     element_name: str,
                     values: t.Sequence[OCProcValue],
                     common_metadata: t.Optional[DefaultValueDict] = None,
                     specific_metadata: t.Optional[t.Sequence[DefaultValueDict]] = None,
                     metadata: t.Optional[DefaultValueDict] = None):
        """Build a multi-valued element from the given values."""
        for i in range(0, len(values)):
            value_metadata = {}
            if common_metadata:
                value_metadata.update(common_metadata)
            if specific_metadata:
                value_metadata.update(specific_metadata[i])
            element = self.ensure_element(values[i], value_metadata)
            self.add_element(element_name, element)
        if metadata:
            obj = self.get(element_name)
            for key in metadata:
                obj.metadata[key] = metadata[key]

    def update(self, map_: dict = None, **kwargs):
        if map_ is not None:
            for key in map_:
                self.set_element(key, map_[key])
        if kwargs:
            for key in kwargs:
                self.set_element(key, kwargs[key])

    @staticmethod
    def ensure_element(value: OCProcValue, metadata: t.Optional[DefaultValueDict] = None, **kwargs):
        if not isinstance(value, AbstractElement):
            value = SingleElement(value)
        if metadata:
            value.metadata.update(metadata)
        if kwargs:
            value.metadata.update(kwargs)
        return value
