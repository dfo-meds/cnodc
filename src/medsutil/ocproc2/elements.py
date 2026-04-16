"""Structural elements of OCPROC2 values."""
import decimal
import functools
import hashlib
import typing as t
import datetime

from uncertainties import ufloat, UFloat
from decimal import Decimal

from medsutil.awaretime import AwareDateTime
from medsutil.lazy_load import LazyLoadDict
from medsutil.units.units import convert
import medsutil.awaretime as awaretime
import medsutil.types as ct
from medsutil.sanitize import coerce
import medsutil.ocproc2.util as ocut
from medsutil.ocproc2.util import SupportedStorage

UNIFORM_CONVERSION_FACTOR = decimal.Decimal("0.57735026918963")
type SupportedValueOrElement = ocut.SupportedValue | AbstractElement
DefaultValueDict = dict[str, SupportedValueOrElement]


def normalize_data_value(dv: ocut.SupportedValue) -> ocut.SupportedStorage:
    """Convert a data value to its normalized form for OCPROC2."""
    if dv is None or isinstance(dv, (bool, float, int)):
        return dv
    if isinstance(dv, str):
        return coerce.as_normalized_string(dv)
    if isinstance(dv, t.Mapping):
        return {str(x): normalize_data_value(y) for x, y in dv.items()}
    if isinstance(dv, t.Iterable):
        return [normalize_data_value(x) for x in dv]
    if isinstance(dv, (datetime.date, datetime.time)):
        return coerce.date_as_iso_string(dv)
    if isinstance(dv, decimal.Decimal):
        return str(dv)
    raise ValueError(f"Invalid OCPROC2 value: [{dv}] of type [{dv.__class__.__name__}]")


def duck_type_catch[**P](cb: t.Callable[P, None]) ->t.Callable[P, bool]:
    """Wrapper to check using duck typing"""
    @functools.wraps(cb)
    def _inner(*args, **kwargs) -> bool:
        try:
            cb(*args, **kwargs)
            return True
        except (TypeError, ValueError):
            return False
    return _inner


type AnyElementExport = ExportMultipleWithMetadata | ExportWithMetadata | ExportComplexValue | ocut.SupportedStorage | list[AnyElementExport] | ocut.SupportedStorage
type MetadataDict = dict[str, AnyElementExport]

class ExportWithMetadata(t.TypedDict):
    _value: ocut.SupportedStorage
    _metadata: MetadataDict


class ExportComplexValue(t.TypedDict):
    _value: ocut.SupportedStorage


class ExportMultipleWithMetadata(t.TypedDict):
    _values: list[AnyElementExport]
    _metadata: MetadataDict


class AbstractElement[X: SupportedStorage]:
    """Base class for Value and MultiValue."""

    _metadata: ElementMap | None = None

    def __repr__(self) -> str:  # pragma: no coverage
        s = f'{self.__class__.__name__}({str(self)})'
        if self.metadata:
            s += "("
            s += ';'.join(f"{x}={repr(self.metadata[x])}" for x in self.metadata)
            s += ")"
        return s

    @property
    def value(self) -> X:
        """Get the value associated with this entry."""
        raise NotImplementedError  # pragma: no coverage

    @property
    def metadata(self) -> ElementMap:
        if self._metadata is None:
            self._metadata = ElementMap()
        return t.cast(ElementMap, self._metadata)

    @t.overload
    def best(self) -> X: ...

    @t.overload
    def best[T](self, coerce: t.Callable[[X], T] | type[T]) -> T | None: ...

    def best[T](self, coerce: t.Optional[t.Callable[[X], T] | type[T]] = None) -> X | T | None:
        """Find the best value and coerce it if needed."""
        v = self.ideal().value
        if coerce is not None and v is not None:
            return coerce(v)
        return v

    @duck_type_catch
    def is_numeric(self):
        """Check if the value is a number."""
        self.to_float()
        return True

    @duck_type_catch
    def is_integer(self):
        """Check if the value is an integer."""
        self.to_int()

    @duck_type_catch
    def is_iso_datetime(self):
        """Check if the value is an ISO 8601 date-time."""
        self.to_datetime()

    def _coerce_to_numeric[T: (int, float, Decimal, UFloat)](self,
                                                             coerce: t.Callable[[str | int | float | None], T] | type[T],
                                                             units: str | None = None,
                                                             no_loss: bool = False) -> T:
        bv: AbstractElement = self.ideal()
        v = bv.value
        if v is None:
            raise ValueError('Cannot convert None to numeric')
        true_v = coerce(v)
        if no_loss:
            diff = abs(float(v) - true_v)
            if diff > 1e-9:
                raise ValueError("Loss of value encountered")
        return convert(true_v, bv.units(), units)

    def to_decimal(self, units: t.Optional[str] = None) -> decimal.Decimal:
        """Convert this value to a decimal number"""
        return self._coerce_to_numeric(Decimal, units)

    def to_ufloat(self, units: t.Optional[str] = None) -> UFloat | float:
        """Convert this value to a UFloat."""
        bv: AbstractElement = self.ideal()
        if bv.metadata.has_value('Uncertainty'):
            unc = bv.metadata.best('Uncertainty', coerce=Decimal)
            if bv.metadata.best('UncertaintyType', 'normal') == 'uniform':
                unc = unc * UNIFORM_CONVERSION_FACTOR
            if unc != decimal.Decimal("0"):
                return self._coerce_to_numeric(lambda x: ufloat(x, unc), units)
        return self._coerce_to_numeric(float, units)

    def to_float(self, units: t.Optional[str] = None) -> float:
        """Convert this value to a float."""
        return self._coerce_to_numeric(float, units)

    def to_int(self, units: t.Optional[str] = None, no_loss: bool = True) -> int:
        """Convert this value to an integer."""
        return self._coerce_to_numeric(int, units, no_loss)

    def to_datetime(self) -> AwareDateTime:
        """Convert this value to a datetime."""
        return awaretime.utc_from_isoformat(t.cast(str, self.ideal().value))

    def to_date(self) -> datetime.date:
        """ Convert this value to a date. """
        return datetime.date.fromisoformat(t.cast(str, self.ideal().value))

    def to_time(self) -> datetime.time:
        return datetime.time.fromisoformat(t.cast(str, self.ideal().value))

    def to_string(self) -> str:
        return str(self.ideal().value)

    def update_hash(self, h: ct.SupportsHashUpdate):
        """Update a hash with the unique value of this value."""
        for v in self.all_values(True):
            if v.value is None:
                h.update(b'\x00')
            else:
                h.update(str(v.value).encode('utf-8', 'replace'))
            v.metadata.update_hash(h)

    @property
    def quality(self) -> int:
        if self.metadata.has_value('Quality'):
            return self.metadata.best('Quality', coerce=int)
        if self.metadata.has_value('WorkingQuality'):
            return self.metadata.best('WorkingQuality', coerce=int)
        if self.is_empty():
            return 9
        return 0

    def working_quality(self) -> int:
        """Retrieve the working quality of the value."""
        return self.ideal().metadata.best('WorkingQuality', default=0, coerce=int)

    def units(self) -> t.Optional[str]:
        """Retrieve the units of the value."""
        return self.ideal().metadata.best('Units', coerce=str)

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

    def is_multivalue(self) -> bool:
        return False

    def ideal(self) -> SingleElement: raise NotImplementedError
    def all_values(self, srt: bool = False) -> t.Iterable[SingleElement]: raise NotImplementedError
    def to_mapping(self) -> AnyElementExport: raise NotImplementedError
    def from_mapping(self, map_: AnyElementExport): raise NotImplementedError
    def find_child(self, path: list[str]): raise NotImplementedError
    def stable_sort_key(self) -> bytes: raise NotImplementedError

    @staticmethod
    def build_from_mapping(map_: AnyElementExport) -> AbstractElement:
        if isinstance(map_, t.Mapping):
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
        elif isinstance(map_, t.Iterable) and not isinstance(map_, str):
            return MultiElement(
                AbstractElement.build_from_mapping(x) for x in map_
            )
        else:
            return SingleElement(map_, _skip_normalization=True)



class SingleElement(AbstractElement[SupportedStorage]):
    """Represents a single value with a single set of metadata."""

    __slots__ = ('_metadata', '_value')

    def __init__(self, value: ocut.SupportedValue = None, _skip_normalization: bool = False, **kwargs):
        self._value: ocut.SupportedStorage = t.cast(ocut.SupportedStorage, value) if _skip_normalization else normalize_data_value(value)
        self._metadata = None
        if kwargs:
            self.metadata.update(kwargs)

    def __contains__(self, item) -> bool:
        return item == self._value

    def __eq__(self, other: AbstractElement) -> bool:
        try:
            return self.value == other.value and self.metadata == other.metadata
        except AttributeError:
            return False

    def __str__(self) -> str:
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
    def value(self) -> ocut.SupportedStorage:
        return self._value

    @value.setter
    def value(self, value: ocut.SupportedValue):
        self._value = normalize_data_value(value)

    def stable_sort_key(self) -> bytes:
        h = hashlib.new('sha256')
        if self._value is None:
            h.update(b"\x00")
        else:
            h.update(str(self._value).encode('utf-8', errors='replace'))
        self.metadata.update_hash(t.cast(ct.SupportsHashUpdate, t.cast(object, h)))
        return h.digest()

    def to_mapping(self) -> ExportWithMetadata | ExportComplexValue | ocut.SupportedStorage:
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
    def build(v: t.Any, metadata: DefaultValueDict = None):
        v = SingleElement(v)
        if metadata is not None:
            v.metadata.update(metadata)
        return v


class MultiElement(AbstractElement[list[AbstractElement]]):
    """Represents a set of multiple values."""

    __slots__ = ('_value', '_metadata')

    def __init__(self, values: t.Iterable[AbstractElement | ocut.SupportedValue] = None, _skip_normalization: bool = False, **kwargs):
        self._value: list[AbstractElement] = [
            t.cast(AbstractElement, v)
            if _skip_normalization or isinstance(v, AbstractElement) else
            SingleElement(v)
            for v in values
        ] if values else []
        self._metadata = None
        if kwargs:
            self.metadata.update(kwargs)

    def __str__(self) -> str:
        return '\n'.join(str(x) for x in self._value)  # pragma: no coverage

    def __len__(self) -> int:
        return len(self._value)

    def __getitem__(self, item: int) -> AbstractElement:
        return self._value[item]

    def __eq__(self, other: AbstractElement) -> bool:
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

    def ideal(self) -> SingleElement | None:
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

    def all_values(self, srt: bool = False) -> t.Iterable[SingleElement]:
        if srt:
            items = [x for x in self.all_values(False)]
            yield from sorted(items, key=lambda x: x.stable_sort_key())
        else:
            for v in self._value:
                yield from v.all_values()

    def values(self) -> list[AbstractElement]:
        return self._value

    @property
    def value(self) -> list[AbstractElement]:
        return self._value

    def append(self, value: AbstractElement):
        self._value.append(value)

    def to_mapping(self) -> ExportMultipleWithMetadata | list[AnyElementExport]:
        md = self.metadata
        if md:
            export: ExportMultipleWithMetadata = {
                '_values': [v.to_mapping() for v in self._value],
                '_metadata': md.to_mapping()
            }
            return export
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

    def update_hash(self, h: ct.SupportsHashUpdate):
        """Update a hash with all the values of this map"""
        for k in sorted(self.keys()):
            h.update(k.encode('utf-8', 'replace'))
            self._load(k).update_hash(h)

    @t.overload
    def best(self, item: str) -> ocut.SupportedStorage: ...

    @t.overload
    def best[T](self, item: str, coerce: t.Callable[[ocut.SupportedStorage], T] | type[T]) -> T: ...

    @t.overload
    def best[Y](self, item: str, default: Y) -> ocut.SupportedStorage | Y: ...

    @t.overload
    def best[T,Y](self, item: str, default: Y, coerce: t.Callable[[ocut.SupportedStorage], T] | type[T]) -> T | Y: ...

    def best[T,Y](self, item: str, default=None, coerce=None):
        """Find the best value for the given element name, or the default if it is not set."""
        try:
            return self._load(item).best(coerce=coerce)
        except KeyError:
            return default

    def ideal(self, item: str) -> SingleElement | None:
        try:
            return self._load(item).ideal()
        except KeyError:
            return None

    def has_value(self, item: str) -> bool:
        """Check if the item exists in the map and has a non-empty value."""
        if item not in self:
            return False
        return not self._load(item).is_empty()

    def append_to(self,
                  element_name: str,
                  value: SupportedValueOrElement,
                  metadata: t.Optional[DefaultValueDict] = None,
                  **kwargs: SupportedValueOrElement):
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
            value: SupportedValueOrElement,
            metadata: t.Optional[DefaultValueDict] = None,
            **kwargs: SupportedValueOrElement):
        """Set an element to the given value and metadata."""
        self.set_element(element_name, ElementMap.ensure_element(value, metadata, **kwargs))

    def set_element(self, element_name: str, value: AbstractElement):
        super().__setitem__(element_name, value)

    def __setitem__(self, key: str, value: SupportedValueOrElement):
        self.set_element(key, ElementMap.ensure_element(value))

    def set_many(self,
                 element_name: str,
                 values: t.Sequence[SupportedValueOrElement],
                 common_metadata: t.Optional[DefaultValueDict] = None,
                 specific_metadata: t.Optional[t.Sequence[DefaultValueDict | None]] = None,
                 metadata: t.Optional[DefaultValueDict] = None):
        """Build a multi-valued element from the given values."""
        cm = common_metadata or {}
        self.set_many_elements(
            element_name=element_name,
            values=(
                self.ensure_element(values[i], specific_metadata[i] if specific_metadata is not None else None, **cm)
                for i in range(0, len(values))
            ),
            metadata=metadata
        )

    def set_many_elements(self,
                          element_name: str,
                          values: t.Iterable[AbstractElement],
                          metadata: t.Optional[DefaultValueDict] = None):
        if element_name not in self:
            element = MultiElement(values, _skip_normalization=True)
            self.set_element(element_name, element)
        else:
            element = self.get(element_name)
            if isinstance(element, MultiElement):
                element.value.extend(values)
            else:
                element = MultiElement([element], _skip_normalization=True)
                self.set_element(element_name, element)
        if metadata:
            element.metadata.update(metadata)

    def update(self, map_: dict[str, SupportedValueOrElement] = None, **kwargs: SupportedValueOrElement):
        if map_ is not None:
            for key in map_:
                self.set(key, map_[key])
        if kwargs:
            for key in kwargs:
                self.set(key, kwargs[key])

    def update_elements(self, map_: dict[str, AbstractElement] = None, **kwargs: AbstractElement):
        if map_ is not None:
            for key in map_:
                self.set_element(key, map_[key])
        if kwargs:
            for key in kwargs:
                self.set_element(key, kwargs[key])

    def to_mapping(self) -> MetadataDict:
        return super().to_mapping()

    def from_mapping(self, map_: MetadataDict):
        super().from_mapping(t.cast(dict[str, ct.SupportsNativeJson], map_))

    @staticmethod
    def ensure_element(value: SupportedValueOrElement, metadata: t.Optional[DefaultValueDict] = None, **kwargs: SupportedValueOrElement) -> AbstractElement:
        v = SingleElement(value) if not isinstance(value, AbstractElement) else value
        if metadata:
            v.metadata.update(metadata)
        if kwargs:
            v.metadata.update(kwargs)
        return v
