"""Structural elements of OCPROC2 values."""
import decimal
import functools
import hashlib
import typing as t
import datetime

from medsutil.awaretime import AwareDateTime
from medsutil.lazy_load import LazyLoadDict
from medsutil.units.units import convert
import medsutil.awaretime as awaretime

from medsutil.sanitize import coerce
import medsutil.ocproc2.util as ocut

UNIFORM_CONVERSION_FACTOR = decimal.Decimal("0.57735026918963")

if t.TYPE_CHECKING:
    from medsutil.iso_duration import ISODuration
    from uncertainties import ufloat, UFloat
    import medsutil.types as ct
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


class ExportQCInfo(t.TypedDict):
    sys_flag: int | None
    user_flag: int | None
    ignore_test: bool
    note: str | None
    ref_value: t.Any

class ExportWithMetadata(t.TypedDict):
    _value: ocut.SupportedStorage
    _metadata: t.NotRequired[MetadataDict]
    _qc_info: t.NotRequired[dict[str, ExportQCInfo]]

class ExportComplexValue(t.TypedDict):
    _value: ocut.SupportedStorage

class ExportMultipleWithMetadata(t.TypedDict):
    _values: list[AnyElementExport]
    _metadata: t.NotRequired[MetadataDict]
    _qc_info: t.NotRequired[dict[str, ExportQCInfo]]


class QCInfo:

    def __init__(self,
                 system_recommended_flag: int | None = None,
                 user_provided_flag: int | None = None,
                 ignore_test: bool = False,
                 ref_value: t.Any = None,
                 note: str | None = None):
        self.system_recommended_flag = system_recommended_flag
        self.user_provided_flag = user_provided_flag
        self.ignore_test: bool = ignore_test
        self.ref_value: t.Any = ref_value
        self.note: str | None = note

    def to_mapping(self) -> ExportQCInfo:
        return {
            'sys_flag': self.system_recommended_flag,
            'user_flag': self.user_provided_flag,
            'ignore_test': self.ignore_test,
            'ref_value': self.ref_value,
            'note': self.note
        }

    @classmethod
    def build_from_mapping(cls, map_: dict) -> QCInfo:
        return QCInfo(
            system_recommended_flag=map_.get('sys_flag', None),
            user_provided_flag=map_.get("user_flag", None),
            ignore_test=map_.get("ignore_test", False),
            ref_value=map_.get('ref_value', None),
            note=map_.get('note', None)
        )


class QCInfoMap(LazyLoadDict[QCInfo]):

    def __init__(self):
        super().__init__(QCInfo.build_from_mapping)

    def to_mapping(self) -> dict[str, ExportQCInfo]:
        return {
            k: v.to_mapping()
            for k, v in self.items()
        }


class AbstractElement[X]:
    """Base class for Value and MultiValue."""

    _metadata: ElementMap | None = None
    _qc_info: QCInfoMap | None = None

    def __repr__(self) -> str:  # pragma: no coverage
        s = f'{self.__class__.__name__}({str(self)})'
        if self.metadata:
            s += "("
            s += ';'.join(f"{x}={repr(self.metadata[x])}" for x in self.metadata)
            s += ")"
        return s

    @property
    def qc_info(self) -> QCInfoMap:
        if self._qc_info is None:
            self._qc_info = QCInfoMap()
        return t.cast(QCInfoMap, self._qc_info)

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

    def is_string_like(self):
        return not self.is_list_like()

    def is_list_like(self):
        return isinstance(self.ideal().value, list)

    @duck_type_catch
    def is_numeric(self):
        """Check if the value is a number."""
        self.to_float()
        return True

    @duck_type_catch
    def is_integer(self):
        """Check if the value is an integer."""
        self.to_int()
        return True

    @duck_type_catch
    def is_iso_datetime(self):
        """Check if the value is an ISO 8601 date-time."""
        self.to_datetime()
        return True

    @duck_type_catch
    def is_duration(self):
        self.to_duration()
        return True

    def _coerce_to_numeric[T](self,
                                                             coerce: t.Callable[[str | int | float | None], T] | type[T],
                                                             units: str | None = None,
                                                             no_loss: bool = False) -> T:
        bv: AbstractElement = self.ideal()
        v = bv.value
        if v is None:
            raise ValueError('Cannot convert None to numeric')
        try:
            true_v = coerce(v)
        except (TypeError, ValueError, decimal.DecimalException) as ex:
            raise ValueError('Error during coercion') from ex
        if no_loss:
            diff = abs(float(v) - true_v)
            if diff > 1e-9:
                raise ValueError("Loss of value encountered")
        return convert(true_v, bv.units(), units)

    def to_numeric(self, units: t.Optional[str] = None) -> float:
        """ This will be the type that tests use. """
        return self.to_float(units)

    def to_decimal(self, units: t.Optional[str] = None) -> decimal.Decimal:
        """Convert this value to a decimal number"""
        return self._coerce_to_numeric(decimal.Decimal, units)

    def to_ufloat(self, units: t.Optional[str] = None) -> UFloat | float:
        """Convert this value to a UFloat."""
        bv: AbstractElement = self.ideal()
        if bv.metadata.has_value('Uncertainty'):
            unc = bv.metadata.best('Uncertainty', coerce=decimal.Decimal)
            if bv.metadata.best('UncertaintyType', 'normal') == 'uniform':
                unc = unc * UNIFORM_CONVERSION_FACTOR
            if unc != decimal.Decimal("0"):
                from uncertainties import ufloat
                return self._coerce_to_numeric(lambda x: ufloat(x, abs(unc)), units)
        return self._coerce_to_numeric(float, units)

    def to_float(self, units: t.Optional[str] = None) -> float:
        """Convert this value to a float."""
        return self._coerce_to_numeric(float, units)

    def to_int(self, units: t.Optional[str] = None, no_loss: bool = True) -> int:
        """Convert this value to an integer."""
        return self._coerce_to_numeric(int, units, no_loss)

    def to_duration(self) -> ISODuration:
        """Convert this value to a ISO 8601 duration."""
        from medsutil.iso_duration import ISODuration
        return ISODuration.from_iso_format(self.ideal().to_string())

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

    def units(self) -> t.Optional[str]:
        """Retrieve the units of the value."""
        return self.ideal().metadata.best('Units', coerce=str)

    @property
    def quality(self) -> int:
        if self.metadata.has_value('Quality'):
            return self.metadata.best('Quality', coerce=int)
        elif self.metadata.has_value('WorkingQuality'):
            return self.metadata.best('WorkingQuality', coerce=int)
        elif self.is_empty():
            return 9
        return 0

    @property
    def sensor_rank(self) -> int | None:
        if self.metadata.has_value("SensorRank"):
            return self.metadata.best('SensorRank', coerce=int)
        return None

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
            try:
                qc = map_['_qc_info']
                if qc:
                    element.qc_info.from_mapping(qc)
            except KeyError:
                pass
            return element
        elif isinstance(map_, t.Iterable) and not isinstance(map_, str):
            return MultiElement(
                AbstractElement.build_from_mapping(x) for x in map_
            )
        else:
            return SingleElement(map_, _skip_normalization=True)



class SingleElement(AbstractElement):
    """Represents a single value with a single set of metadata."""

    __slots__ = ('_metadata', '_value', '_qc_info')

    def __init__(self, value: ocut.SupportedValue = None, _skip_normalization: bool = False, **kwargs):
        self._value: ocut.SupportedStorage = value if _skip_normalization else normalize_data_value(value)
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
        self.metadata.update_hash(h)
        return h.digest()

    def to_mapping(self) -> ExportWithMetadata | ExportComplexValue | ocut.SupportedStorage:
        if self._metadata is None:
            if not isinstance(self._value, dict):
                return self._value
            return {'_value': self._value}
        return {
            '_value': self._value,
            '_metadata': self._metadata.to_mapping()
        }

    @staticmethod
    def build(v: t.Any, metadata: DefaultValueDict = None):
        v = SingleElement(v)
        if metadata is not None:
            v.metadata.update(metadata)
        return v


class MultiElement(AbstractElement[list[AbstractElement]]):
    """Represents a set of multiple values."""

    __slots__ = ('_value', '_metadata', '_qc_info')

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
        best_value: None | SingleElement = None
        best_wq: int | None = None
        best_sr: int | None = None
        for idx, v in enumerate(self.all_values()):
            wq = v.quality
            sr = v.sensor_rank
            if sr is None:
                sr = -1 * idx
            replace = False

            if best_wq is None:
                replace = True
            elif wq in ALLOWED_QUALITY_MAP[best_wq] and best_wq != 5:
                replace = True
            elif wq == best_wq:
                if best_sr is None:
                    replace = True
                elif best_sr > -1 and sr > -1:
                    replace = sr < best_sr
                elif best_sr < 0 and sr < 0:
                    replace = sr > best_sr
                elif sr > -1 > best_sr:
                    replace = True
            if replace:
                best_value, best_wq, best_sr = v, wq, sr

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

    def to_mapping(self) -> ExportMultipleWithMetadata:
        if self._metadata is None:
            return {
                '_values': [v.to_mapping() for v in self._value],
            }
        else:
            return {
                '_values': [v.to_mapping() for v in self._value],
                '_metadata': self._metadata.to_mapping()
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

    @staticmethod
    def ensure_element(value: SupportedValueOrElement, metadata: t.Optional[DefaultValueDict] = None, **kwargs: SupportedValueOrElement) -> AbstractElement:
        v = SingleElement(value) if not isinstance(value, AbstractElement) else value
        if metadata:
            v.metadata.update(metadata)
        if kwargs:
            v.metadata.update(kwargs)
        return v


ALLOWED_QUALITY_MAP: dict[int | None, set[int]] = {
    None: {0, 1, 2, 3, 4, 5, 7, 9, -1},
    0: {1, 2, 3, 4, 5, 7, 9, -1},
    1: {2, 3, 4, 5, 7, 9, -1},
    5: {2, 3, 4, 5, 7, 9, -1},
    2: {3, 4, 5, 7, 9, -1},
    3: {4, 5, 7, 9, -1},
    4: {5, 7, 9, -1},
    7: {-1},
    9: {-1},
    -1: set(),
}
