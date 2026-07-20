import enum
import typing as t

from medsutil import ocproc2 as ocproc2, math as amath
from medsutil.awaretime import AwareDateTime
from medsutil.math import ScienceNumber

from medsutil.ocproc2.refs import RecordRef, ChildRecordRef, RecordSetRef
from medsutil.units.structures import UnitError

if t.TYPE_CHECKING:
    from medsutil.iso_duration import ISODuration
    from medsutil.ocproc2.structures import ChildRecord, RecordSet
    from medsutil.ocproc2.elements import SingleElement


def normalize_ocproc_path(path: t.Union[None, str, t.Iterable[str]]) -> str:
    """Normalize the path for a QC result."""
    if path is None:
        return ''
    actual_path = ('/'.join(path) if not isinstance(path, str) else path).strip()
    while '//' in actual_path:
        actual_path = actual_path.replace('//', '/')
    return actual_path.strip('/')


if t.TYPE_CHECKING:
    import decimal
    import datetime
    import medsutil.types as ct
    type _SupportedStorage = None | str | float | int | bool
    type SupportedStorage = _SupportedStorage | list[SupportedStorage] | dict[str, SupportedStorage]
    type _SupportedValue = None | str | float | int | bool | datetime.date | decimal.Decimal
    type SupportedValue = _SupportedValue | t.Iterable[SupportedValue] | t.Mapping[ct.SupportsString, SupportedValue]
    type ObjectWithMetadata = ocproc2.AbstractElement | ocproc2.BaseRecord | ocproc2.RecordSet


class QualityError(Exception): ...


class QualityErrorGroup(ExceptionGroup):

    def __str__(self):
        return f"Quality Errors: {";".join(str(x) for x in self.exceptions)}"


class Quality(enum.IntEnum):

    BAD_STRUCTURE = -1

    UNCHECKED = 0
    GOOD = 1
    PROBABLY_GOOD = 2
    DUBIOUS = 3
    ERRONEOUS = 4
    MODIFIED = 5
    OFF_POSITION = 7

    MISSING = 9

    @staticmethod
    def new_quality_allowed(new_quality: int, old_quality: int | None):
        from medsutil.ocproc2.elements import ALLOWED_QUALITY_MAP
        return new_quality in ALLOWED_QUALITY_MAP[old_quality]


class RequiredQuality(enum.IntFlag):
    NOT_DUBIOUS = enum.auto() # 1
    NOT_MISSING = enum.auto() # 2
    NOT_ERRONEOUS = enum.auto() # 4
    GOOD_STRUCTURE = enum.auto() # 8
    HAS_UNITS = enum.auto() # 16
    HAS_VALUE = enum.auto() # 32
    NOT_FINAL = enum.auto() # 64
    IS_NUMERIC = enum.auto() # 128
    IS_DATETIME = enum.auto() # 256
    IS_INTEGER = enum.auto() # 512
    IS_DURATION = enum.auto() # 1024

    GOOD_OR_DUBIOUS_VALUE = NOT_MISSING | NOT_ERRONEOUS | GOOD_STRUCTURE | HAS_VALUE

    GOOD_VALUE = NOT_DUBIOUS | NOT_MISSING | NOT_ERRONEOUS | GOOD_STRUCTURE | HAS_VALUE
    GOOD_VALUE_WITH_UNITS = NOT_DUBIOUS | NOT_MISSING | NOT_ERRONEOUS | GOOD_STRUCTURE | HAS_VALUE | HAS_UNITS

    GOOD_NUMERIC = GOOD_VALUE | IS_NUMERIC
    GOOD_DATETIME = GOOD_VALUE | IS_DATETIME
    GOOD_INTEGER = GOOD_VALUE | IS_INTEGER
    GOOD_DURATION = GOOD_VALUE | IS_DURATION

    QC_INCOMPLETE = NOT_MISSING | NOT_ERRONEOUS | GOOD_STRUCTURE | NOT_FINAL


class CoordinateTracker:

    def __init__(self):
        self._latitude: amath.AnyNumber | None = None
        self._longitude: amath.AnyNumber | None = None
        self._depth: amath.AnyNumber | None = None
        self._pressure: amath.AnyNumber | None = None
        self._time: AwareDateTime | None = None
        self._time_offset: ISODuration | None = None
        self._central_frequency: amath.AnyNumber | None = None
        self._obs_number: int | None = None
        self._wave_sensor: int | None = None

    @property
    def central_frequency(self) -> amath.AnyNumber | None:
        return self._central_frequency

    @property
    def observation_number(self) -> int | None:
        return self._obs_number

    @property
    def wave_sensor(self) -> int | None:
        return self._wave_sensor

    @property
    def latitude(self) -> amath.AnyNumber | None:
        return self._latitude

    @property
    def longitude(self) -> amath.AnyNumber | None:
        return self._longitude

    @property
    def depth(self) -> amath.AnyNumber | None:
        return self._depth

    @property
    def pressure(self) -> amath.AnyNumber | None:
        return self._pressure

    @property
    def time(self) -> AwareDateTime | None:
        if self._time_offset is not None and self._time is not None:
            return self._time_offset.add_to(self._time)
        return self._time

    def update_from_record(self, record: ocproc2.BaseRecord):
        if "Latitude" in record.coordinates:
            self._latitude = high_quality_numeric(record.coordinates["Latitude"], "degrees_north")
        if "Longitude" in record.coordinates:
            self._longitude = high_quality_numeric(record.coordinates["Longitude"], "degrees_east")
        if "Depth" in record.coordinates or "Pressure" in record.coordinates:
            self._depth, self._pressure = high_quality_depth_pressure(
                record.coordinates.get("Pressure", None),
                record.coordinates.get("Depth", None),
                self._latitude
            )
        if "CentralFrequency" in record.coordinates:
            self._central_frequency = high_quality_numeric(record.coordinates["CentralFrequency"], "Hz")
        if "ObservationNumber" in record.coordinates:
            self._obs_number = high_quality_int(record.coordinates["ObservationNumber"])
        if "WaveSensor" in record.coordinates:
            self._wave_sensor = high_quality_int(record.coordinates["WaveSensor"])
        if "Time" in record.coordinates:
            self._time = high_quality_datetime(record.coordinates["Time"])
        if "TimeOffset" in record.coordinates:
            self._time_offset = high_quality_duration(record.coordinates["TimeOffset"])


def high_quality_depth_pressure(pressure_element: ocproc2.AbstractElement | None = None,
                                depth_element: ocproc2.AbstractElement | None = None,
                                latitude: ocproc2.AbstractElement | amath.AnyNumber | None = None) -> tuple[amath.AnyNumber | None, amath.AnyNumber | None]:
    pressure = high_quality_numeric(pressure_element, "dbar")
    depth = high_quality_numeric(depth_element, "m")
    lat = high_quality_numeric(latitude, "degrees_north") if isinstance(latitude, ocproc2.AbstractElement) else latitude
    if pressure is not None:
        if depth is not None:
            return pressure, depth
        if lat is not None:
            import medsutil.seawater as seawater
            return pressure, seawater.eos80_depth(pressure, lat)
        return pressure, None
    elif depth is not None:
        if lat is not None:
            import medsutil.seawater as seawater
            return seawater.eos80_pressure(depth, lat), depth
        return None, depth
    else:
        return None, None


def high_quality_datetime(v: ocproc2.AbstractElement | None) -> AwareDateTime | None:
    if v is not None and is_of_quality(v, RequiredQuality.GOOD_DATETIME):
        return v.to_datetime()
    return None

def high_quality_duration(v: ocproc2.AbstractElement | None) -> ISODuration | None:
    if v is not None and is_of_quality(v, RequiredQuality.GOOD_DURATION):
        return v.to_duration()
    return None


def high_quality_numeric(v: ocproc2.AbstractElement | None,
                         units: str = None,
                         required_quality: RequiredQuality = RequiredQuality.GOOD_NUMERIC) -> amath.AnyNumber | None:
    if units is not None:
        required_quality = required_quality | RequiredQuality.HAS_UNITS
    if v is not None and is_of_quality(v, required_quality):
        return v.to_numeric(units)
    return None


def high_quality_int(v: ocproc2.AbstractElement | None, units: str = None, required_quality: RequiredQuality = RequiredQuality.GOOD_INTEGER) -> int | None:
    if units is not None:
        required_quality = required_quality | RequiredQuality.HAS_UNITS
    if v is not None and is_of_quality(v, required_quality):
        return v.to_int(units)
    return None


def high_quality_float(v: ocproc2.AbstractElement | None, units: str = None, required_quality: RequiredQuality = RequiredQuality.GOOD_NUMERIC) -> float | None:
    if units is not None:
        required_quality = required_quality | RequiredQuality.HAS_UNITS
    if v is not None and is_of_quality(v, required_quality):
        return v.to_float(units)
    return None


def can_set_working_quality(element: ObjectWithMetadata, working_quality: int) -> bool:
    quality = element.metadata.best("Quality", coerce=int, default=None)
    existing_quality = element.metadata.best("WorkingQuality", coerce=int, default=None)
    if existing_quality is None:
        existing_quality = quality
    return Quality.new_quality_allowed(working_quality, existing_quality)


def set_working_quality(element: ObjectWithMetadata, working_quality: int) -> bool:
    if can_set_working_quality(element, working_quality):
        element.metadata["WorkingQuality"] = working_quality
        return True
    return False


def check_any_of_quality(objs: list[ObjectWithMetadata], required_quality: RequiredQuality) -> bool:
    any_passed: bool = False
    exs = []
    for obj in objs:
        try:
            check_quality(obj, required_quality)
            any_passed = True
        except QualityError as ex:
            exs.append(ex)
    if any_passed:
        return True
    if exs:
        if len(exs) == 1:
            raise exs[0]
        else:
            raise QualityErrorGroup("Multiple errors", exs)
    return False

def is_of_quality(obj: ObjectWithMetadata | None, required_quality: RequiredQuality) -> bool:
    try:
        check_quality(obj, required_quality)
        return True
    except QualityError:
        return False


def check_quality(obj: ObjectWithMetadata | None, required_quality: RequiredQuality):
    if obj is None:
        raise QualityError("element_is_none")

    final_quality = obj.metadata.best("Quality", coerce=int, default=0)
    if RequiredQuality.NOT_FINAL in required_quality and final_quality != Quality.UNCHECKED:
        raise QualityError("element_has_final_quality")

    working_quality = obj.metadata.best("WorkingQuality", coerce=int, default=final_quality)
    if RequiredQuality.NOT_MISSING in required_quality and working_quality == Quality.MISSING:
        raise QualityError("element_is_flagged_empty")
    if RequiredQuality.NOT_ERRONEOUS in required_quality and working_quality == Quality.ERRONEOUS:
        raise QualityError("element_is_flagged_erroneous")
    if RequiredQuality.NOT_DUBIOUS in required_quality and working_quality == Quality.DUBIOUS:
        raise QualityError("element_is_flagged_dubious")

    if RequiredQuality.GOOD_STRUCTURE in required_quality and working_quality == Quality.BAD_STRUCTURE:
        raise QualityError("element_has_bad_structure")

    if isinstance(obj, ocproc2.AbstractElement):
        if RequiredQuality.HAS_VALUE in required_quality and obj.is_empty():
            raise QualityError("element_is_empty")
        if RequiredQuality.HAS_UNITS in required_quality and not obj.metadata.has_value("Units"):
            raise QualityError("element_missing_units")
        if RequiredQuality.IS_INTEGER in required_quality and not obj.is_integer():
            raise QualityError("element_not_integer")
        if RequiredQuality.IS_NUMERIC in required_quality and not obj.is_numeric():
            raise QualityError("element_not_numeric")
        if RequiredQuality.IS_DATETIME in required_quality and not obj.is_iso_datetime():
            raise QualityError("element_not_datetime")
        if RequiredQuality.IS_DURATION in required_quality and not obj.is_duration():
            raise QualityError("element_not_duration")

def pair_lists[T](*lsts: list[T], comparator: t.Callable[[T, T], float | None]) -> t.Iterable[tuple[tuple[T | None, float | None], ...]]:
    used: tuple[set[int], ...] = tuple(
        set() for _ in lsts
    )
    for i in range(0, len(lsts)):
        for idx, item in enumerate(lsts[i]):
            if idx in used[i]:
                continue
            best: list[tuple[T | None, float | None]] = []
            for j in range(0, i):
                best.append((None, None))
            best.append((item, None))
            for j in range(i + 1, len(lsts)):
                best_idx: int | None = None
                best_score: float | None = None
                for k in range(0, len(lsts[j])):
                    if k in used[j]:
                        continue
                    score = comparator(item, lsts[j][k])
                    if score is not None and (best_score is None or best_score < score):
                        best_score = score
                        best_idx = k
                if best_idx is None:
                    best.append((None, None))
                else:
                    best.append((lsts[j][best_idx], best_score))
                    used[j].append(best_idx)
            yield tuple(best)


def dates_overlap(min_a: AwareDateTime,
                  max_a: AwareDateTime,
                  min_b: AwareDateTime,
                  max_b: AwareDateTime) -> bool:
    if min_a > max_b or min_b > max_a:
        return False
    if max_a < min_b or max_b < min_a:
        return False
    return True


def pair_up_single_elements(*element_lists: list[SingleElement]) -> t.Iterable[tuple[tuple[SingleElement | None, float | None], ...]]:
    yield from pair_lists(*element_lists, comparator=compare_elements)

def compare_elements(a: SingleElement, b: SingleElement) -> float | None:
    if a.is_empty():
        if b.is_empty():
            return 1
        else:
            return None
    elif b.is_empty():
        return None
    elif a.is_iso_datetime():
        if b.is_iso_datetime():
            return 1 if dates_overlap(*a.to_scidate().range(), *b.to_scidate().range()) else 0
        else:
            return None
    elif b.is_iso_datetime():
        return None
    elif a.is_science_number() or b.is_science_number():
        if a.is_numeric() and b.is_numeric():
            return compare_parameters(a.to_scinum(), b.to_scinum())
        else:
            return None
    elif a.is_integer():
        if b.is_integer():
            return a.to_int() == b.to_int()
        elif b.is_numeric():
            return compare_numeric(a.to_numeric(), b.to_numeric())
        else:
            return None
    elif a.is_numeric():
        if b.is_numeric():
            return compare_numeric(a.to_numeric(), b.to_numeric())
        else:
            return None
    elif a.is_string_like():
        if b.is_string_like():
            return 1 if a.to_string() == b.to_string() else 0
        else:
            return None
    else:
        return None

def compare_parameters(a: ScienceNumber, b: ScienceNumber) -> float | None:
    try:
        if a.units is not None and b.units is not None and a.units != b.units:
            b = b.convert(a.units)
        if amath.is_close(a.nominal_value, b.nominal_value):
            return 1
        elif a.is_compatible(b, 1):
            return 0.75
        elif a.is_compatible(b, 2):
            return 0.5
        elif a.is_compatible(b, 3):
            return 0.1
        else:
            return None
    except UnitError:
        return None


def compare_numeric(a: float, b: float) -> int:
    if amath.is_close(a, b):
        return 1
    return 0

def pair_up_records[T: RecordRef | ChildRecord](*record_lists: list[T]) -> t.Iterable[tuple[tuple[T | None, float | None], ...]]:
    yield from pair_lists(*record_lists, comparator=compare_child_records)


def compare_child_records(a: ChildRecord | ChildRecordRef, b: ChildRecord | ChildRecordRef) -> float | None:
    from medsutil.ocproc2.structures import ChildRecord
    a_rec = a if isinstance(a, ChildRecord) else a.record
    b_rec = b if isinstance(b, ChildRecord) else b.record

    agreements = None
    total = 0
    for k in set(*a_rec.coordinates.keys(), *b_rec.coordinates.keys()):
        total += 1
        a_coord = a.coordinates.ideal(k)
        b_coord = b.coordinates.ideal(k)
        if a_coord is None or b_coord is None:
            continue
        if agreements is None:
            agreements = 0
        agreements += compare_elements(a_coord, b_coord)
    return agreements


def pair_up_recordsets[T: RecordSetRef | RecordSet](*recordset_lists: list[T]) -> t.Iterable[tuple[tuple[T | None, float | None], ...]]:
    yield from pair_lists(*recordset_lists, comparator=compare_recordsets)


def compare_recordsets(a: RecordSet | RecordSetRef, b: RecordSet | RecordSetRef) -> float | None:
    from medsutil.ocproc2.structures import RecordSet
    rs_a = a if isinstance(a, RecordSet) else a.recordset
    rs_b = b if isinstance(b, RecordSet) else b.recordset
    a_coordinates = set()
    b_coordinates = set()
    a_parameters = set()
    b_parameters = set()

    for record in rs_a.records.iterate_with_load():
        a_coordinates.update(record.coordinates.keys())
        a_parameters.update(record.parameters.keys())
    for record in rs_b.records.iterate_with_load():
        b_coordinates.update(record.coordinates.keys())
        b_parameters.update(record.parameters.keys())
    coordinate_matches = a_coordinates.intersection(b_coordinates)
    parameter_matches = a_parameters.intersection(b_parameters)
    if len(coordinate_matches) == 0 or len(parameter_matches) == 0:
        return None
    len_a = len(rs_a.records)
    len_b = len(rs_b.records)
    max_l = max(len_a, len_b)
    length_factor = (max_l - abs(len_a - len_b)) / max_l
    element_factor = (len(coordinate_matches) + len(parameter_matches)) / len(set(*a_coordinates, *b_coordinates, *a_parameters, *b_parameters))
    return (length_factor + element_factor) / 2.0
