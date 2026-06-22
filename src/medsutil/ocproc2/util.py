import enum
import typing as t

from medsutil import ocproc2 as ocproc2, math as amath
from medsutil.awaretime import AwareDateTime

from medsutil.ocproc2.elements import ALLOWED_QUALITY_MAP

if t.TYPE_CHECKING:
    from medsutil.iso_duration import ISODuration


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
        return new_quality in ALLOWED_QUALITY_MAP[old_quality]


class RequiredQuality(enum.IntFlag):
    NOT_DUBIOUS = enum.auto()
    NOT_MISSING = enum.auto()
    NOT_ERRONEOUS = enum.auto()
    GOOD_STRUCTURE = enum.auto()
    HAS_UNITS = enum.auto()
    HAS_VALUE = enum.auto()
    NOT_FINAL = enum.auto()
    IS_NUMERIC = enum.auto()
    IS_DATETIME = enum.auto()
    IS_INTEGER = enum.auto()
    IS_DURATION = enum.auto()

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


def set_working_quality(element: ObjectWithMetadata, working_quality: int):
    quality = element.metadata.best("Quality", coerce=int, default=None)
    existing_quality = element.metadata.best("WorkingQuality", coerce=int, default=None)
    if existing_quality is None:
        element.metadata["WorkingQuality"] = quality
        existing_quality = quality
    if Quality.new_quality_allowed(working_quality, existing_quality):
        element.metadata["WorkingQuality"] = working_quality


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
        raise ExceptionGroup("quality errors", exs)
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
        if RequiredQuality.IS_NUMERIC in required_quality and not obj.is_numeric():
            raise QualityError("element_not_numeric")
        if RequiredQuality.IS_DATETIME in required_quality and not obj.is_iso_datetime():
            raise QualityError("element_not_datetime")
        if RequiredQuality.IS_DURATION in required_quality and not obj.is_duration():
            raise QualityError("element_not_duration")
