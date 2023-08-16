import datetime
import enum
import functools
import itertools
import typing as t

from cnodc.decode.ocproc2_bin import OCProc2BinaryCodec
from cnodc.ocproc2 import DataRecord

from cnodc.util import haversine_distance_km


class SourceFileStatus(enum.Enum):

    NEW = 'NEW'

    DOWNLOAD_QUEUED = 'DLQ'
    DOWNLOAD_REQUEUED = 'DLR'
    DOWNLOAD_ERROR = 'DLE'
    DOWNLOADED = 'DLC'

    EXTRACT_QUEUED = 'EXQ'
    EXTRACT_ERROR = 'EXE'
    EXTRACTED = 'EXC'

    ERRORED = 'ERR'


class ObservationStatus(enum.Enum):

    NEW = 'NEW'
    UNDER_QC = 'QC'
    ERRORED = 'ERR'
    DUPLICATE = 'DUP'
    INVALID = 'BAD'
    VALID = 'VAL'
    QC_ERROR = 'QCE'


class QualityControlStatus(enum.Enum):

    STARTING = 'ST'
    QUEUED = 'QU'
    IN_PROGRESS = 'IP'
    PASSED = 'PS'
    SKIPPED = "SK"

    RESTARTING = 'RS'
    ERRORED = 'ER'
    MANUAL_REVIEW = 'MR'
    COMPLETE = 'CM'
    QUEUE_ERROR = 'QE'


class _NODBBaseObject:

    def __init__(self, pkey: t.Optional[str], **kwargs):
        self.pkey = str(pkey) if pkey else None
        self._data = {}
        self.modified_values = set()
        for x in kwargs:
            if hasattr(self, x):
                setattr(self, x, kwargs[x])
            else:
                self._data[x] = kwargs[x]
        self.loaded_values = set(x for x in kwargs)
        if self.pkey is not None:
            # Reset modified values if we loaded an original object
            # so we don't update all the values all the time.
            self.modified_values = set()

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)

    def get(self, item):
        if item in self._data:
            return self._data[item]
        raise KeyError(item)

    def __setattr__(self, item, value):
        self.set(item, value)

    def __setitem__(self, item, value):
        self.set(item, value)

    def set(self, item, value, coerce=None):
        if coerce is not None and value is not None:
            value = coerce(value)
        if item not in self._data or self._data[item] != value:
            self._data[item] = value
            self.mark_modified(item)

    def mark_modified(self, item):
        self.modified_values.add(item)

    @classmethod
    def make_property(cls, item, coerce=None):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=coerce)
        )

    @classmethod
    def make_datetime_property(cls, item):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_datetime)
        )

    @classmethod
    def make_enum_property(cls, item, enum_cls: type):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_enum(enum_cls))
        )

    @staticmethod
    def to_enum(enum_cls):
        def _coerce(x):
            if isinstance(x, str):
                return enum_cls(x)
            return x
        return _coerce

    @staticmethod
    def to_datetime(dt):
        if isinstance(dt, str):
            return datetime.datetime.fromisoformat(dt)
        else:
            return dt


class NODBHistory:

    def __init__(self,
                 message: str,
                 source_name: str,
                 source_version: str,
                 timestamp: t.Union[datetime.datetime, str],
                 message_type):
        self.message = message
        self.source_name = source_name
        self.source_version = source_version
        self.timestamp = datetime.datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
        self.message_type = message_type

    def to_dict(self):
        return {
            "m": self.message,
            "n": self.source_name,
            "v": self.source_version,
            "t": self.timestamp.isoformat(),
            "c": self.message_type
        }

    @staticmethod
    def from_dict(d: dict):
        return NODBHistory(
            d["m"],
            d["n"],
            d["v"],
            d["t"],
            d["c"]
        )


class _NODBWithHistoryAndMetadata(_NODBBaseObject):

    history: list[dict] = _NODBBaseObject.make_property("history")
    metadata: dict[str, t.Union[str, bool, None, int, float, list, dict]] = _NODBBaseObject.make_property("metadata")

    def add_history(self, message, source_name, source_version, message_type="INFO"):
        hist = NODBHistory(message, source_name, source_version, datetime.datetime.utcnow(), message_type)
        if self.history is None:
            self.history = []
        self.history.append(hist.to_dict())
        self.mark_modified("history")

    def set_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {}
        self.metadata[key] = value
        self.mark_modified("metadata")

    def get_metadata(self, key, default=None):
        if self.metadata is not None and key in self.metadata:
            return self.metadata[key]
        else:
            return default

    def delete_metadata(self, key):
        if key in self.metadata:
            del self.metadata[key]
        self.mark_modified("metadata")

    def report_error(self, message, source_name, source_version):
        self.add_history(message, source_name, source_version, "ERROR")

    def report_exception(self, ex: Exception, source_name, source_version):
        self.report_error(f"{ex.__class__.__name__}: {str(ex)}", source_name, source_version)


class NODBSourceFile(_NODBWithHistoryAndMetadata):

    origin_name: str = _NODBBaseObject.make_property("origin_name", coerce=str)
    origin_version: str = _NODBBaseObject.make_property("origin_version", coerce=str)
    original_url: str = _NODBBaseObject.make_property("original_url", coerce=str)
    file_name: str = _NODBBaseObject.make_property("file_name", coerce=str)
    correlation_id: str = _NODBBaseObject.make_property("correlation_id", coerce=str)
    internal_url: str = _NODBBaseObject.make_property("internal_url", coerce=str)
    status: SourceFileStatus = _NODBBaseObject.make_enum_property("status", SourceFileStatus)
    qc_name: str = _NODBBaseObject.make_property("qc_name", coerce=str)
    extractor_name: str = _NODBBaseObject.make_property("extractor_name", coerce=str)


class NODBStation(_NODBWithHistoryAndMetadata):

    platform_type: str = _NODBBaseObject.make_property("platform_type", coerce=str)
    platform_wmo_id: str = _NODBBaseObject.make_property("platform_wmo_id", coerce=str)
    platform_wigos_id: str = _NODBBaseObject.make_property("platform_wigos_id", coerce=str)
    station_name: str = _NODBBaseObject.make_property("station_name", coerce=str)
    station_id: str = _NODBBaseObject.make_property("station_id", coerce=str)
    verified: bool = _NODBBaseObject.make_property("verified", coerce=bool)
    map_to_uuid: str = _NODBBaseObject.make_property("map_to_uuid", coerce=bool)
    platform_max_speed: float = _NODBBaseObject.make_property("platform_max_speed", coerce=float)
    search_range_hours: float = _NODBBaseObject.make_property("search_range_hours", coerce=float)


class NODBObservation(_NODBWithHistoryAndMetadata):

    origin_name: str = _NODBBaseObject.make_property("origin_name", coerce=str)
    origin_version: str = _NODBBaseObject.make_property("origin_version", coerce=str)
    origin_batch: str = _NODBBaseObject.make_property("origin_batch", coerce=str)
    source_file_uuid: str = _NODBBaseObject.make_property("source_file_uuid", coerce=str)
    message_idx: int = _NODBBaseObject.make_property("message_idx", coerce=int)
    record_idx: int = _NODBBaseObject.make_property("record_idx", coerce=int)
    program_name: str = _NODBBaseObject.make_property("program_name", coerce=str)
    source_type: str = _NODBBaseObject.make_property("source_type", coerce=str)
    status: ObservationStatus = _NODBBaseObject.make_enum_property("status", ObservationStatus)
    mission_id: str = _NODBBaseObject.make_property("mission_id", coerce=str)
    platform_type: str = _NODBBaseObject.make_property("platform_type", coerce=str)
    obs_time: datetime.datetime = _NODBBaseObject.make_datetime_property("obs_time")
    latitude: float = _NODBBaseObject.make_property("latitude", coerce=float)
    longitude: float = _NODBBaseObject.make_property("longitude", coerce=float)
    min_time: datetime.datetime = _NODBBaseObject.make_datetime_property("min_time")
    max_time: datetime.datetime = _NODBBaseObject.make_datetime_property("max_time")
    min_latitude: float = _NODBBaseObject.make_property("min_latitude", coerce=float)
    min_longitude: float = _NODBBaseObject.make_property("min_longitude", coerce=float)
    max_latitude: float = _NODBBaseObject.make_property("max_latitude", coerce=float)
    max_longitude: float = _NODBBaseObject.make_property("max_longitude", coerce=float)
    min_depth: float = _NODBBaseObject.make_property("min_depth", coerce=float)
    max_depth: float = _NODBBaseObject.make_property("max_depth", coerce=float)
    min_pressure: float = _NODBBaseObject.make_property("min_pressure", coerce=float)
    max_pressure: float = _NODBBaseObject.make_property("max_pressure", coerce=float)
    data_record_type: str = _NODBBaseObject.make_property("data_record_type", coerce=str)
    qc_name: str = _NODBBaseObject.make_property("qc_name", coerce=str)
    qc_current_step: int = _NODBBaseObject.make_property("qc_current_step", coerce=int)
    qc_status: QualityControlStatus = _NODBBaseObject.make_enum_property("qc_status", QualityControlStatus)
    qc_tests_complete: list = _NODBBaseObject.make_property("qc_tests_complete")
    tags: set = _NODBBaseObject.make_property("tags", coerce=set)
    data_record: bytearray = _NODBBaseObject.make_property("data_record")
    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    dedupe_result: str = _NODBBaseObject.make_property("dedupe_result", coerce=str)

    def distance_to(self, nodb_obs):
        if nodb_obs.latitude is None or nodb_obs.longitude is None or self.latitude is None or self.longitude is None:
            return None
        return haversine_distance_km(
            self.latitude,
            nodb_obs.latitude,
            self.longitude,
            nodb_obs.longitude
        )

    def time_diff_seconds(self, nodb_obs):
        if nodb_obs.obs_time is None or self.obs_time is None:
            return None
        return abs((nodb_obs.obs_time - self.obs_time).total_seconds())

    def speed_to(self, nodb_obs):
        # km hr-1
        distance = self.distance_to(nodb_obs)
        if distance is None:
            return None
        time_diff = self.time_diff_seconds(nodb_obs)
        if time_diff is None:
            return None
        if distance == 0:
            return None
        return (3600 * distance) / time_diff

    def qc_test_complete(self, test_name):
        return self.qc_tests_complete and test_name in self.qc_tests_complete

    def mark_test_complete(self, test_name):
        if self.qc_tests_complete is None:
            self.qc_tests_complete = [test_name]
        elif test_name not in self.qc_tests_complete:
            self.qc_tests_complete.append(test_name)
            self.mark_modified("qc_tests_complete")

    def add_qc_error_for_review(self, error_code):
        if not self.metadata:
            self.metadata = {}
        if 'QC_ERRORS' not in self.metadata:
            self.metadata['QC_ERRORS'] = []
        self.metadata['QC_ERRORS'].append(error_code)
        self.mark_modified('metadata')

    def add_tags(self, tags):
        if self.tags is None:
            self.tags = set()
        self.tags.update(tags)
        self.mark_modified("tags")

    def add_tag(self, tag):
        if self.tags is None:
            self.tags = set()
        self.tags.add(tag)
        self.mark_modified("tags")

    def decode_data_record(self) -> t.Union[None, DataRecord]:
        if self.data_record:
            codec = OCProc2BinaryCodec()
            records = [x for x in codec.decode(self.data_record)]
            if records:
                if len(records) > 1:
                    print("ohno")
                return records[0]
        return None

    def import_from_data_record(self, dr: DataRecord):
        self.min_time = None
        self.max_time = None
        self.min_latitude = None
        self.max_latitude = None
        self.min_longitude = None
        self.max_longitude = None
        self.min_depth = None
        self.max_depth = None
        self.min_pressure = None
        self.max_pressure = None
        self._scan_record_for_coordinates(dr)
        subrecord_types = [x for x in set(
            x[:x.rfind('_')] if '_' in x else x for x in dr.subrecords
        ) if x not in ('SENSORS', 'SPEC_WAVE')]
        self.data_record_type = 'SURFACE'
        if subrecord_types:
            self.data_record_type = ';'.join(subrecord_types)
        codec = OCProc2BinaryCodec()
        encoded_record = bytearray()
        for bytes_ in codec.encode(dr):
            encoded_record.extend(bytes_)
        self.data_record = encoded_record
        for mn in ('GTS_HEADER', 'WMO_ID', 'STATION_ID', 'STATION_NAME', 'WIGOS_ID'):
            if mn in dr.metadata:
                self.metadata[mn] = dr.metadata[mn].value()
        self.mark_modified("metadata")
        if 'OBS_PLATFORM_TYPE' in dr.metadata:
            self.platform_type = dr.metadata['OBS_PLATFORM_TYPE'].value()
        if 'LAT' in dr.coordinates:
            self.latitude = dr.coordinates['LAT'].value()
        if 'LON' in dr.coordinates:
            self.longitude = dr.coordinates['LON'].value()
        if 'OBS_TIME' in dr.coordinates:
            obs_time = dr.coordinates['OBS_TIME'].value()
            try:
                self.obs_time = datetime.datetime.fromisoformat(obs_time) if obs_time else None
            except ValueError:
                pass

    def _scan_record_for_coordinates(self, dr: DataRecord):
        if 'LAT' in dr.coordinates:
            lat = dr.coordinates['LAT'].value()
            if lat is not None:
                if self.min_latitude is None or self.min_latitude > lat:
                    self.min_latitude = lat
                if self.max_latitude is None or self.max_latitude < lat:
                    self.max_latitude = lat
        if 'LONGITUDE' in dr.coordinates:
            lon = dr.coordinates['LON'].value()
            if lon is not None:
                if self.min_longitude is None or self.min_longitude > lon:
                    self.min_longitude = lon
                if self.max_longitude is None or self.max_longitude < lon:
                    self.max_longitude = lon
        if 'PRESSURE' in dr.coordinates:
            p = dr.coordinates['PRESSURE'].value()
            if p is not None:
                if self.min_pressure is None or self.min_pressure > p:
                    self.min_pressure = p
                if self.max_pressure is None or self.max_pressure < p:
                    self.max_pressure = p
        if 'DEPTH' in dr.coordinates:
            d = dr.coordinates['DEPTH'].value()
            if d is not None:
                if self.min_depth is None or self.min_depth > d:
                    self.min_depth = d
                if self.max_depth is None or self.max_depth < d:
                    self.max_depth = d
        if 'OBS_TIME' in dr.coordinates:
            dt = dr.coordinates['OBS_TIME'].value()
            if dt is not None:
                try:
                    dt = datetime.datetime.fromisoformat(dt)
                    if self.min_time is None or self.min_time > dt:
                        self.min_time = dt
                    if self.max_time is None or self.max_time < dt:
                        self.max_time = dt
                except ValueError:
                    pass
        self.add_tags(itertools.chain((cn for cn in dr.coordinates), (vn for vn in dr.variables)))
        if dr.subrecords:
            for subrecord_set_name in dr.subrecords:
                for subrecord in dr.subrecords[subrecord_set_name]:
                    self._scan_record_for_coordinates(subrecord)
