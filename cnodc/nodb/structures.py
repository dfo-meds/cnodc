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
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    ERROR = 'ERROR'
    QUEUE_ERROR = 'QUEUE_ERROR'
    COMPLETE = 'COMPLETE'


class ObservationStatus(enum.Enum):

    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    RTQC_PASS = 'RTQC_PASS'
    DMQC_PASS = 'DMQC_PASS'
    DISCARDED = 'DISCARDED'


class QualityControlStatus(enum.Enum):

    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    MANUAL_REVIEW = 'MANUAL_REVIEW'
    ERROR = 'ERROR'
    PASSED = 'PASSED'
    BATCH = 'BATCH'


class ObservationWorkingStatus(enum.Enum):

    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    DISCARDED = 'DISCARDED'
    BATCH = 'BATCH'
    ERROR = 'ERROR'
    QUEUE_ERROR = 'QUEUE_ERROR'


class StationStatus(enum.Enum):

    ACTIVE = 'ACTIVE'
    INCOMPLETE = 'INCOMPLETE'
    INACTIVE = 'INACTIVE'


class _NODBBaseObject:

    def __init__(self, pkey: t.Optional[str] = None, **kwargs):
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

    def get(self, item):
        if item in self._data:
            return self._data[item]
        raise KeyError(item)

    def __setitem__(self, item, value):
        self.set(item, value)

    def set(self, value, item, coerce=None):
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


class _NODBWithMetadata:

    metadata: t.Optional[dict] = _NODBBaseObject.make_property("metadata")

    def set_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {key: value}
            self.modified_values.add("metadata")
        else:
            self.metadata[key] = value
            self.modified_values.add("metadata")

    def clear_metadata(self, key):
        if self.metadata is None:
            return
        if key not in self.metadata:
            return
        del self.metadata[key]
        self.modified_values.add("metadata")
        if not self.metadata:
            self.metadata = None

    def get_metadata(self, key, default=None):
        if self.metadata is None or key not in self.metadata:
            return default
        return self.metadata[key]

    def add_to_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {key: [value]}
            self.modified_values.add("metadata")
        elif key not in self.metadata:
            self.metadata[key] = [value]
            self.modified_values.add("metadata")
        elif value not in self.metadata[key]:
            self.metadata[key].append(value)
            self.modified_values.add("metadata")


class _NODBWithQCMetdata(_NODBWithMetadata):

    qc_metadata: t.Optional[dict] = _NODBBaseObject.make_property("qc_metadata")

    def set_qc_metadata(self, key, value):
        if self.qc_metadata is None:
            self.qc_metadata = {key: value}
            self.modified_values.add("qc_metadata")
        else:
            self.qc_metadata[key] = value
            self.modified_values.add("qc_metadata")

    def clear_qc_metadata(self, key):
        if self.qc_metadata is None:
            return
        if key not in self.qc_metadata:
            return
        del self.qc_metadata[key]
        self.modified_values.add("qc_metadata")
        if not self.qc_metadata:
            self.qc_metadata = None

    def get_qc_metadata(self, key, default=None):
        if self.qc_metadata is None or key not in self.qc_metadata:
            return default
        return self.qc_metadata[key]

    def add_to_qc_metadata(self, key, value):
        if self.qc_metadata is None:
            self.qc_metadata = {key: [value]}
            self.modified_values.add("qc_metadata")
        elif key not in self.qc_metadata:
            self.qc_metadata[key] = [value]
            self.modified_values.add("qc_metadata")
        elif value not in self.qc_metadata[key]:
            self.qc_metadata[key].append(value)
            self.modified_values.add("qc_metadata")

    def apply_qc_code(self, qc_message_code):
        self.add_to_qc_metadata("qc_codes", qc_message_code)

    def has_qc_code(self, qc_message_code):
        return self.qc_metadata and 'qc_codes' in self.qc_metadata and qc_message_code in self.qc_metadata['qc_codes']

    def has_any_qc_code(self):
        return self.qc_metadata and 'qc_codes' in self.qc_metadata and self.qc_metadata['qc_codes']

    def clear_qc_codes(self):
        if self.qc_metadata and 'qc_codes' in self.qc_metadata:
            del self.qc_metadata['qc_codes']


class _NODBWithQCProperties(_NODBWithQCMetdata):

    qc_test_status: QualityControlStatus = _NODBBaseObject.make_enum_property("qc_test_status", QualityControlStatus)
    qc_process_name: str = _NODBBaseObject.make_property("qc_process_name", coerce=str)
    qc_current_step: int = _NODBBaseObject.make_property("qc_current_step", coerce=int)
    working_status: ObservationWorkingStatus = _NODBBaseObject.make_enum_property("working_status", ObservationWorkingStatus)


class _NODBWithDataRecord:

    data_record: bytes = _NODBBaseObject.make_property("data_record")
    data_record_cache: DataRecord = None

    def extract_data_record(self) -> DataRecord:
        pass

    def store_data_record(self, dr: DataRecord):
        pass


class NODBSourceFile(_NODBWithMetadata, _NODBBaseObject):

    source_path: str = _NODBBaseObject.make_property("source_path", coerce=str)
    persistent_path: str = _NODBBaseObject.make_property("persistent_path", coerce=str)
    file_name: str = _NODBBaseObject.make_property("file_name", coerce=str)

    original_uuid: str = _NODBBaseObject.make_property("original_uuid", coerce=str)
    original_idx: int = _NODBBaseObject.make_property("original_idx", coerce=int)

    status: SourceFileStatus = _NODBBaseObject.make_enum_property("status", SourceFileStatus)

    history: list = _NODBBaseObject.make_property("history")

    def report_error(self, message, name, version, instance):
        self.add_history(message, name, version, instance, 'ERROR')

    def report_warning(self, message, name, version, instance):
        self.add_history(message, name, version, instance, 'WARNING')

    def add_history(self, message, name, version, instance, level='INFO'):
        if self.history is None:
            self.history = []
        self.history.append({
            'msg': message,
            'src': name,
            'ver': version,
            'ins': instance,
            'lvl': level,
            'asc': datetime.datetime.utcnow().isoformat()
        })
        self.modified_values.add('history')


class NODBStation(_NODBWithMetadata, _NODBBaseObject):

    station_type_name: str = _NODBBaseObject.make_property("station_type_name", coerce=str)
    wmo_id: str = _NODBBaseObject.make_property("wmo_id", coerce=str)
    wigos_id: str = _NODBBaseObject.make_property("wigos_id", coerce=str)
    station_name: str = _NODBBaseObject.make_property("station_name", coerce=str)
    station_id: str = _NODBBaseObject.make_property("station_id", coerce=str)
    map_to_uuid: str = _NODBBaseObject.make_property("map_to_uuid", coerce=str)
    status: StationStatus = _NODBBaseObject.make_enum_property("status", StationStatus)


class NODBQCBatch(_NODBWithQCProperties, _NODBBaseObject):
    pass


class NODBQCProcess(_NODBBaseObject):

    version_no: int = _NODBBaseObject.make_property("version_no", coerce=int)
    rt_qc_steps: list = _NODBBaseObject.make_property("rt_qc_steps")
    dm_qc_steps: list = _NODBBaseObject.make_property("dm_qc_steps")
    dm_qc_freq_days: int = _NODBBaseObject.make_property("dm_qc_freq_days", coerce=int)
    dm_qc_delay_days: int = _NODBBaseObject.make_property("dm_qc_delay_days", coerce=int)


class NODBWorkingObservation(_NODBWithDataRecord, _NODBWithQCProperties, _NODBBaseObject):

    qc_batch_id: str = _NODBBaseObject.make_property("qc_batch_id", coerce=str)

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)

    def mark_qc_test_complete(self, qc_test_name):
        self.add_to_metadata("qc_tests", qc_test_name)

    def qc_test_completed(self, qc_test_name):
        return self.metadata and "qc_tests" in self.metadata and qc_test_name in self.metadata["qc_tests"]

    @staticmethod
    def create_from_primary(primary_obs):
        working_obs = NODBWorkingObservation(primary_obs.pkey)
        working_obs.data_record = primary_obs.data_record
        working_obs.data_record_cache = primary_obs.data_record_cache
        working_obs.station_uuid = primary_obs.station_uuid
        return working_obs


class NODBObservation(_NODBWithDataRecord, _NODBWithMetadata, _NODBBaseObject):

    source_file_uuid: str = _NODBBaseObject.make_property("source_file_uuid", coerce=str)
    message_idx: int = _NODBBaseObject.make_property("message_idx", coerce=int)
    record_idx: int = _NODBBaseObject.make_property("record_idx", coerce=int)

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    mission_name: str = _NODBBaseObject.make_property("mission_name", coerce=str)

    obs_time: t.Optional[datetime.datetime] = _NODBBaseObject.make_datetime_property("obs_time")
    latitude: t.Optional[float] = _NODBBaseObject.make_property("latitude", coerce=float)
    longitude: t.Optional[float] = _NODBBaseObject.make_property("longitude", coerce=float)

    status: ObservationStatus = _NODBBaseObject.make_enum_property("status", ObservationStatus)
    duplicate_uuid: str = _NODBBaseObject.make_property("duplicate_uuid", coerce=str)

    search_data: dict = _NODBBaseObject.make_property("search_data")
