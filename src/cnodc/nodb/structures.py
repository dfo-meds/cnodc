import datetime
import enum
import functools
import hashlib
import json
import typing as t
import secrets
import psycopg2.extensions

from cnodc import geodesy
from cnodc.decode.ocproc2_bin import OCProc2BinaryCodec
from cnodc.ocproc2 import DataRecord
from cnodc.ocproc2.structures import NODBQCFlag



class UserStatus(enum.Enum):

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class SourceFileStatus(enum.Enum):

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    ERROR = 'ERROR'
    COMPLETE = 'COMPLETE'


class ObservationStatus(enum.Enum):

    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    RTQC_PASS = 'RTQC_PASS'
    DMQC_PASS = 'DMQC_PASS'
    DISCARDED = 'DISCARDED'


class QualityControlStatus(enum.Enum):

    UNCHECKED = 'UNCHECKED'
    REVIEW = 'REVIEW'
    ERROR = 'ERROR'
    COMPLETE = 'COMPLETE'
    IN_PROGRESS = 'IN_PROGRESS'
    DISCARD = 'DISCARD'


class ObservationWorkingStatus(enum.Enum):

    NEW = 'NEW'
    AUTO_QUEUED = 'AUTO_QUEUED'
    AUTO_IN_PROGRESS = 'AUTO_IN_PROGRESS'
    USER_QUEUED = 'USER_QUEUED'
    USER_IN_PROGRESS = 'USER_IN_PROGRESS'
    USER_CHECKED = 'USER_CHECKED'
    QUEUE_ERROR = 'QUEUE_ERROR'
    ERROR = 'ERROR'


class StationStatus(enum.Enum):

    ACTIVE = 'ACTIVE'
    INCOMPLETE = 'INCOMPLETE'
    INACTIVE = 'INACTIVE'


class QueueStatus(enum.Enum):

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class _NODBBaseObject:

    def __init__(self, *, is_new: bool = True, **kwargs):
        self._data = {}
        self.modified_values = set()
        self._allow_set_readonly = True
        for x in kwargs:
            if hasattr(self, x):
                setattr(self, x, kwargs[x])
            else:
                self._data[x] = kwargs[x]
        self.loaded_values = set(x for x in kwargs)
        if not is_new:
            # Reset modified values if we loaded an original object
            # so we don't update all the values all the time.
            self.modified_values = set()
        self._allow_set_readonly = False

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        if item in self._data:
            return self._data[item]
        return default

    def get_for_db(self, item, default=None):
        retval = default
        if item in self._data:
            retval = self._data[item]
        if isinstance(retval, enum.Enum):
            retval = retval.value
        elif isinstance(retval, (list, tuple, set, dict)):
            return json.dumps(retval)
        return retval

    def __setitem__(self, item, value):
        self.set(item, value)

    def set(self, value, item, coerce=None, readonly: bool = False):
        if readonly and not self._allow_set_readonly:
            raise AttributeError(f"{item} is read-only")
        if coerce is not None and value is not None:
            value = coerce(value)
        if item not in self._data or self._data[item] != value:
            self._data[item] = value
            self.mark_modified(item)

    def mark_modified(self, item):
        self.modified_values.add(item)

    def clear_modified(self):
        self.modified_values.clear()

    @classmethod
    def make_property(cls, item, coerce=None, readonly: bool = False):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=coerce, readonly=readonly)
        )

    @classmethod
    def make_datetime_property(cls, item, readonly: bool = False):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_datetime, readonly=readonly)
        )

    @classmethod
    def make_date_property(cls, item, readonly: bool = False):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_date, readonly=readonly)
        )

    @classmethod
    def make_enum_property(cls, item, enum_cls: type, readonly: bool = False):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_enum(enum_cls), readonly=readonly)
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

    @staticmethod
    def to_date(dt):
        if isinstance(dt, str):
            return datetime.date.fromisoformat(dt)
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


class NODBQueueItem(_NODBBaseObject):

    queue_uuid: str = _NODBBaseObject.make_property("queue_uuid", coerce=str)
    created_date: datetime.datetime = _NODBBaseObject.make_datetime_property("created_date", readonly=True)
    modified_date: datetime.datetime = _NODBBaseObject.make_datetime_property("modified_date", readonly=True)
    status: QueueStatus = _NODBBaseObject.make_enum_property("status", QueueStatus, readonly=True)
    locked_by: t.Optional[str] = _NODBBaseObject.make_property("locked_by", coerce=str, readonly=True)
    locked_since: t.Optional[datetime.datetime] = _NODBBaseObject.make_datetime_property("locked_since", readonly=True)
    queue_name: str = _NODBBaseObject.make_property("queue_name", readonly=True, coerce=str)
    unique_item_name: t.Optional[str] = _NODBBaseObject.make_property("unique_item_name", readonly=True, coerce=str)
    priority: t.Optional[int] = _NODBBaseObject.make_property("priority", readonly=True, coerce=int)
    data: dict = _NODBBaseObject.make_property("data", readonly=True)


class NODBSourceFile(_NODBWithMetadata, _NODBBaseObject):

    source_uuid: str = _NODBBaseObject.make_property("source_uuid", coerce=str)
    partition_key: datetime.date = _NODBBaseObject.make_date_property("partition_key")

    source_path: str = _NODBBaseObject.make_property("source_path", coerce=str)
    persistent_path: str = _NODBBaseObject.make_property("persistent_path", coerce=str)
    file_name: str = _NODBBaseObject.make_property("file_name", coerce=str)

    original_uuid: str = _NODBBaseObject.make_property("original_uuid", coerce=str)
    original_idx: int = _NODBBaseObject.make_property("original_idx", coerce=int)

    status: SourceFileStatus = _NODBBaseObject.make_enum_property("status", SourceFileStatus)

    history: list = _NODBBaseObject.make_property("history")

    qc_workflow_name: str = _NODBBaseObject.make_property("qc_workflow_name", coerce=str)

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


class NODBUser(_NODBBaseObject):

    username: str = _NODBBaseObject.make_property("username", coerce=str)
    phash: bytes = _NODBBaseObject.make_property("phash")
    salt: bytes = _NODBBaseObject.make_property("salt")
    status: UserStatus = _NODBBaseObject.make_enum_property("status", UserStatus)
    roles: list = _NODBBaseObject.make_property("roles")

    def assign_role(self, role_name):
        if self.roles is None:
            self.roles = [role_name]
            self.modified_values.add('roles')
        elif role_name not in self.roles:
            self.roles.append(role_name)
            self.modified_values.add('roles')

    def unassign_role(self, role_name):
        if self.roles is not None and role_name in self.roles:
            self.roles.remove(role_name)
            self.modified_values.add('roles')

    def set_password(self, new_password, salt_length: int = 16):
        self.salt = secrets.token_bytes(salt_length)
        self.phash = NODBUser.hash_password(new_password, self.salt)

    def check_password(self, password):
        check_hash = NODBUser.hash_password(password, self.salt)
        return secrets.compare_digest(check_hash, self.phash)

    @staticmethod
    def hash_password(password: str, salt: bytes, iterations=752123) -> bytes:
        return hashlib.pbkdf2_hmac('sha512', password.encode('utf-8', errors="replace"), salt, iterations)


class NODBSession(_NODBBaseObject):

    session_id: str = _NODBBaseObject.make_property("session_id", coerce=str)
    start_time: datetime = _NODBBaseObject.make_datetime_property("start_time")
    expiry_time: datetime = _NODBBaseObject.make_datetime_property("expiry_time")
    username: str = _NODBBaseObject.make_property("username", coerce=str)
    session_data: dict = _NODBBaseObject.make_property("session_data")

    def set_session_value(self, key, value):
        if self.session_data is None:
            self.session_data = {}
        self.session_data[key] = value

    def get_session_value(self, key, default=None):
        return self.session_data[key] if self.session_data and key in self.session_data else default























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

    data_record: t.Union[bytes, bytearray] = _NODBBaseObject.make_property("data_record")
    _data_record_cache: t.Optional[DataRecord] = None

    def extract_data_record(self) -> DataRecord:
        if self._data_record_cache is None:
            if self.data_record is not None:
                codec = OCProc2BinaryCodec()
                self._data_record_cache = codec.decode([self.data_record])
        return self._data_record_cache

    def store_data_record(self, dr: DataRecord, **kwargs):
        self._data_record_cache = dr
        codec = OCProc2BinaryCodec()
        new_data = bytearray()
        for bytes_ in codec.encode(dr, **kwargs):
            new_data.extend(bytes_)
        self.data_record = new_data

    def clear_data_record_cache(self):
        self._data_record_cache = None





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

    def has_rt_qc(self) -> bool:
        return bool(self.rt_qc_steps)


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
        working_obs._data_record_cache = primary_obs._data_record_cache
        working_obs.station_uuid = primary_obs.station_uuid
        working_obs.metadata = primary_obs.metadata
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

    def update_from_working(self, working_record: NODBWorkingObservation):
        self.station_uuid = working_record.station_uuid
        self.metadata = working_record.metadata
        self.data_record = working_record.data_record
        self.data_record_cache = working_record._data_record_cache
        self.update_from_data_record(working_record.extract_data_record())

    def update_from_data_record(self, data_record: DataRecord):
        search_data_agg = _SearchDataAggregator(data_record)
        self.latitude = search_data_agg.statistics['_rpr_lat']
        self.longitude = search_data_agg.statistics['_rpr_lon']
        self.obs_time = search_data_agg.statistics['_rpr_time']
        # Underscore attributes are for other purposes
        self.search_data = {
            x: search_data_agg.statistics[x]
            for x in search_data_agg.statistics
            if x[0] != '_'
        }


def searchable_time(t: str):
    if t is None:
        return None
    # TODO: ensure UTC
    dt = datetime.datetime.fromisoformat(t)
    return int(dt.strftime("%Y%m%d%H%M%S"))


class _SearchDataAggregator:

    def __init__(self, data_record: DataRecord):
        self.statistics: dict[str, t.Union[str, set, list, float, int, None]] = {
            'record_type': self._detect_geometry(data_record),
            '_rpr_lat': None,
            '_rpr_lon': None,
            '_rpr_time': None
        }
        self._internals = {
            'coordinates': [],
            'time': []
        }
        self._consider_record(data_record)
        if (data_record.coordinates.has_value('LAT')
                and data_record.coordinates.has_value('LON')
                and data_record.coordinates['LAT'].nodb_flag != NODBQCFlag.BAD
                and data_record.coordinates['LON'].value != NODBQCFlag.BAD):
            self.statistics['_rpr_lat'] = data_record.coordinates.get_value('LAT')
            self.statistics['_rpr_lon'] = data_record.coordinates.get_value('LON')
        if data_record.coordinates.has_value('TIME') and data_record.coordinates['TIME'].nodb_flag != NODBQCFlag.BAD:
            self.statistics['_rpr_time'] = data_record.coordinates['TIME'].as_datetime()
        if self.statistics['_rpr_lat'] is None or self.statistics['_rpr_lon'] is None or self.statistics['_rpr_time'] is None:
            rlat, rlon, rtime = self._estimate_best_position()
            if self.statistics['_rpr_lat'] is None:
                self.statistics['_rpr_lat'] = rlat
            if self.statistics['_rpr_lon'] is None:
                self.statistics['_rpr_lon'] = rlon
            if self.statistics['_rpr_time'] is None:
                self.statistics['_rpr_time'] = rtime
        for x in self.statistics:
            if isinstance(self.statistics[x], set):
                self.statistics[x] = list(self.statistics[x])

    def _estimate_best_position(self) -> tuple[t.Optional[float], t.Optional[float], t.Optional[float]]:
        # No entries, no position
        if not self._internals['coordinates']:
            return None, None, self._estimate_best_time()
        # One entry, one position
        if len(self._internals['coordinates']) == 1:
            return self._internals['coordinates'][0][0], self._internals['coordinates'][0][1], self._estimate_best_time()
        # Use a geodetic mean of the coordinates. This is slow but coordinates is usually going to be small in size
        mean = geodesy.mean_vector(self._internals['coordinates'])
        return mean[0], mean[1], self._estimate_best_time()

    def _estimate_best_time(self) -> t.Optional[float]:
        # No entries, no time
        if not self._internals['time']:
            return None
        # One entry, one time
        if len(self._internals['time']) == 1:
            return self._internals['time'][0]
        # Find the first time
        min_time = min(x for x in self._internals['time'])
        # Calculate the average number of seconds since the earliest time for all records
        total_diff = 0
        entries = 0
        for x in self._internals['time']:
            total_diff += (x - min_time).total_seconds()
            entries += 1
        # Calculate an average
        return min_time + datetime.timedelta(seconds=(total_diff / entries))

    def _consider_record(self, record: DataRecord):
        lat = self._add_coordinate_statistics(record, 'LAT')
        lon = self._add_coordinate_statistics(record, 'LON')
        if lat is not None and lon is not None:
            self._internals['coordinates'].append((lon, lat))
        self._add_coordinate_statistics(record, 'DEPTH')
        self._add_coordinate_statistics(record, 'PRESSURE')
        time = self._add_coordinate_statistics(record, 'TIME', coerce=searchable_time)
        if time is not None:
            self._internals['time'].append(datetime.datetime.strptime(str(time), "%Y%m%d%H%M%S"))
        if 'vars' not in self.statistics:
            self.statistics['vars'] = set()
        self.statistics['vars'].update(vname for vname in record.variables)
        self.statistics['vars'].update(vname for vname in record.coordinates)
        if record.subrecords:
            if 'child_types' not in self.statistics:
                self.statistics['child_types'] = set()
            for srs_name in record.subrecords:
                # Omit last component since it is the index
                self.statistics['child_types'].add(srs_name[:srs_name.rfind("_")])
                for sr in record.subrecords[srs_name]:
                    self._consider_record(sr)

    def _detect_geometry(self, parent_record: DataRecord):

        # Level 1
        parent_coords = set(x for x in parent_record.coordinates if parent_record.coordinates[x].value() is not None)

        # Level 2
        subrecord_types = parent_record.subrecords.record_types()

        # Level 3
        subsubrecord_types = set()
        for srt in parent_record.subrecords:
            for record in parent_record.subrecords[srt]:
                subsubrecord_types.update(record.subrecords.record_types())

        # X, Y, T specified (point or profile)
        if 'LAT' in parent_coords and 'LON' in parent_coords and 'TIME' in parent_coords:

            # Profiles
            if 'PROFILE' in subrecord_types:
                return 'profile'

            # At-depth observation
            elif 'DEPTH' in parent_coords or 'PRESSURE' in parent_coords:
                return 'at_depth'

            # Surface observation
            elif 'TSERIES' not in subrecord_types and 'TRAJ' not in subrecord_types:
                return 'surface'

        # No TIME specified, but has TIMESERIES means it is a time series at a fixed station
        elif 'LAT' in parent_coords and 'LON' in parent_coords and 'TSERIES' in subrecord_types:
            return 'time_series_profile' if 'PROFILE' in subsubrecord_types else 'time_series'

        # No LAT, LON, or TIME means a trajectory
        elif 'LAT' not in parent_coords and 'LON' not in parent_coords and 'TIME' not in parent_coords and 'TRAJ' in subrecord_types:
            return 'trajectory_profile' if 'PROFILE' in subsubrecord_types else 'trajectory'

        return 'other'

    def _add_coordinate_statistics(self, record: DataRecord, coordinate_name: str, coerce: callable = None):
        if record.coordinates.has_value(coordinate_name):
            coord = record.coordinates[coordinate_name]
            if coord.nodb_flag == NODBQCFlag.BAD:
                return None
            key = coordinate_name.lower()
            val = coerce(coord.value()) if coerce is not None else coord.value()
            if f"min_{key}" not in self.statistics or self.statistics[f"min_{key}"] > val:
                self.statistics[f"min_{key}"] = val
            if f"max_{key}" not in self.statistics or self.statistics[f"max_{key}"] < val:
                self.statistics[f"max_{key}"] = val
            return val
        return None



