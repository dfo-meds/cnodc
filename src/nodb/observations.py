import datetime
import enum
import typing as t

import medsutil.ocproc2 as ocproc2
import nodb.base as s
import medsutil.types as ct
import nodb.interface as interface
from medsutil.math import is_science_number, ScienceNumber
from medsutil.ocproc2 import AbstractElement
from medsutil.ocproc2.codecs.ocproc2bin import OCProc2BinCodec
from medsutil.awaretime import AwareDateTime
from medsutil.sanitize import coerce


class SourceFileStatus(enum.Enum):
    """Status of a source file in the database."""

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    ERROR = 'ERROR'
    COMPLETE = 'COMPLETE'


class ObservationStatus(enum.Enum):
    """Status of an archived observation in the database."""

    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    DISCARDED = 'DISCARDED'
    DUPLICATE = 'DUPLICATE'
    ARCHIVED = 'ARCHIVED'
    DUBIOUS = 'DUBIOUS'


class ObservationType(enum.Enum):
    """Type of observation (i.e. profile vs. surface vs. measurement at depth)."""

    SURFACE = 'SURFACE'
    AT_DEPTH = 'AT_DEPTH'
    PROFILE = 'PROFILE'
    OTHER = 'OTHER'


class BatchStatus(enum.Enum):
    """Status of a batch in the database."""

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    MANUAL_REVIEW = 'MANUAL_REVIEW'
    COMPLETE = 'COMPLETE'
    ERRORED = 'ERRORED'


class PlatformStatus(enum.Enum):
    """Status of a platform in the database."""

    ACTIVE = 'ACTIVE'
    INCOMPLETE = 'INCOMPLETE'
    HISTORICAL = 'HISTORICAL'
    REMOVED = 'REMOVED'
    REPLACED = 'REPLACED'


class DataMode(enum.Enum):
    REAL_TIME = "RT"
    DELAYED_MODE = "DM"
    UNKNOWN = "??"

    @staticmethod
    def is_better_than(b: DataMode, a: DataMode) -> bool:
        match a, b:
            case DataMode.REAL_TIME, DataMode.DELAYED_MODE:
                return True
            case DataMode.UNKNOWN, DataMode.DELAYED_MODE:
                return True
            case DataMode.UNKNOWN, DataMode.REAL_TIME:
                return True
            case _:
                return False



class QualityCheckFlags(enum.IntFlag):
    DEDUPLICATE = 1
    GTSPP = 2


class ObservationRelationshipType(enum.Enum):

    # A (relationship types) B
    IS_DUPLICATE = 'is_duplicate_of'
    IS_BETTER = 'is_better_than'
    IS_CORRECTION = "is_correction_to"
    IS_BROADCAST = "is_broadcast_of"
    IS_SUPPLEMENTAL = "is_supplemental_to"
    IS_MERGE = "was_merged_from"


class SubrecordInfo:

    POSITION_COMMON_UNITS = {
        "Latitude": "degrees_north",
        "Longitude": "degrees_east",
        "Depth": "m",
        "Pressure": "dbar",
    }

    def __init__(self):
        self.min_latitude: float | None = None
        self.max_latitude: float | None = None
        self.min_longitude: float | None = None
        self.max_longitude: float | None = None
        self.min_depth: float | None = None
        self.max_depth: float | None = None
        self.min_time: AwareDateTime | None = None
        self.max_time: AwareDateTime | None = None
        self.depths: list[float] = []
        self.profile_parameters: set[str] = set()
        self.surface_parameters: set[str] = set()
        self.instruments: set[str] = set()

    def build_info(self, record: ocproc2.BaseRecord):
        self._extract_subrecord_info(record, {})

    @property
    def time(self) -> AwareDateTime | None:
        if self.min_time is None and self.max_time is None:
            return None
        elif self.min_time is None:
            return self.max_time
        elif self.max_time is None:
            return self.min_time
        else:
            diff = (self.max_time - self.min_time).total_seconds()
            return self.min_time + datetime.timedelta(seconds=diff)

    @property
    def obs_type(self) -> ObservationType:
        if self.min_latitude is None or self.min_longitude is None or self.min_time is None:
            return ObservationType.OTHER
        if not self.depths:
            return ObservationType.SURFACE
        max_depth = max(self.depths)
        if max_depth <= 0:
            return ObservationType.SURFACE
        elif len(self.depths) == 1:
            return ObservationType.AT_DEPTH
        else:
            return ObservationType.PROFILE

    @property
    def wkt(self) -> str | None:
        if self.min_latitude is not None and self.max_latitude is not None and self.min_longitude is not None and self.max_longitude is not None:
            if self.min_latitude == self.max_latitude and self.min_longitude == self.max_longitude:
                return f"POINT ({self.min_longitude:.6f} {self.min_latitude:.6f})"
            elif self.min_latitude == self.max_latitude or self.min_longitude == self.max_longitude:
                return f"LINESTRING ({self.min_longitude:.6f} {self.min_latitude:.6f}, {self.max_longitude:.6f} {self.max_latitude:.6f})"
            else:
                coords = [
                    (self.min_longitude, self.min_latitude),
                    (self.max_longitude, self.min_latitude),
                    (self.max_longitude, self.max_latitude),
                    (self.min_longitude, self.max_latitude),
                    (self.min_longitude, self.min_latitude),
                ]
                return f"POLYGON(({",".join(f"{x:.6f} {y:.6f}" for x, y in (coords))}))"
        return None

    def _extract_subrecord_info(self, record: ocproc2.BaseRecord, position: dict[str, ScienceNumber]):
        for key in record.coordinates:
            for sv in record.coordinates[key].all_values():
                if sv.metadata.has_value('SensorType'):
                    self.instruments.add(sv.metadata["SensorType"].to_string())
        for key in record.parameters:
            for sv in record.parameters[key].all_values():
                if sv.metadata.has_value('SensorType'):
                    self.instruments.add(sv.metadata["SensorType"].to_string())

        position = {x: position[x] for x in position if position[x] is not None}
        for key, units in self.POSITION_COMMON_UNITS.items():
            if record.coordinates.has_value(key) and record.coordinates[key].is_numeric():
                position[key] = record.coordinates[key].to_scinum().convert(units)

        if record.coordinates.has_value("Time") and record.coordinates["Time"].is_iso_datetime():
            time = record.coordinates["Time"].to_scidate()
            min_t, max_t = time.range()
            if self.min_time is None or self.min_time > min_t:
                self.min_time = min_t
            if self.max_time is None or self.max_time < min_t:
                self.max_time = max_t

        if record.coordinates.has_value("Latitude") and record.coordinates.has_value("Longitude") and position["Latitude"] and position["Longitude"]:
            min_lat, max_lat = position["Latitude"].range()
            min_lon, max_lon = position["Longitude"].range()
            if self.min_latitude is None or self.min_latitude > min_lat:
                self.min_latitude = coerce.as_float(min_lat)
            if self.max_latitude is None or self.max_latitude < max_lat:
                self.max_latitude = coerce.as_float(max_lat)
            if self.min_longitude is None or self.min_longitude > max_lon:
                self.min_longitude = coerce.as_float(min_lon)
            if self.max_longitude is None or self.max_longitude < min_lon:
                self.max_longitude = coerce.as_float(max_lon)

        if record.coordinates.has_value("Depth") or record.coordinates.has_value("Pressure"):
            depth = None
            if 'Depth' in position and position["Depth"]:
                depth = position['Depth']
            elif 'Pressure' in position and position['Pressure'] and 'Latitude' in position:
                from medsutil.seawater import eos80_depth
                depth = eos80_depth(position['Pressure'], position['Latitude'])
            if depth is not None:
                min_d, max_d = depth.range() if is_science_number(depth) else (depth, depth)
                if self.min_depth is None or min_d < self.min_depth:
                    self.min_depth = coerce.as_float(min_d)
                if self.max_depth is None or self.max_depth < max_d:
                    self.max_depth = coerce.as_float(max_d)
                self.depths.append(coerce.as_float(min_d))

        is_surface = True
        if "Depth" in position:
            is_surface = position["Depth"] > 0
        elif "Pressure" in position:
            is_surface = position["Pressure"] > 0

        if is_surface:
            self.surface_parameters.update(x for x in record.parameters.keys())
        else:
            self.profile_parameters.update(x for x in record.parameters.keys())

        for subrecord in record.iter_subrecords():
            self._extract_subrecord_info(subrecord, position)


class _RecordMixin(s.NODBBaseObject):

    data_record: t.Optional[bytes] = s.ByteArrayColumn()

    @property
    def record(self) -> t.Optional[ocproc2.ParentRecord]:
        """Extract the data record."""
        return self._with_cache('loaded_record', self._record)

    def _record(self):
        if self.data_record is None:
            return None
        decoder = OCProc2BinCodec()
        records = [x for x in decoder.load(self.data_record)]
        return records[0] if records else None

    @record.setter
    def record(self, data_record: ocproc2.ParentRecord):
        """Set the data record."""
        self._set_cache('loaded_record', data_record)
        if data_record is None:
            self.data_record = None
        else:
            self._update_from_data_record(data_record)
            decoder = OCProc2BinCodec()
            ba = bytearray()
            for byte_ in decoder.encode_records(
                    [data_record],
                    codec='JSON',
                    compression='LZMA2CRC4',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba

    def _update_from_data_record(self, data_record: ocproc2.ParentRecord):
        update_common_from_data_record(self, data_record)


def update_common_from_data_record(obj, data_record: ocproc2.ParentRecord):
    info = SubrecordInfo()
    info.build_info(data_record)
    
    # Time
    if hasattr(obj, 'obs_time'):
        obj.obs_time = info.time
        
    # Location
    if hasattr(obj, 'location'):
        obj.location = info.wkt

    # Parameters collected
    if hasattr(obj, "profile_parameters"):
        obj.profile_parameters = info.profile_parameters
    if hasattr(obj, "surface_parameters"):
        obj.surface_parameters = info.surface_parameters

    # Instrument types used in data collection
    if hasattr(obj, "instrument_types"):
        obj.instrument_types = info.instruments

    # Depth range
    if hasattr(obj, "min_depth"):
        obj.min_depth = info.min_depth
    if hasattr(obj, "max_depth"):
        obj.max_depth = info.max_depth

    # Time range
    if hasattr(obj, "min_time"):
        obj.min_time = info.min_time
    if hasattr(obj, "max_time"):
        obj.max_time = info.max_time

    # Observation type
    if hasattr(obj, "observation_type"):
        obj.observation_type = info.obs_type

    # Platform Identifier
    if hasattr(obj, 'platform_uuid') and data_record.metadata.has_value('CNODCPlatform'):
        obj.platform_uuid = data_record.metadata.best('CNODCPlatform', None)
        
    # Observation Identifier
    if hasattr(obj, 'observation_identifier') and data_record.metadata.has_value('CNODCObservationID'):
        obj.observation_identifier = data_record.metadata.best('CNODCObservationID', default=None, coerce=str)
        
    # Data Mode
    if hasattr(obj, "data_mode") and data_record.metadata.has_value("CNODCDataMode"):
        obj.data_mode = data_record.metadata.best("CNODCDataMode", default=None, coerce=str)

    # Quality Flags (these indicate if broad QC tests has been run
    if hasattr(obj, "quality_checks") and data_record.metadata.has_value("CNODCQualityFlags"):
        obj.quality_checks = data_record.metadata.best("CNODCQualityFlags", default=None, coerce=int)

    # Mission identifier
    if hasattr(obj, "mission_uuid"):
        obj.mission_uuid = data_record.metadata.best('CNODCMission', default=None, coerce=str)

    # Embargo date
    if hasattr(obj, "embargo_date"):
        obj.embargo_date = obj.metadata.best('CNODCEmbargoUntil', default=None, coerce=AwareDateTime.fromisoformat)

    # Quality Control Test Results
    if hasattr(obj, "qc_tests"):
        qc_test_names = set(x.test_name for x in data_record.qc_tests)
        qc_test_info = {}
        for x in qc_test_names:
            best_result = data_record.latest_test_result(x, True)
            if best_result is not None:
                qc_test_info[x] = {
                    'version': best_result.test_version,
                    'date_run': best_result.test_date,
                    'result': best_result.result.value,
                }
        obj.qc_tests = qc_test_info

    # Observation Status
    if hasattr(obj, "status"):
        new_status = data_record.metadata.best('CNODCStatus', coerce=str, default=None)
        if new_status is not None and hasattr(ObservationStatus, new_status):
            obj.status = getattr(ObservationStatus, new_status)


class NODBSourceFile(s.MetadataMixin, s.NODBBaseObject):

    TABLE_NAME: str = "nodb_source_files"
    PRIMARY_KEYS: tuple[str] = ("source_uuid", "received_date",)
    MOCK_INDEX_KEYS = (
        ('source_path', ),
        ('original_idx', 'original_uuid', 'received_date'),
    )

    db_created_date: AwareDateTime = s.DateTimeColumn(readonly=True)
    db_modified_date: AwareDateTime = s.DateTimeColumn(readonly=True)
    source_uuid: str = s.UUIDColumn()
    received_date: datetime.date = s.DateColumn()

    source_file_identifier: str | None = s.StringColumn()
    source_file_version: int | None = s.IntColumn()

    source_path: str = s.StringColumn()
    file_name: str = s.StringColumn()
    source_name: str = s.StringColumn()
    program_name: str = s.StringColumn()

    original_uuid: str = s.StringColumn()
    original_idx: int = s.IntColumn()

    status: SourceFileStatus = s.EnumColumn(SourceFileStatus)

    history: list = s.JsonListColumn()

    def report_error(self, message, name, version, instance):
        """Add an error to the file history."""
        self.add_history(message, name, version, instance, 'ERROR')

    def report_warning(self, message, name, version, instance):
        """Add a warning to the file history."""
        self.add_history(message, name, version, instance, 'WARNING')

    def add_history(self, message, name, version, instance, level='INFO'):
        """Add a history entry to this file."""
        self.history.append({
            'msg': message,
            'src': name,
            'ver': version,
            'ins': instance,
            'lvl': level,
            'rpt': AwareDateTime.now()
        })
        self._modified_values.add('history')

    def stream_observation_data(self, db: interface.NODBInstance, **kwargs) -> t.Iterable[NODBObservationData]:
        """Find all observations associated with this source file."""
        yield from db.stream_objects(
            obj_cls=NODBObservationData,
            filters={
                'received_date': self.received_date,
                'source_file_uuid': self.source_uuid,
            },
            **kwargs
        )

    def stream_working_records(self, db: interface.NODBInstance, **kwargs) -> t.Iterable[NODBWorkingRecord]:
        """Find a working record associated with this source file."""
        yield from db.stream_objects(
            obj_cls=NODBWorkingRecord,
            filters={
                'received_date': self.received_date,
                'source_file_uuid': self.source_uuid,
            },
            **kwargs
        )

    @classmethod
    def find_by_source_path(cls, db: interface.NODBInstance, source_path: str, **kwargs) -> NODBSourceFile | None:
        """Locate a source file by the source path."""
        return db.load_object(cls, filters={
            'source_path': source_path
        }, **kwargs)

    @classmethod
    def find_by_identifier(cls, db: interface.NODBInstance, identifier: str, **kwargs) -> t.Iterable[NODBSourceFile]:
        """Locate a source file by the source path."""
        return db.stream_objects(cls, filters={
            'source_file_identifier': identifier
        }, **kwargs)

    @classmethod
    def find_by_original_info(cls, db: interface.NODBInstance, original_uuid: str, received_date: ct.AcceptAsDateTime, message_idx: int, **kwargs) -> NODBSourceFile | None:
        """Locate a source file that was a part of another source file by the original file info."""
        return db.load_object(cls, filters={
            'original_idx': message_idx,
            'received_date': coerce.as_date(received_date),
            'original_uuid': original_uuid
        }, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, source_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> NODBSourceFile | None:
        """Locate a source file by UUID."""
        return db.load_object(cls, filters={
            'source_uuid': source_uuid,
            'received_date': coerce.as_date(received_date)
        }, **kwargs)


class NODBMission(s.MetadataMixin, s.NODBBaseObject):

    TABLE_NAME = 'nodb_missions'
    PRIMARY_KEYS = ("mission_uuid",)

    mission_uuid: str = s.UUIDColumn()
    mission_id: str = s.StringColumn()
    start_date: AwareDateTime | None = s.DateTimeColumn()
    end_date: AwareDateTime | None = s.DateTimeColumn()

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, mission_uuid: str, **kwargs) -> t.Optional[NODBMission]:
        """Find a workflow by name."""
        return db.load_object(cls, {"mission_uuid": mission_uuid},  **kwargs)

    @classmethod
    def search(cls, db: interface.NODBInstance, mission_id: t.Optional[str] = None, **kwargs) -> t.Iterable[NODBMission]:
        if mission_id is not None:
            yield from db.stream_objects(
                obj_cls=cls,
                filters={'mission_id': mission_id},
            **kwargs)


class NODBPlatform(s.MetadataMixin, s.NODBBaseObject):

    TABLE_NAME = 'nodb_platforms'
    PRIMARY_KEYS = ('platform_uuid',)

    platform_uuid: str = s.UUIDColumn()
    wmo_id: str | None = s.StringColumn()
    wigos_id: str | None = s.StringColumn()
    ship_code: str | None = s.StringColumn()
    platform_name: str | None = s.StringColumn()
    platform_id: str | None = s.StringColumn()
    platform_type: str | None = s.StringColumn()
    service_start_date: AwareDateTime | None = s.DateTimeColumn()
    service_end_date: AwareDateTime | None = s.DateTimeColumn()
    instrumentation: dict = s.JsonDictColumn()
    map_to_uuid: str | None = s.UUIDColumn()
    status: PlatformStatus = s.EnumColumn(PlatformStatus)
    embargo_data_days: int | None = s.IntColumn()

    @property
    def mandatory_review(self) -> bool:
        return bool(self.metadata.get("mandatory_review", False))

    @property
    def skip_speed_check(self) -> bool:
        return bool(self.metadata.get('skip_speed_check', False))

    @property
    def skip_on_land_check(self) -> bool:
        return bool(self.metadata.get("skip_on_land_check", False))

    @property
    def dedupe_time_window(self) -> int | float | None:
        x: str | int | float | None = self.metadata.get("dedupe_time_window", None)
        if x is None:
            return None
        else:
            return float(x)

    @property
    def dedupe_distance_window(self) -> int | float | None:
        x: str | int | float | None = self.metadata.get("dedupe_distance_window", None)
        if x is None:
            return None
        return float(x)

    @property
    def top_speed(self) -> float | int | tuple[float | int, str] | None:
        top_speed = self.metadata.get('top_speed', None)
        if isinstance(top_speed, (int, float)):
            return top_speed
        elif isinstance(top_speed, str):
            if " " in top_speed:
                speed, units = top_speed.split(" ", maxsplit=1)
            else:
                speed = top_speed
                units = "m s-1"
            return float(speed.strip()), units.strip()
        elif isinstance(top_speed, dict):
            try:
                element = AbstractElement.build_from_mapping(top_speed)
                if element.is_numeric():
                    if element.metadata.has_value("Units"):
                        return element.to_float(), element.metadata.best("Units", coerce=str)
                    else:
                        return element.to_float()
            except KeyError: ...
        return None

    @classmethod
    def search(cls,
               db: interface.NODBInstance,
               in_service_time: t.Optional[AwareDateTime] = None,
               wmo_id: t.Optional[str] = None,
               wigos_id: t.Optional[str] = None,
               platform_id: t.Optional[str] = None,
               platform_name: t.Optional[str] = None,
               ship_code: t.Optional[str] = None,
               **kwargs) -> t.Iterable[NODBPlatform]:
        """Search for a platform by various identifiers."""
        filters = {}
        # TODO: we should standardize these
        if wmo_id is not None and wmo_id != '':
            filters['wmo_id'] = wmo_id
        if wigos_id is not None and wigos_id != '':
            filters['wigos_id'] = wigos_id
        # TODO: we should do case-insensitive comparisons for these
        if platform_id is not None and platform_id != '':
            filters['platform_id'] = platform_id
        if platform_name is not None and platform_name != '':
            filters['platform_name'] = platform_name
        if ship_code is not None and ship_code != '':
            filters['ship_code'] = ship_code
        if filters:
            res = db.stream_objects(cls, filters=filters, join_str='OR', **kwargs)
            if in_service_time is None:
                yield from res
            else:
                for p in res:
                    if p.service_start_date is not None and p.service_start_date > in_service_time:
                        continue
                    elif p.service_end_date is not None and p.service_end_date < in_service_time:
                        continue
                    else:
                        yield p

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, platform_uuid: str, **kwargs) -> t.Optional[NODBPlatform]:
        """Locate a platform by its unique identifier."""
        return db.load_object(cls, filters={
            'platform_uuid': platform_uuid
        }, **kwargs)

    @classmethod
    def find_all_raw(cls, db: interface.NODBInstance, **kwargs) -> t.Iterable[dict]:
        """Retrieve all platforms in a raw (i.e. database dictionary) format."""
        yield from db.stream_raw(obj_cls=NODBPlatform, **kwargs)


class NODBBatch(s.MetadataMixin, s.NODBBaseObject):

    TABLE_NAME = 'nodb_qc_batches'
    PRIMARY_KEYS = ("batch_uuid",)

    batch_uuid: str = s.UUIDColumn()
    status: BatchStatus = s.EnumColumn(BatchStatus)

    db_created_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)
    db_modified_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)

    def stream_working_records(self, db: interface.NODBInstance, **kwargs) -> t.Iterable[NODBWorkingRecord]:
        yield from db.stream_objects(
            obj_cls=NODBWorkingRecord,
            filters={
                'qc_batch_id': self.batch_uuid,
            },
            **kwargs
        )

    def count_working_records(self, db: interface.NODBInstance) -> int:
        return NODBBatch.count_working_by_uuid(db, self.batch_uuid)

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, batch_uuid: str, **kwargs) -> t.Optional[NODBBatch]:
        return db.load_object(cls, filters={
            'batch_uuid': batch_uuid
        }, **kwargs)

    @classmethod
    def count_working_by_uuid(cls, db: interface.NODBInstance, batch_uuid: str) -> int:
        return db.count_objects(
            obj_cls=NODBWorkingRecord,
            filters={'qc_batch_id': batch_uuid}
        )


class NODBObservation(s.NODBBaseObject):
    """Represents an archived observation in the database.

        In particular, this table/class represents the characteristics of data records
        that are usually searchable. The actual record is stored as an NODBObservationData.
    """

    TABLE_NAME = "nodb_obs"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = s.UUIDColumn()
    received_date: datetime.date = s.DateColumn()

    platform_uuid: t.Optional[str] = s.UUIDColumn()
    mission_uuid: t.Optional[str] = s.UUIDColumn()
    obs_time: t.Optional[AwareDateTime] = s.DateTimeColumn()
    min_depth: t.Optional[float] = s.FloatColumn()
    max_depth: t.Optional[float] = s.FloatColumn()
    min_time: AwareDateTime | None = s.DateTimeColumn()
    max_time: AwareDateTime | None = s.DateTimeColumn()
    location: str = s.WKTColumn()
    observation_type: ObservationType = s.EnumColumn(ObservationType)
    surface_parameters: set[str] = s.JsonSetColumn()
    profile_parameters: set[str] = s.JsonSetColumn()
    data_mode: DataMode = s.EnumColumn(DataMode, default=DataMode.UNKNOWN)
    quality_checks: int = s.IntColumn(default=0)
    embargo_date: t.Optional[AwareDateTime] = s.DateTimeColumn()
    instrument_types: set[str] = s.JsonSetColumn()
    observation_identifier: str | None = s.StringColumn()

    @classmethod
    def search(cls,
               db: interface.NODBInstance,
               platform_uuid: str | None = None,
               start_time: AwareDateTime | None = None,
               end_time: AwareDateTime | None = None,
               min_latitude: float | None = None,
               max_latitude: float | None = None,
               min_longitude: float | None = None,
               max_longitude: float | None = None,
               data_mode: DataMode | None = None,
               quality_checks: int | QualityCheckFlags | None = None,
               **kwargs) -> t.Iterable[NODBObservation]:
        filters = {}
        if min_latitude is not None or min_longitude is not None or max_latitude is not None or max_longitude is not None:
            filters['location'] = ((
                min_longitude if min_longitude is not None else -180,
                min_latitude if min_latitude is not None else -90,
                max_longitude if max_longitude is not None else 180,
                max_latitude if max_latitude is not None else 90,
            ), 'IN_ENVELOPE', False)
        if platform_uuid is not None:
            filters['platform_uuid'] = platform_uuid
        if start_time is not None:
            filters['obs_time'] = (start_time, '>=', False)
        if end_time is not None:
            filters['obs_time'] = (end_time, '<=', False)
        if data_mode is not None:
            filters['data_mode'] = data_mode.value
        if quality_checks:
            filters['quality_checks'] = (quality_checks, '&', False)
        yield from db.stream_objects(cls, filters=filters, **kwargs)

    @classmethod
    def prepare_insert(cls, db: interface.NODBInstance, name: str) -> interface.PreparedStatementProtocol:
        return db.prepared_insert(cls, data_map={
            'obs_uuid': 'UUID',
            'received_date': 'DATE',
            'platform_uuid': 'UUID',
            'mission_uuid': 'UUID',
            'obs_time': 'TIMESTAMPTZ',
            'min_depth': 'FLOAT',
            'max_depth': 'FLOAT',
            'location': 'geography',
            'observation_type': 'obs_type',
            'surface_parameters': 'JSONB',
            'profile_parameters': 'JSONB',
            'instruments': 'JSONB',
            'data_mode': 'VARCHAR',
            'quality_checks': 'INT',
            'embargo_date': 'TIMESTAMPTZ',
        }, name=name)

    def find_observation_data(self, db: interface.NODBInstance, **kwargs) -> NODBObservationData | None:
        return NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)

    def find_relationships(self, db, **kwargs) -> t.Iterable[NODBObservationRelationship]:
        yield from NODBObservationRelationship.find_by_observation(db, self.obs_uuid, self.received_date, **kwargs)

    def update_from_record(self, record: ocproc2.ParentRecord):
        update_common_from_data_record(self, record)

    @classmethod
    def find_best_copy(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, known_better_than: t.Iterable[tuple[str, ct.AcceptAsDateTime]] | None = None) -> set[tuple[str, datetime.date]]:
        results = set()
        exclude_list = set()
        if known_better_than is not None:
            search_list = {*known_better_than}
            if search_list:
                exclude_list.add((obs_uuid, received_date))
            else:
                results.add((obs_uuid, received_date))
        else:
            search_list = {(obs_uuid, received_date)}
        while search_list:
            check_uuid, check_date = search_list.pop()
            if (check_uuid, check_date) in exclude_list:
                continue
            exclude_list.add((check_uuid, check_date))
            found = False
            for rel in NODBObservationRelationship.better_than(db, check_uuid, check_date):
                search_list.add((rel.right_obs_uuid, rel.right_received_date))
                found = True
            # no records are better than it
            if not found:
                results.add((check_uuid, check_date))
        return results

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Self | None:
        """Find an observation by UUID and received date."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": coerce.as_date(received_date)
        }, **kwargs)

    @classmethod
    def find_by_observation_identifier(cls, db: interface.NODBInstance, identifier: str, **kwargs) -> t.Iterable[NODBObservation]:
        """Find a working record by its identifier"""
        yield from db.stream_objects(cls, {
            "observation_identifier": identifier,
        }, **kwargs)


class NODBObservationData(_RecordMixin, s.MetadataMixin, s.NODBBaseObject):
    """Represents the 'meat' of an archived observation; the full record and associated metadata."""

    TABLE_NAME = "nodb_obs_data"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = s.UUIDColumn()
    received_date: datetime.date = s.DateColumn()
    source_file_uuid: str = s.StringColumn()
    message_idx: int = s.IntColumn()
    record_idx: int = s.IntColumn()
    qc_tests: dict[str, dict[str, str]] = s.JsonDictColumn()
    data_mode: DataMode = s.EnumColumn(DataMode, default=DataMode.UNKNOWN)
    quality_checks: int = s.IntColumn(default=0)
    status: ObservationStatus = s.EnumColumn(ObservationStatus, default=ObservationStatus.UNVERIFIED)
    observation_identifier: str | None = s.StringColumn()

    @classmethod
    def prepare_insert(cls, db: interface.NODBInstance, name: str) -> interface.PreparedStatementProtocol:
        return db.prepared_insert(cls, data_map={
            'obs_uuid': 'UUID',
            'received_date': 'DATE',
            'source_file_uuid': 'UUID',
            'message_idx': 'INT',
            'record_idx': 'INT',
            'qc_tests': 'JSON',
            'duplicate_uuid': 'UUID',
            'duplicate_received_date': 'DATE',
            'status': 'obs_status',
            'data_mode': 'CHAR(2)',
            'quality_checks': 'BIGINT',
        }, name=name)

    @classmethod
    def get_mock_index_keys(cls) -> list[list[str]]:
        keys = super().get_mock_index_keys()
        keys.append(['source_file_uuid', 'received_date', 'message_idx', 'record_idx', 'processing_level'])
        return keys

    def find_source_file(self, db: interface.NODBInstance):
        return NODBSourceFile.find_by_uuid(db, self.source_file_uuid, self.received_date)

    def find_observation(self, db: interface.NODBInstance):
        return NODBObservation.find_by_uuid(db, self.obs_uuid, self.received_date)

    def find_relationships(self, db, **kwargs) -> t.Iterable[NODBObservationRelationship]:
        yield from NODBObservationRelationship.find_by_observation(db, self.obs_uuid, self.received_date, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Optional[NODBObservationData]:
        """Locate a record by UUID."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": s.parse_received_date(received_date)
        }, **kwargs)

    @classmethod
    def find_by_observation_identifier(cls, db: interface.NODBInstance, identifier: str, **kwargs) -> t.Iterable[NODBObservationData]:
        """Find a working record by its identifier"""
        yield from db.stream_objects(cls, {
            "observation_identifier": identifier,
        }, **kwargs)

    @classmethod
    def find_all_by_source_file_raw(cls,
                            db: interface.NODBInstance,
                            source_file_uuid: str,
                            source_received_date: ct.AcceptAsDateTime,
                            **kwargs) -> t.Iterable[dict]:
        """Locate a record by information about it in the source file."""
        filters = {
            "received_date": coerce.as_date(source_received_date),
            "source_file_uuid": source_file_uuid,
        }
        if 'filters' in kwargs:
            kwargs['filters'].update(filters)
        else:
            kwargs['filters'] = filters
        return db.stream_raw(cls, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db: interface.NODBInstance,
                            source_file_uuid: str,
                            source_received_date: ct.AcceptAsDateTime,
                            message_idx: int,
                            record_idx: int,
                            data_mode: DataMode | None = None,
                            quality_checks: int | QualityCheckFlags | None = None,
                            **kwargs) -> t.Optional[NODBObservationData]:
        """Locate a record by information about it in the source file."""
        filters: dict[str, t.Any] = {
            "received_date": coerce.as_date(source_received_date),
            "source_file_uuid": source_file_uuid,
            "message_idx": message_idx,
            "record_idx": record_idx,
        }
        if data_mode is not None:
            filters['data_mode'] = data_mode.value
        if quality_checks:
            filters['quality_checks'] = (quality_checks, '&', False)
        return db.load_object(cls, filters, **kwargs)


class NODBWorkingRecord(_RecordMixin, s.MetadataMixin, s.NODBBaseObject):
    """Represents a record currently being processed in the database."""

    TABLE_NAME = "nodb_working"
    PRIMARY_KEYS = ("working_uuid",)
    MOCK_INDEX_KEYS = (
        ('source_file_uuid', 'received_date', 'message_idx', 'record_idx'),
    )

    working_uuid: str | None = s.UUIDColumn()
    record_uuid: str | None = s.UUIDColumn()
    received_date: datetime.date | None = s.DateColumn()
    source_file_uuid: str | None = s.UUIDColumn()
    message_idx: int | None = s.IntColumn()
    record_idx: int | None = s.IntColumn()
    qc_batch_id: str | None = s.UUIDColumn()
    platform_uuid: str | None = s.UUIDColumn()
    obs_time: AwareDateTime | None = s.DateTimeColumn()
    min_time: AwareDateTime | None = s.DateTimeColumn()
    max_time: AwareDateTime | None = s.DateTimeColumn()
    location: str | None = s.WKTColumn()
    data_mode: DataMode = s.EnumColumn(DataMode, default=DataMode.UNKNOWN)
    quality_checks: int = s.IntColumn(default=0)
    observation_identifier: str | None = s.StringColumn()

    db_created_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)
    db_modified_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)

    @classmethod
    def find_by_observation_identifier(cls, db: interface.NODBInstance, identifier: str, **kwargs) -> t.Iterable[NODBWorkingRecord]:
        """Find a working record by its identifier"""
        yield from db.stream_objects(cls, {
            "observation_identifier": identifier,
        }, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, **kwargs) -> t.Optional[NODBWorkingRecord]:
        """Find a working record by its identifier"""
        return db.load_object(cls, {
            "working_uuid": obs_uuid,
        }, **kwargs)

    @classmethod
    def search(cls,
               db: interface.NODBInstance,
               platform_uuid: str | None = None,
               start_time: AwareDateTime | None = None,
               end_time: AwareDateTime | None = None,
               min_latitude: float | None = None,
               max_latitude: float | None = None,
               min_longitude: float | None = None,
               max_longitude: float | None = None,
               qc_flag: int | QualityCheckFlags | None = None,
               data_mode: DataMode | None = None,
               **kwargs) -> t.Iterable[NODBWorkingRecord]:
        filters = {}
        if min_latitude is not None or min_longitude is not None or max_latitude is not None or max_longitude is not None:
            filters['location'] = ((
                min_longitude if min_longitude is not None else -180,
                min_latitude if min_latitude is not None else -90,
                max_longitude if max_longitude is not None else 180,
                max_latitude if max_latitude is not None else 90,
            ), 'IN_ENVELOPE', False)
        if platform_uuid is not None:
            filters['platform_uuid'] = platform_uuid
        if start_time is not None:
            filters['obs_time'] = (start_time, '>=', False)
        if end_time is not None:
            filters['obs_time'] = (end_time, '<=', False)
        if qc_flag:
            filters['quality_checks'] = (qc_flag, '&', False)
        if data_mode is not None:
            filters['data_mode'] = data_mode.value
        yield from db.stream_objects(cls, filters=filters, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db: interface.NODBInstance,
                            source_file_uuid: str,
                            source_received_date: ct.AcceptAsDateTime,
                            message_idx: int,
                            record_idx: int,
                            data_mode: DataMode | None = None,
                            quality_checks: int | QualityCheckFlags | None = None,
                            **kwargs) -> t.Optional[NODBWorkingRecord]:
        """Find a working record by its source information"""
        filters: dict[str, t.Any] = {
            "received_date": s.parse_received_date(source_received_date),
            "source_file_uuid": source_file_uuid,
            "message_idx": message_idx,
            "record_idx": record_idx
        }
        if data_mode is not None:
            filters['data_mode'] = data_mode.value
        if quality_checks:
            filters['quality_checks'] = (quality_checks, '&', False)
        return db.load_object(cls, filters=filters, **kwargs)

    @staticmethod
    def bulk_set_batch_uuid(
            db: interface.NODBInstance,
            working_uuids: list[str],
            batch_uuid: str):
        db.bulk_update_objects(
            NODBWorkingRecord,
            updates={'qc_batch_id': batch_uuid},
            key_field='working_uuid',
            key_values=working_uuids
        )


class NODBObservationRelationship(s.NODBBaseObject):

    TABLE_NAME = "nodb_observation_relationships"
    PRIMARY_KEYS = ("left_obs_uuid", "left_received_date", "right_obs_uuid", "right_received_date", "relationship_type",)

    left_obs_uuid: str = s.UUIDColumn()
    left_received_date: datetime.date = s.DateColumn()
    right_obs_uuid: str = s.UUIDColumn()
    right_received_date: datetime.date = s.DateColumn()
    relationship_type: ObservationRelationshipType = s.EnumColumn(ObservationRelationshipType)

    def exists(self, db) -> bool:
        return db.load_object(self.__class__, filters={
            'left_obs_uuid': self.left_obs_uuid,
            'left_received_date': self.left_received_date,
            'right_obs_uuid': self.right_obs_uuid,
            'right_received_date': self.right_received_date,
            'relationship_type': self.relationship_type.value,
        }, key_only=True) is not None

    def left_observation(self, db, **kwargs) -> NODBObservation | None:
        return NODBObservation.find_by_uuid(db, self.left_obs_uuid, self.left_received_date, **kwargs)

    def right_observation(self, db, **kwargs) -> NODBObservation | None:
        return NODBObservation.find_by_uuid(db, self.right_obs_uuid, self.right_received_date, **kwargs)

    @classmethod
    def better_than(cls, db, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Iterable[NODBObservationRelationship]:
        yield from db.stream_objects(cls, filters={
            'left_obs_uuid': obs_uuid,
            'left_received_date': s.parse_received_date(received_date),
            'relationship_type': ObservationRelationshipType.IS_BETTER.value
        }, **kwargs)
        yield from db.stream_object(cls, filters={
            'left_obs_uuid': obs_uuid,
            'left_received_date': s.parse_received_date(received_date),
            'relationship_type': ObservationRelationshipType.IS_DUPLICATE.value
        })

    @classmethod
    def find_by_observation(cls, db, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Iterable[NODBObservationRelationship]:
        yield from db.stream_objects(
            cls, filters={
                'left_obs_uuid': obs_uuid,
                'left_received_date': s.parse_received_date(received_date)
            }, **kwargs
        )
        yield from db.stream_objects(
            cls, filters={
                'right_obs_uuid': obs_uuid,
                'right_received_date': s.parse_received_date(received_date)
            }, **kwargs
        )
