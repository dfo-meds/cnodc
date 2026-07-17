import dataclasses
import datetime
import enum
import typing as t

import medsutil.ocproc2 as ocproc2
import nodb.base as s
import medsutil.types as ct
import nodb.interface as interface
from medsutil.ocproc2 import AbstractElement
from medsutil.ocproc2.codecs.ocproc2bin import OCProc2BinCodec
from medsutil.awaretime import AwareDateTime
from medsutil.sanitize import coerce
from nodb.interface import NODBInstance


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


class QualityCheckFlags(enum.IntFlag):
    DEDUPLICATE = 1
    GTSPP = 2


class ObservationRelationshipType(enum.Enum):

    # A (relationship types) B
    IS_DUPLICATE = 'is_duplicate_of'
    BETTER_QUALITY = 'is_better_than'



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
    if hasattr(obj, 'obs_time') and data_record.coordinates.has_value('Time') and data_record.coordinates['Time'].is_iso_datetime():
        obj.obs_time = data_record.coordinates['Time'].to_datetime()
    if hasattr(obj, 'location') and data_record.coordinates.has_value('Latitude') and data_record.coordinates['Latitude'].is_numeric() and data_record.coordinates.has_value('Longitude') and data_record.coordinates['Longitude'].is_numeric():
        lat = data_record.coordinates['Latitude'].to_float()
        lon = data_record.coordinates['Longitude'].to_float()
        obj.location = f"POINT ({round(lon, 5)} {round(lat, 5)})"
    if hasattr(obj, 'platform_uuid') and data_record.metadata.has_value('CNODCPlatform'):
        obj.platform_uuid = data_record.metadata.best('CNODCPlatform', None)


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

    replaces_uuid: str | None = s.UUIDColumn()
    replaces_received_date: datetime.date | None = s.DateColumn()

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

    def replaces_file(self, db: interface.NODBInstance, **kwargs) -> NODBSourceFile | None:
        if self.replaces_uuid is None or self.replaces_received_date is None:
            return None
        return db.load_object(self.__class__, filters={
            'source_uuid': self.replaces_uuid,
            'received_date': self.replaces_received_date,
        }, **kwargs)

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
    platform_name: str | None = s.StringColumn()
    platform_id: str | None = s.StringColumn()
    platform_type: str = s.StringColumn()
    service_start_date: AwareDateTime | None = s.DateTimeColumn()
    service_end_date: AwareDateTime | None = s.DateTimeColumn()
    instrumentation: dict = s.JsonDictColumn()
    map_to_uuid: str = s.UUIDColumn()
    status: PlatformStatus = s.EnumColumn(PlatformStatus)
    embargo_data_days: int = s.IntColumn()

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
               **kwargs) -> t.Iterable[NODBPlatform]:
        """Search for a platform by various identifiers."""
        filters = {}
        if wmo_id is not None and wmo_id != '':
            filters['wmo_id'] = wmo_id
        if wigos_id is not None and wigos_id != '':
            filters['wigos_id'] = wigos_id
        if platform_id is not None and platform_id != '':
            filters['platform_id'] = platform_id
        if platform_name is not None and platform_name != '':
            filters['platform_name'] = platform_name
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


@dataclasses.dataclass
class SubrecordInfo:
    min_depth: t.Optional[float] = None
    max_depth: t.Optional[float] = None
    profile_parameters: set[str] = dataclasses.field(default_factory=set)
    surface_parameters: set[str] = dataclasses.field(default_factory=set)


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
    location: str = s.WKTColumn()
    observation_type: ObservationType = s.EnumColumn(ObservationType)
    surface_parameters: set[str] = s.JsonSetColumn()
    profile_parameters: set[str] = s.JsonSetColumn()
    data_mode: DataMode = s.EnumColumn(DataMode, default=DataMode.UNKNOWN)
    quality_checks: int = s.IntColumn(default=0)
    embargo_date: t.Optional[AwareDateTime] = s.DateTimeColumn()

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
            'surface_parameters': 'JSON',
            'profile_parameters': 'JSON',
            'processing_level': 'processing_level',
            'embargo_date': 'TIMESTAMPTZ',
        }, name=name)

    def find_observation_data(self, db: interface.NODBInstance, **kwargs) -> NODBObservationData | None:
        return NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)

    def find_relationships(self, db, **kwargs) -> t.Iterable[NODBObservationRelationship]:
        yield from NODBObservationRelationship.find_by_observation(db, self.obs_uuid, self.received_date, **kwargs)

    def update_from_record(self, record: ocproc2.ParentRecord):
        update_common_from_data_record(self, record)
        self.mission_uuid = record.metadata.best('CNODCMission', default=None, coerce=str)
        if record.metadata.has_value('CNODCEmbargoUntil'):
            self.embargo_date = record.metadata['CNODCEmbargoUntil'].to_datetime()
        ref_info = SubrecordInfo()
        NODBObservation._extract_subrecord_info(record, ref_info)
        self.profile_parameters = ref_info.profile_parameters
        self.surface_parameters = ref_info.surface_parameters
        self.min_depth = ref_info.min_depth
        self.max_depth = ref_info.max_depth
        if self.location is None or self.obs_time is None:
            self.observation_type = ObservationType.OTHER
        elif self.min_depth is not None and self.min_depth > 0:
            self.observation_type = ObservationType.AT_DEPTH
        elif (self.min_depth is None or self.min_depth == 0) and (self.max_depth is None or self.max_depth == 0):
            self.observation_type = ObservationType.SURFACE
        else:
            self.observation_type = ObservationType.PROFILE

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Self | None:
        """Find an observation by UUID and received date."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": coerce.as_date(received_date)
        }, **kwargs)

    @staticmethod
    def _extract_subrecord_info(record: ocproc2.BaseRecord, ref_info: SubrecordInfo, position: dict = None):
        if position is None:
            position = {}
        else:
            position = { x: position[x] for x in position  if position[x] is not None}
        for key in ('Latitude', 'Longitude', 'Depth', 'Pressure'):
            if record.coordinates.has_value(key):
                if key == 'Depth':
                    position[key] = record.coordinates['Depth'].to_float('m')
                elif key == 'Pressure':
                    position[key] = record.coordinates['Pressure'].to_float('dbar')
                else:
                    position[key] = record.coordinates[key].to_float('degree')

        depth = None
        if 'Depth' in position:
            depth = position['Depth']
        elif 'Pressure' in position and 'Depth' not in position and 'Latitude' in position:
            from medsutil.seawater import eos80_depth
            depth = eos80_depth(position['Pressure'], position['Latitude'])
        if depth is not None:
            if ref_info.min_depth is None or ref_info.min_depth > depth:
                ref_info.min_depth = coerce.as_float(depth)
            if ref_info.max_depth is None or ref_info.max_depth < depth:
                ref_info.max_depth = coerce.as_float(depth)

        if ('Depth' in position and position['Depth'] != 0) or ('Pressure' in position and position['Pressure'] != 0):
            ref_info.profile_parameters.update(x for x in record.parameters.keys())
        else:
            ref_info.surface_parameters.update(x for x in record.parameters.keys())

        for subrecord in record.iter_subrecords():
            NODBObservation._extract_subrecord_info(subrecord, ref_info, position)


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

    def find_observation(self, db: interface.NODBInstance):
        return NODBObservation.find_by_uuid(db, self.obs_uuid, self.received_date)

    def _update_from_data_record(self, data_record: ocproc2.ParentRecord):
        super()._update_from_data_record(data_record)
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
        self.qc_tests = qc_test_info
        if data_record.metadata.has_value('CNODCDuplicateId') and data_record.metadata.has_value('CNODCDuplicateDate'):
            self.duplicate_received_date = data_record.metadata['CNODCDuplicateDate'].to_date()
            self.duplicate_uuid = data_record.metadata.best('CNODCDuplicateId', coerce=str)
        if data_record.metadata.has_value('CNODCStatus'):
            new_status = data_record.metadata.best('CNODCStatus', coerce=str)
            if hasattr(ObservationStatus, new_status):
                self.status = getattr(ObservationStatus, new_status)

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
    location: str | None = s.WKTColumn()
    data_mode: DataMode = s.EnumColumn(DataMode, default=DataMode.UNKNOWN)
    quality_checks: int = s.IntColumn(default=0)

    db_created_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)
    db_modified_date: AwareDateTime | None = s.DateTimeColumn(readonly=True)


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
    left_obs_uuid: str = s.UUIDColumn()
    left_received_date: datetime.date = s.DateColumn()
    right_obs_uuid: str = s.UUIDColumn()
    right_received_date: datetime.date = s.DateColumn()
    relationship_type: ObservationRelationshipType = s.EnumColumn(ObservationRelationshipType)

    def left_observation(self, db, **kwargs) -> NODBObservation | None:
        return NODBObservation.find_by_uuid(db, self.left_obs_uuid, self.left_received_date, **kwargs)

    def right_observation(self, db, **kwargs) -> NODBObservation | None:
        return NODBObservation.find_by_uuid(db, self.right_obs_uuid, self.right_received_date, **kwargs)

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
