import dataclasses
import datetime
import enum
import typing as t

import medsutil.ocproc2 as ocproc2
import nodb.base as s
import medsutil.types as ct
import nodb.interface as interface
from medsutil.ocproc2.codecs.ocproc2bin import OCProc2BinCodec
from medsutil.seawater import eos80_depth
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



class ProcessingLevel(enum.Enum):
    """Processing level of a record in the database."""

    RAW = 'RAW'
    ADJUSTED = 'ADJUSTED'
    REAL_TIME = 'REAL_TIME'
    DELAYED_MODE = 'DELAYED_MODE'
    UNKNOWN = 'UNKNOWN'


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
    if hasattr(obj, 'processing_level'):
        obj.processing_level = ProcessingLevel.UNKNOWN
        level = data_record.metadata.best('CNODCLevel', coerce=str, default=None)
        if level is not None and hasattr(ProcessingLevel, level):
            obj.processing_level = getattr(ProcessingLevel, level)
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
    wmo_id: str = s.StringColumn()
    wigos_id: str = s.StringColumn()
    platform_name: str = s.StringColumn()
    platform_id: str = s.StringColumn()
    platform_type: str = s.StringColumn()
    service_start_date: AwareDateTime | None = s.DateTimeColumn()
    service_end_date: AwareDateTime | None = s.DateTimeColumn()
    instrumentation: dict = s.JsonDictColumn()
    map_to_uuid: str = s.UUIDColumn()
    status: PlatformStatus = s.EnumColumn(PlatformStatus)
    embargo_data_days: int = s.IntColumn()

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
    processing_level: ProcessingLevel = s.EnumColumn(ProcessingLevel)
    embargo_date: t.Optional[AwareDateTime] = s.DateTimeColumn()

    def find_observation_data(self, db: interface.NODBInstance) -> NODBObservationData | None:
        return NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date)

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
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs):
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
    duplicate_uuid: str = s.UUIDColumn()
    duplicate_received_date: datetime.date = s.DateColumn()
    status: ObservationStatus = s.EnumColumn(ObservationStatus)
    processing_level: ProcessingLevel = s.EnumColumn(ProcessingLevel)

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

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, received_date: ct.AcceptAsDateTime, **kwargs) -> t.Optional[NODBObservationData]:
        """Locate a record by UUID."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": s.parse_received_date(received_date)
        }, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db: interface.NODBInstance,
                            source_file_uuid: str,
                            source_received_date: ct.AcceptAsDateTime,
                            message_idx: int,
                            record_idx: int,
                            processing_level: t.Optional[t.Union[str, ProcessingLevel]] = None,
                            **kwargs) -> t.Optional[NODBObservationData]:
        """Locate a record by information about it in the source file."""
        if processing_level is None:
            plevel = ProcessingLevel.UNKNOWN.value
        elif not isinstance(processing_level, str):
            plevel = processing_level.value
        else:
            plevel = processing_level
        filters = {
            "received_date": coerce.as_date(source_received_date),
            "source_file_uuid": source_file_uuid,
            "message_idx": message_idx,
            "record_idx": record_idx,
            "processing_level": plevel
        }
        return db.load_object(cls, filters, **kwargs)




class NODBWorkingRecord(_RecordMixin, s.MetadataMixin, s.NODBBaseObject):
    """Represents a record currently being processed in the database."""

    TABLE_NAME = "nodb_working"
    PRIMARY_KEYS = ("working_uuid",)
    MOCK_INDEX_KEYS = (
        ('source_file_uuid', 'received_date', 'message_idx', 'record_idx'),
    )

    working_uuid: str = s.UUIDColumn()
    record_uuid: t.Optional[str] = s.UUIDColumn()
    received_date: datetime.date = s.DateColumn()
    source_file_uuid: str = s.UUIDColumn()
    message_idx: int = s.IntColumn()
    record_idx: int = s.IntColumn()
    qc_batch_id: str = s.UUIDColumn()
    platform_uuid: str = s.UUIDColumn()
    obs_time: AwareDateTime = s.DateTimeColumn()
    location: str = s.WKTColumn()

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, obs_uuid: str, **kwargs) -> t.Optional[NODBWorkingRecord]:
        """Find a working record by its identifier"""
        return db.load_object(cls, {
            "working_uuid": obs_uuid,
        }, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db: interface.NODBInstance,
                            source_file_uuid: str,
                            source_received_date: ct.AcceptAsDateTime,
                            message_idx: int,
                            record_idx: int,
                            **kwargs) -> t.Optional[NODBWorkingRecord]:
        """Find a working record by its source information"""
        return db.load_object(cls, {
                "received_date": s.parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, **kwargs)

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

