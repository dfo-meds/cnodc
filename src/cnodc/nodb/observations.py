from __future__ import annotations
import datetime
import enum
import typing as t
from cnodc.ocproc2.codecs.ocproc2bin import OCProc2BinCodec
import cnodc.ocproc2 as ocproc2
from cnodc.science.seawater import eos80_depth
import cnodc.nodb.base as s


if t.TYPE_CHECKING:  # pragma: no coverage
    from cnodc.nodb import NODBControllerInstance


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


class NODBSourceFile(s.NODBBaseObject, s.MetadataMixin):

    TABLE_NAME: str = "nodb_source_files"
    PRIMARY_KEYS: tuple[str] = ("source_uuid", "received_date",)

    source_uuid: str = s.UUIDColumn("source_uuid")
    received_date: datetime.date = s.DateColumn("received_date")

    source_path: str = s.StringColumn("source_path")
    file_name: str = s.StringColumn("file_name")

    original_uuid: str = s.StringColumn("original_uuid")
    original_idx: int = s.IntColumn("original_idx", coerce=int)

    status: SourceFileStatus = s.EnumColumn("status", SourceFileStatus)

    history: list = s.JsonColumn("history")

    metadata: t.Optional[dict] = s.JsonColumn("metadata")

    def report_error(self, message, name, version, instance):
        """Add an error to the file history."""
        self.add_history(message, name, version, instance, 'ERROR')

    def report_warning(self, message, name, version, instance):
        """Add a warning to the file history."""
        self.add_history(message, name, version, instance, 'WARNING')

    def add_history(self, message, name, version, instance, level='INFO'):
        """Add a history entry to this file."""
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

    def stream_observation_data(self, db: NODBControllerInstance, **kwargs) -> t.Iterable[NODBObservationData]:
        """Find all observations associated with this source file."""
        yield from db.stream_objects(
            obj_cls=NODBObservationData,
            filters={
                'received_date': self.received_date,
                'source_file_uuid': self.source_uuid,
            },
            **kwargs
        )

    def stream_working_records(self, db: NODBControllerInstance, **kwargs) -> t.Iterable[NODBWorkingRecord]:
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
    def find_by_source_path(cls, db: NODBControllerInstance, source_path: str, **kwargs):
        """Locate a source file by the source path."""
        return db.load_object(cls, {
            'source_path': source_path
        }, **kwargs)

    @classmethod
    def find_by_original_info(cls, db: NODBControllerInstance, original_uuid: str, received_date: t.Union[datetime.date, str], message_idx: int, **kwargs):
        """Locate a source file that was a part of another source file by the original file info."""
        return db.load_object(cls, {
            'original_idx': message_idx,
            'received_date': s.parse_received_date(received_date),
            'original_uuid': original_uuid
        }, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, source_uuid: str, received: t.Union[datetime.date, str], **kwargs):
        """Locate a source file by UUID."""
        return db.load_object(cls, {
            'source_uuid': source_uuid,
            'received_date': s.parse_received_date(received)
        }, **kwargs)


class NODBMission(s.NODBBaseObject):

    TABLE_NAME = 'nodb_missions'
    PRIMARY_KEYS = ("mission_uuid",),

    mission_uuid: str = s.UUIDColumn("mission_uuid")
    mission_id: str = s.StringColumn("mission_id")
    metadata: dict = s.JsonColumn("metadata")
    start_date: datetime.datetime = s.DateTimeColumn("start_date")
    end_date: t.Optional[datetime.datetime] = s.DateTimeColumn("end_date")

    @classmethod
    def find_by_uuid(cls, db, mission_uuid: str, **kwargs) -> t.Optional[NODBMission]:
        """Find a workflow by name."""
        return db.load_object(cls, {"mission_uuid": mission_uuid},  **kwargs)

    @staticmethod
    def search(db: NODBControllerInstance, mission_id: t.Optional[str] = None, **kwargs):
        if mission_id is None:
            return []
        else:
            yield from db.stream_objects(NODBMission, {'mission_id': mission_id}, **kwargs)


class NODBObservation(s.NODBBaseObject):
    """Represents an archived observation in the database.

        In particular, this table/class represents the characteristics of data records
        that are usually searchable. The actual record is stored as an NODBObservationData.
    """

    TABLE_NAME = "nodb_obs"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = s.UUIDColumn("obs_uuid")
    received_date: datetime.date = s.DateColumn("received_date")

    platform_uuid: t.Optional[str] = s.UUIDColumn("platform_uuid")
    mission_uuid: t.Optional[str] = s.UUIDColumn("mission_uuid")
    source_name: str = s.StringColumn("source_name")
    program_name: str = s.StringColumn("program_name")
    obs_time: t.Optional[datetime.datetime] = s.DateTimeColumn("obs_time")
    min_depth: t.Optional[float] = s.FloatColumn("min_depth")
    max_depth: t.Optional[float] = s.FloatColumn("max_depth")
    location: str = s.WKTColumn("location")
    observation_type: ObservationType = s.EnumColumn("observation_type", ObservationType)
    surface_parameters: list = s.JsonColumn("surface_parameters")
    profile_parameters: list = s.JsonColumn("profile_parameters")
    processing_level: ProcessingLevel = s.EnumColumn("processing_level", ProcessingLevel)
    embargo_date: datetime.datetime = s.DateTimeColumn("embargo_date")

    def update_from_record(self, record: ocproc2.ParentRecord):
        self.program_name = record.metadata.best('CNODCProgram', None)
        self.source_name = record.metadata.best('CNODCSource', None)
        self.mission_uuid = record.metadata.best('CNODCMission', None)
        self.platform_uuid = record.metadata.best('CNODCPlatform', None)
        if record.metadata.has_value('CNODCEmbargoUntil'):
            self.embargo_date = datetime.datetime.fromisoformat(record.metadata.best('CNODCEmbargoUntil'))
        if record.coordinates.has_value('Time'):
            self.obs_time = datetime.datetime.fromisoformat(record.coordinates.best('Time'))
        if record.coordinates.has_value('Time') and record.coordinates['Time'].is_iso_datetime():
            self.obs_time = record.coordinates['Time'].to_datetime()
        if record.coordinates.has_value('Latitude') and record.coordinates['Latitude'].is_numeric() and record.coordinates.has_value('Longitude') and record.coordinates['Longitude'].is_numeric():
            lat = record.coordinates['Latitude'].to_float()
            lon = record.coordinates['Longitude'].to_float()
            self.location = f"POINT ({round(lon, 5)} {round(lat, 5)})"
        level = record.metadata.best('CNODCLevel', None)
        if hasattr(ProcessingLevel, level):
            self.processing_level = getattr(ProcessingLevel, level)
        else:
            self.processing_level = ProcessingLevel.UNKNOWN
        self.surface_parameters = list(set(x for x in record.parameters))
        ref_info = {
            'profile_parameters': set(),
            'surface_parameters': set(),
            'min_depth': None,
            'max_depth': None
        }
        NODBObservation._extract_subrecord_info(record, ref_info)
        self.profile_parameters = list(ref_info['profile_parameters'])
        self.surface_parameters = list(ref_info['surface_parameters'])
        self.min_depth = ref_info['min_depth']
        self.max_depth = ref_info['max_depth']
        if self.location is None or self.obs_time is None:
            self.observation_type = ObservationType.OTHER
        elif self.min_depth is not None and self.min_depth > 0:
            self.observation_type = ObservationType.AT_DEPTH
        elif (self.min_depth is None or self.min_depth == 0) and (self.max_depth is None or self.max_depth == 0):
            self.observation_type = ObservationType.SURFACE
        else:
            self.observation_type = ObservationType.PROFILE

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], **kwargs):
        """Find an observation by UUID and received date."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": s.parse_received_date(received_date)
        }, **kwargs)

    @staticmethod
    def _extract_subrecord_info(record: ocproc2.BaseRecord, ref_info: dict, position: dict = None):
        if position is None:
            position = {}
        else:
            position = { x: position[x] for x in position  if position[x] is not None}
        for key in ('Latitude', 'Longitude', 'Depth', 'Pressure'):
            if record.coordinates.has_value(key):
                position[key] = record.coordinates.best(key)

        depth = None
        if 'Depth' in position:
            depth = position['Depth']
        elif 'Pressure' in position and 'Depth' not in position and 'Latitude' in position:
            depth = eos80_depth(position['Pressure'], position['Latitude'])
        if depth is not None:
            if ref_info['min_depth'] is None or ref_info['min_depth'] > depth:
                ref_info['min_depth'] = depth
            if ref_info['max_depth'] is None or ref_info['max_depth'] < depth:
                ref_info['max_depth'] = depth

        if ('Depth' in position and position['Depth'] != 0) or ('Pressure' in position and position['Pressure'] != 0):
            param_key = 'profile_parameters'
        else:
            param_key = 'surface_parameters'
        ref_info[param_key].update(x for x in record.parameters)

        for subrecord in record.iter_subrecords():
            NODBObservation._extract_subrecord_info(subrecord, ref_info, position)



class NODBObservationData(s.NODBBaseObject):
    """Represents the 'meat' of an archived observation; the full record and associated metadata."""

    TABLE_NAME = "nodb_obs_data"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = s.UUIDColumn("obs_uuid")
    received_date: datetime.date = s.DateColumn("received_date")
    source_file_uuid: str = s.StringColumn("source_file_uuid")
    message_idx: int = s.IntColumn("message_idx")
    record_idx: int = s.IntColumn("record_idx")
    data_record: t.Optional[bytes] = s.ByteColumn("data_record")
    process_metadata: dict = s.JsonColumn("process_metadata")
    qc_tests: dict = s.JsonColumn("qc_tests")
    duplicate_uuid: str = s.UUIDColumn("duplicate_uuid")
    duplicate_received_date: datetime.date = s.DateColumn("duplicate_received_date")
    status: ObservationStatus = s.EnumColumn("status", ObservationStatus)
    processing_level: ProcessingLevel = s.EnumColumn("processing_level", ProcessingLevel)

    def get_process_metadata(self, key, default=None):
        """Retrieve metadata information about the record."""
        if self.process_metadata and key in self.process_metadata:
            return self.process_metadata[key]
        return default

    def set_process_metadata(self, key, value):
        """Set metadata about the record."""
        if self.process_metadata is None:
            self.process_metadata = {}
        self.process_metadata[key] = value
        self.modified_values.add('process_metadata')

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], **kwargs):
        """Locate a record by UUID."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": s.parse_received_date(received_date)
        }, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db,
                            source_file_uuid: str,
                            source_received_date: t.Union[str, datetime.date],
                            message_idx: int,
                            record_idx: int,
                            processing_level: t.Optional[str] = None,
                            **kwargs):
        """Locate a record by information about it in the source file."""
        if processing_level is None:
            processing_level = ProcessingLevel.UNKNOWN.value
        filters = {
            "received_date": s.parse_received_date(source_received_date),
            "source_file_uuid": source_file_uuid,
            "message_idx": message_idx,
            "record_idx": record_idx,
            "processing_level": processing_level
        }
        return db.load_object(cls, filters, **kwargs)

    @property
    def record(self) -> t.Optional[ocproc2.ParentRecord]:
        """Extract the data record."""
        return self._with_cache('loaded_record', self._record)

    def _record(self):
        if self.data_record is None:
            return None
        decoder = OCProc2BinCodec()
        records = [x for x in decoder.load_all(self.data_record)]
        return records[0] if records else None

    @record.setter
    def record(self, data_record: ocproc2.ParentRecord):
        """Set the data record."""
        self._cache['loaded_record'] = data_record
        if data_record is None:
            self.data_record = None
            self.mark_modified('data_record')
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
            self.mark_modified('data_record')

    def _update_from_data_record(self, data_record: ocproc2.ParentRecord):
        qc_test_names = set(x.test_name for x in data_record.qc_tests)
        qc_test_info = {}
        for x in qc_test_names:
            best_result = data_record.latest_test_result(x, True)
            qc_test_info[x] = {
                'version': best_result.test_version,
                'date_run': best_result.test_date,
                'result': best_result.result,
            }
        self.qc_tests = qc_test_info
        if data_record.metadata.has_value('CNODCDuplicateId') and data_record.metadata.has_value('CNODCDuplicateDate'):
            self.duplicate_received_date = datetime.date.fromisoformat(data_record.metadata.best('CNODCDuplicateDate'))
            self.duplicate_uuid = data_record.metadata.best('CNODCDuplicateId')
        if data_record.metadata.has_value('CNODCStatus'):
            new_status = data_record.metadata.best('CNODCStatus')
            if hasattr(ObservationStatus, new_status):
                self.status = getattr(ObservationStatus, new_status)
        level = data_record.metadata.best('CNODCLevel', None)
        if hasattr(ProcessingLevel, level):
            self.processing_level = getattr(ProcessingLevel, level)
        else:
            self.processing_level = ProcessingLevel.UNKNOWN


class NODBPlatform(s.NODBBaseObject):

    TABLE_NAME = 'nodb_platforms'
    PRIMARY_KEYS = ('platform_uuid', )

    platform_uuid: str = s.UUIDColumn("platform_uuid")
    wmo_id: str = s.StringColumn("wmo_id")
    wigos_id: str = s.StringColumn("wigos_id")
    platform_name: str = s.StringColumn("platform_name")
    platform_id: str = s.StringColumn("platform_id")
    platform_type: str = s.StringColumn("platform_type")
    service_start_date: datetime.datetime = s.DateTimeColumn("service_start_date")
    service_end_date: datetime.datetime = s.DateTimeColumn("service_end_date")
    instrumentation: dict = s.JsonColumn('instrumentation')
    metadata: dict = s.JsonColumn('metadata')
    map_to_uuid: str = s.UUIDColumn("map_to_uuid")
    status: PlatformStatus = s.EnumColumn("status", PlatformStatus)
    embargo_data_days: int = s.IntColumn('embargo_data_days')

    def get_metadata(self, metadata_key, default=None):
        """Retrieve metadata about a platform."""
        if self.metadata is not None and metadata_key in self.metadata:
            return self.metadata[metadata_key] or default
        return None

    @classmethod
    def search(cls,
               db: NODBControllerInstance,
               in_service_time: t.Optional[datetime.datetime] = None,
               wmo_id: t.Optional[str] = None,
               wigos_id: t.Optional[str] = None,
               platform_id: t.Optional[str] = None,
               platform_name: t.Optional[str] = None,
               **kwargs) -> list[NODBPlatform]:
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
        if in_service_time is not None:
            filters['service_start_date'] = (in_service_time, '<=', True)
            filters['service_end_date'] = (in_service_time, '>=', True)
        if not filters:
            return []
        yield from db.stream_objects(
            obj_cls=NODBPlatform,
            filters=filters,
            filter_type=' OR ',
            **kwargs
        )

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, platform_uuid: str, **kwargs) -> t.Optional[NODBPlatform]:
        """Locate a platform by its unique identifier."""
        return db.load_object(cls, {
            'platform_uuid': platform_uuid
        }, **kwargs)

    @classmethod
    def find_all_raw(cls, db: NODBControllerInstance, **kwargs) -> t.Iterable[dict]:
        """Retrieve all platforms in a raw (i.e. database dictionary) format."""
        yield from db.stream_objects(cls, raw=True, **kwargs)


class NODBWorkingRecord(s.NODBBaseObject):
    """Represents a record currently being processed in the database."""

    TABLE_NAME = "nodb_working"
    PRIMARY_KEYS = ("working_uuid",)

    working_uuid: str = s.UUIDColumn("working_uuid")
    record_uuid: t.Optional[str] = s.UUIDColumn("record_uuid")
    received_date: datetime.date = s.DateColumn("received_date")
    source_file_uuid: str = s.UUIDColumn("source_file_uuid")
    message_idx: int = s.IntColumn("message_idx")
    record_idx: int = s.IntColumn("record_idx")
    data_record: t.Optional[bytes] = s.ByteColumn("data_record")
    qc_metadata: dict = s.JsonColumn("qc_metadata")
    qc_batch_id: str = s.UUIDColumn("qc_batch_id")
    platform_uuid: str = s.UUIDColumn("platform_uuid")
    obs_time: datetime.datetime = s.DateTimeColumn("obs_time")
    location: str = s.WKTColumn("location")

    def set_metadata(self, key, value):
        """Set metadata on the working record"""
        if self.qc_metadata is None:
            self.qc_metadata = {}
        self.qc_metadata[key] = value
        self.mark_modified('qc_metadata')

    def get_metadata(self, key, default=None):
        """Retrieve metadata on the working record."""
        if self.qc_metadata is None or key not in self.qc_metadata:
            return default
        return self.qc_metadata[key]

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, *args, **kwargs):
        """Find a working record by its identifier"""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
        }, *args, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db,
                            source_file_uuid: str,
                            source_received_date: t.Union[str, datetime.date],
                            message_idx: int,
                            record_idx: int,
                            *args, **kwargs):
        """Find a working record by its source information"""
        return db.load_object(cls, {
                "received_date": s.parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, *args, **kwargs)

    @property
    def record(self) -> t.Optional[ocproc2.ParentRecord]:
        """Extract the OCProc2 record."""
        return self._with_cache('loaded_record', self._record)

    def _record(self):
        if self.data_record is None:
            return None
        decoder = OCProc2BinCodec()
        records = [x for x in decoder.load_all(self.data_record)]
        return records[0] if records else None

    @record.setter
    def record(self, data_record: ocproc2.ParentRecord):
        """Set the OCProc2 record."""
        self._cache['loaded_record'] = data_record
        self._update_from_data_record(data_record)
        if data_record is None:
            self.data_record = None
            self.mark_modified('data_record')
        else:
            decoder = OCProc2BinCodec()
            ba = bytearray()
            for byte_ in decoder.encode_records(
                    [data_record],
                    codec='PICKLE',
                    compression='LZMA2',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba
            self.mark_modified('data_record')

    def _update_from_data_record(self, data_record: ocproc2.ParentRecord):
        if data_record.coordinates.has_value('Time') and data_record.coordinates['Time'].is_iso_datetime():
            self.obs_time = data_record.coordinates['Time'].to_datetime()
        if data_record.coordinates.has_value('Latitude') and data_record.coordinates['Latitude'].is_numeric() and data_record.coordinates.has_value('Longitude') and data_record.coordinates['Longitude'].is_numeric():
            lat = data_record.coordinates['Latitude'].to_float()
            lon = data_record.coordinates['Longitude'].to_float()
            self.location = f"POINT ({round(lon, 5)} {round(lat, 5)})"
        self.platform_uuid = data_record.metadata.best('CNODCPlatform', None)

    @staticmethod
    def bulk_set_batch_uuid(
            db: NODBControllerInstance,
            working_uuids: list[str],
            batch_uuid: str
    ):
        db.bulk_update(NODBWorkingRecord, {'qc_batch_id': batch_uuid}, 'working_uuid', working_uuids)


class NODBBatch(s.NODBBaseObject):

    TABLE_NAME = 'nodb_qc_batches'

    batch_uuid: str = s.NODBBaseObject.make_property("batch_uuid", coerce=str)
    qc_metadata: dict = s.NODBBaseObject.make_property("qc_metadata")
    status: BatchStatus = s.EnumColumn("status", BatchStatus)

    def stream_working_records(self, db: NODBControllerInstance, **kwargs):
        yield from db.stream_objects(
            obj_cls=NODBWorkingRecord,
            filters={
                'qc_batch_id': self.batch_uuid,
            },
            **kwargs
        )

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, batch_uuid: str, **kwargs):
        return db.load_object(cls, {
            'batch_uuid': batch_uuid
        }, **kwargs)

    @classmethod
    def count_working_by_uuid(cls, db: NODBControllerInstance, batch_uuid: str) -> int:
        return db.count_objects(NODBWorkingRecord, {'qc_batch_id': batch_uuid})

