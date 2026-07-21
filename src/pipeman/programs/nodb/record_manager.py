import datetime
import enum
import functools
import logging
import uuid
import typing as t
from collections import defaultdict

from autoinject import injector

from medsutil import ocproc2 as ocproc2
from medsutil.awaretime import AwareDateTime
from nodb.observations import NODBSourceFile, NODBWorkingRecord, NODBObservationData, NODBObservation, NODBPlatform, \
    NODBMission, DataMode, NODBObservationRelationship, ObservationRelationshipType
from nodb.interface import NODBInstance, LockType
from medsutil.ocproc2 import OCProc2Ontology
from medsutil.units import UnitConverter
from pipeman.programs.dedupe.dedupe import RelationshipAction


class CreationResultType(enum.Enum):
    NEW = 'N'
    UPDATE = 'U'
    DUPLICATE = 'D'
    COPY_EXISTS = 'C'
    MERGE = 'M'


class NODBCreationResult:
    def __init__(self,
                 obs_uuid: str,
                 received_date: datetime.date,
                 dupe_action: CreationResultType,
                 other_items: dict[RelationshipAction, set[tuple[str, datetime.date]]]):
        self.obs_uuid = obs_uuid
        self.received_date = received_date
        self.action = dupe_action
        self.merge_with = set()
        if RelationshipAction.MERGE in other_items:
            self.merge_with.update(other_items[RelationshipAction.MERGE])
        if RelationshipAction.REVIEW_MERGE in other_items:
            self.merge_with.update(other_items[RelationshipAction.REVIEW_MERGE])
        self.relationships = {
            k: v
            for k, v in other_items.items()
            if k not in (RelationshipAction.REVIEW_MERGE, RelationshipAction.MERGE)
        }



class NODBRecordManager:

    converter: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self, db: NODBInstance):
        self._log = logging.getLogger("cnodc.nodb.record_manager")
        self._db = db
        self._prep_obs_data = NODBObservationData.prepare_insert(self._db, name="rm_insert_obs_data")
        self._prep_obs = NODBObservation.prepare_insert(self._db, name="rm_insert_obs")
        self._memory = {}

    def __enter__(self):
        self._prep_obs.__enter__()
        self._prep_obs_data.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._prep_obs_data.__exit__(exc_type, exc_val, exc_tb)
        self._prep_obs.__exit__(exc_type, exc_val, exc_tb)
        self._memory = {}

    def create_completed_entry_from_source_file(self,
                                                record: ocproc2.ParentRecord,
                                                message_idx: int,
                                                record_idx: int,
                                                source_file: NODBSourceFile,
                                                data_mode: DataMode,
                                                quality_flags: int = 0) -> NODBCreationResult:
        return self.create_completed_entry(
            record=record,
            message_idx=message_idx,
            record_idx=record_idx,
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            data_mode=data_mode,
            quality_flags=quality_flags
        )

    def create_completed_entry_from_working_record(self, working: NODBWorkingRecord) -> NODBCreationResult:
        return self.create_completed_entry(
            record=working.record,
            message_idx=working.message_idx,
            record_idx=working.record_idx,
            source_file_uuid=working.source_file_uuid,
            received_date=working.received_date,
            original_uuid=working.working_uuid,
            data_mode=working.data_mode,
            quality_flags=working.quality_checks
        )

    def _check_completed_entry(self,
                               source_file_uuid: str,
                               received_date: datetime.date,
                               message_idx: int,
                               record_idx: int,
                               data_mode: DataMode,
                               quality_flags: int) -> tuple[str, str] | None:
        key = f"{source_file_uuid}__{received_date.isoformat()}"
        if 'completed_entries_by_file' not in self._memory:
            self._memory['completed_entries_by_file'] = {}
        if key not in self._memory['completed_entries_by_file']:
            self._memory['completed_entries_by_file'][key] = self._load_all_completed(source_file_uuid, received_date)
        return self._memory['completed_entries_by_file'][key][(message_idx, record_idx, data_mode, quality_flags)]

    def _load_all_completed(self,
                            source_file_uuid: str,
                            received_date: datetime.date) -> dict[tuple[int, int, DataMode, int], tuple[str, str] | None]:
        result: dict[tuple[int, int, DataMode, int], tuple[str, str] | None] = defaultdict(lambda: None)
        for row in NODBObservationData.find_all_by_source_file_raw(self._db, source_file_uuid, received_date, limit_fields=["obs_uuid", "received_date", "message_idx", "record_idx", "data_mode", "quality_checks"]):
            result[(int(row["message_idx"]), int(row["record_idx"]), DataMode(row["data_mode"]), int(row["quality_checks"]))] = (row["obs_uuid"], row["received_date"])
        return result

    def create_completed_entry(self,
                               record: ocproc2.ParentRecord,
                               source_file_uuid: str,
                               received_date: datetime.date,
                               message_idx: int,
                               record_idx: int,
                               data_mode: DataMode,
                               quality_flags: int = 0,
                               original_uuid: str = None,) -> NODBCreationResult:
        check_result = self._check_completed_entry(
            source_file_uuid,
            received_date,
            message_idx,
            record_idx,
            data_mode,
            quality_flags
        )
        if check_result is not None:
            return NODBCreationResult(check_result[0], datetime.date.fromisoformat(check_result[1]), CreationResultType.COPY_EXISTS)
        self._prune_platform_metadata(record)
        self._prune_mission_metadata(record)
        obs, obs_data, result = self.build_nodb_entry(record, source_file_uuid, received_date, message_idx, record_idx, data_mode, quality_flags, original_uuid)
        self._prep_obs.execute(obs)
        self._prep_obs_data.execute(obs_data)
        if result.relationships:
            for key, value in result.relationships.items():
                for obs_uuid, obs_date in value:
                    self.insert_relationship(obs, obs_uuid, obs_date, key)
        return result

    def insert_relationship(self,
                            obs: NODBObservation,
                            other_obs_uuid: str,
                            other_obs_date: datetime.date | str,
                            action: RelationshipAction):
        relationship = None
        match action:
            case RelationshipAction.MARK_DUPLICATE:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=obs.obs_uuid,
                    left_received_date=obs.received_date,
                    right_obs_uuid=other_obs_uuid,
                    right_received_date=other_obs_date,
                    relationship_type=ObservationRelationshipType.IS_DUPLICATE
                )
            case RelationshipAction.MARK_OTHER_DUPLICATE:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=other_obs_uuid,
                    left_received_date=other_obs_date,
                    right_obs_uuid=obs.obs_uuid,
                    right_received_date=obs.received_date,
                    relationship_type=ObservationRelationshipType.IS_DUPLICATE
                )
            case RelationshipAction.MARK_THIS_BETTER:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=obs.obs_uuid,
                    left_received_date=obs.received_date,
                    right_obs_uuid=other_obs_uuid,
                    right_received_date=other_obs_date,
                    relationship_type=ObservationRelationshipType.BETTER_QUALITY
                )
            case RelationshipAction.MARK_OTHER_BETTER:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=other_obs_uuid,
                    left_received_date=other_obs_date,
                    right_obs_uuid=obs.obs_uuid,
                    right_received_date=obs.received_date,
                    relationship_type=ObservationRelationshipType.BETTER_QUALITY
                )
        if relationship is not None and not relationship.exists(self._db):
            self._db.insert_object(relationship)

    def build_nodb_entry(self,
                         record: ocproc2.ParentRecord,
                         source_file_uuid: str,
                         received_date: datetime.date,
                         message_idx: int,
                         record_idx: int,
                         data_mode: DataMode,
                         quality_flags: int = 0,
                         original_uuid: str = None) -> tuple[
            NODBObservation, NODBObservationData, NODBCreationResult
        ]:
        self.finalize(record, True)
        obs_data = NODBObservationData()
        obs_data.obs_uuid = original_uuid or str(uuid.uuid4())
        obs_data.received_date = received_date
        record.metadata['CNODCID'] = f"{obs_data.received_date.strftime('%Y%m%d')}/{obs_data.obs_uuid}"
        obs_data.message_idx = message_idx
        obs_data.record_idx = record_idx
        obs_data.source_file_uuid = source_file_uuid
        obs_data.data_mode = data_mode
        obs_data.quality_checks = quality_flags
        obs_data.record = record

        obs = NODBObservation()
        obs.obs_uuid = obs_data.obs_uuid
        obs.received_date = obs_data.received_date
        obs.data_mode = data_mode
        obs.quality_checks = quality_flags
        obs.update_from_record(record)

        return obs, obs_data, self.check_for_relationships(record, obs_data)

    def check_for_relationships(self,
                                record: ocproc2.ParentRecord,
                                obs_data: NODBObservationData) -> NODBCreationResult:

        result = CreationResultType.NEW
        relationships = {}
        if record.metadata.has_value("CNODCRelationships"):
            relationships = RelationshipAction.decode_action_list(record.metadata["CNODCRelationships"].value)
            if RelationshipAction.MARK_DUPLICATE in relationships:
                result = CreationResultType.DUPLICATE
            elif RelationshipAction.MARK_OTHER_DUPLICATE in relationships:
                result = CreationResultType.UPDATE
            elif RelationshipAction.MERGE in relationships or RelationshipAction.REVIEW_MERGE in relationships:
                result = CreationResultType.MERGE
        return NODBCreationResult(obs_data.obs_uuid, obs_data.received_date, result, relationships)

    def finalize(self, record: ocproc2.BaseRecord, is_top_level: bool = True):
        for key in record.metadata:
            self._finalize_value(record.metadata[key])
        for key in record.parameters:
            self._finalize_value(record.parameters[key])
        for key in record.coordinates:
            self._finalize_value(record.coordinates[key])
        for record in record.iter_subrecords():
            self.finalize(record, False)

    def _finalize_value(self, value: ocproc2.AbstractElement):
        if 'WorkingQuality' in value.metadata:
            value.metadata['Quality'] = value.metadata['WorkingQuality'].best()
            del value.metadata['WorkingQuality']
        if isinstance(value, ocproc2.MultiElement):
            for v in value.values():
                self._finalize_value(v)

    def _prune_platform_metadata(self, record: ocproc2.ParentRecord):
        self._prune_metadata(
            record,
            'CNODCPlatform',
            'metadata:platform',
            functools.partial(NODBPlatform.find_by_uuid, limit_fields=("metadata", 'wmo_id', 'wigos_id', 'platform_name', 'platform_id', 'service_start_date', 'service_end_date', 'map_to_uuid',))
        )

    def _prune_mission_metadata(self, record: ocproc2.ParentRecord):
        self._prune_metadata(
            record,
            'CNODCMission',
            'metadata:mission',
            functools.partial(NODBMission.find_by_uuid, limit_fields=("metadata", 'mission_id',))
        )

    def _get_object_with_cache(self, record: ocproc2.ParentRecord, lookup_key_name: str, finder: t.Callable):
        obj_uuid = record.metadata.best(lookup_key_name, coerce=str, default=None)
        if obj_uuid is None:
            return None, None
        if lookup_key_name not in self._memory:
            self._memory[lookup_key_name] = {}
        if obj_uuid not in self._memory[lookup_key_name]:
            self._memory[lookup_key_name][obj_uuid] = finder(self._db, obj_uuid)
        return obj_uuid, self._memory[lookup_key_name][obj_uuid]

    def _prune_metadata(self, record: ocproc2.ParentRecord, lookup_key_name, group_name, finder: t.Callable):
        obj_uuid, obj = self._get_object_with_cache(record, lookup_key_name, finder)
        if obj is None:
            return
        unloaded = obj.metadata
        changed = False
        remove_keys = []
        for element_name in record.metadata.keys():
            element_group = self.ontology.group_name(element_name)
            if element_group == group_name:
                if element_name not in unloaded:
                    unloaded[element_name] = record.metadata[element_name].to_mapping()
                    changed = True
                    remove_keys.append(element_name)
                elif unloaded[element_name] == record.metadata[element_name].to_mapping():
                    remove_keys.append(element_name)
        for element_name in remove_keys:
            del record.metadata[element_name]
        if changed:
            obj = finder(self._db, obj_uuid, lock_type=LockType.FOR_NO_KEY_UPDATE)
            obj.metadata = unloaded
            self._db.update_object(obj)
            self._memory[lookup_key_name][obj_uuid] = obj

    def create_working_entry_from_source_file(self,
                                              record: ocproc2.ParentRecord,
                                              message_idx: int,
                                              record_idx: int,
                                              source_file: NODBSourceFile,
                                              data_mode: DataMode,
                                              quality_flags: int = 0) -> str | None:
        return self.create_working_entry(
            record=record,
            message_idx=message_idx,
            record_idx=record_idx,
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            data_mode=data_mode,
            quality_flags=quality_flags
        )

    def create_working_entry(self,
                             record: ocproc2.ParentRecord,
                             source_file_uuid: str,
                             received_date: datetime.date,
                             message_idx: int,
                             record_idx: int,
                             data_mode: DataMode,
                             quality_flags: int = 0) -> str | None:
        check = NODBWorkingRecord.find_by_source_info(
            self._db,
            source_file_uuid=source_file_uuid,
            source_received_date=received_date,
            message_idx=message_idx,
            record_idx=record_idx,
            data_mode=data_mode,
            quality_checks=quality_flags,
            key_only=True
        )
        if check is not None:
            return None
        working_record = self.build_nodb_working_entry(
            record=record,
            source_file_uuid=source_file_uuid,
            received_date=received_date,
            message_idx=message_idx,
            record_idx=record_idx,
            data_mode=data_mode,
            quality_flags=quality_flags
        )
        self._db.insert_object(working_record)
        return working_record.working_uuid

    def build_nodb_working_entry(self,
                                 record: ocproc2.ParentRecord,
                                 source_file_uuid: str,
                                 received_date: datetime.date,
                                 message_idx: int,
                                 record_idx: int,
                                 data_mode: DataMode,
                                 quality_flags: int = 0) -> NODBWorkingRecord:
        working_record = NODBWorkingRecord()
        working_record.quality_flags = quality_flags
        working_record.data_mode = data_mode
        working_record.working_uuid = str(uuid.uuid4())
        working_record.received_date = received_date
        working_record.message_idx = message_idx
        working_record.record_idx = record_idx
        working_record.source_file_uuid = source_file_uuid
        working_record.record = record
        return working_record
