import datetime
import functools
import logging
import uuid
import typing as t
from collections import defaultdict

from autoinject import injector

from medsutil import ocproc2 as ocproc2
from medsutil.awaretime import AwareDateTime
from nodb.observations import NODBSourceFile, NODBWorkingRecord, NODBObservationData, NODBObservation, NODBPlatform, NODBMission
from nodb.interface import NODBInstance, LockType
from medsutil.ocproc2 import OCProc2Ontology
from medsutil.units import UnitConverter


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
                                                source_file: NODBSourceFile):
        return self.create_completed_entry(
            record=record,
            message_idx=message_idx,
            record_idx=record_idx,
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date
        )
    def create_completed_entry_from_working_record(self, working: NODBWorkingRecord):
        return self.create_completed_entry(
            record=working.record,
            message_idx=working.message_idx,
            record_idx=working.record_idx,
            source_file_uuid=working.source_file_uuid,
            received_date=working.received_date
        )

    def create_working_entry_from_source_file(self,
                                              record: ocproc2.ParentRecord,
                                              message_idx: int,
                                              record_idx: int,
                                              source_file: NODBSourceFile):
        return self.create_working_entry(
            record=record,
            message_idx=message_idx,
            record_idx=record_idx,
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date
        )

    def _check_completed_entry(self,
                               source_file_uuid: str,
                               received_date: datetime.date,
                               message_idx: int,
                               record_idx: int,
                               cnodc_level: str):
        key = f"{source_file_uuid}__{received_date.isoformat()}"
        if 'completed_entries_by_file' not in self._memory:
            self._memory['completed_entries_by_file'] = {}
        if key not in self._memory['completed_entries_by_file']:
            self._memory['completed_entries_by_file'][key] = self._load_all_completed(source_file_uuid, received_date)
        return self._memory['completed_entries_by_file'][key][(message_idx, record_idx, cnodc_level)]

    def _load_all_completed(self, source_file_uuid: str, received_date: datetime.date) -> dict[tuple[int, int, str], bool]:
        result: dict[tuple[int, int, str], bool] = defaultdict(lambda: False)
        for row in NODBObservationData.find_all_by_source_file_raw(self._db, source_file_uuid, received_date, limit_fields=["message_idx", "record_idx", "processing_level"]):
            result[(int(row["message_idx"]), int(row["record_idx"]), row["processing_level"])] = True
        return result

    def create_completed_entry(self, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int, original_uuid: str = None):
        if self._check_completed_entry(source_file_uuid, received_date, message_idx, record_idx, record.metadata.best('CNODCLevel', coerce=str, default='UNKNOWN')):
            return False
        self._identify_platform(record)
        self._prune_platform_metadata(record)
        self._identify_mission(record)
        self._prune_mission_metadata(record)
        obs, obs_data = self.build_nodb_entry(record, source_file_uuid, received_date, message_idx, record_idx, original_uuid)
        self._prep_obs.execute(obs)
        self._prep_obs_data.execute(obs_data)
        return True

    def _identify_platform(self, record: ocproc2.ParentRecord):
        if record.metadata.has_value('CNODCPlatform'):
            return
        wmo_id = record.metadata.best('WMOID', coerce=str, default=None)
        wigos_id = record.metadata.best('WIGOSID', coerce=str, default=None)
        platform_name = record.metadata.best('PlatformName', coerce=str, default=None)
        platform_id = record.metadata.best('PlatformID', coerce=str, default=None)
        if not (wmo_id or platform_name or platform_id or wigos_id):
            return
        in_service_date = record.coordinates.best('Time', coerce=AwareDateTime.fromisoformat, default=None)
        platform = self._find_platform(wmo_id=wmo_id, in_service_date=in_service_date, wigos_id=wigos_id, platform_name=platform_name, platform_id=platform_id)
        if platform is None:
            platform = NODBPlatform()
            platform.platform_uuid = str(uuid.uuid4())
            platform.wmo_id = wmo_id
            platform.wigos_id = wigos_id
            platform.platform_name = platform_name
            platform.platform_id = platform_id
            platform.platform_type = record.metadata.best("PlatformCNODCType", coerce=str, default="unknown")
            platform.service_start_date = record.metadata.best("PlatformServiceStart", coerce=AwareDateTime, default=None)
            platform.service_end_date = record.metadata.best("PlatformServiceEnd", coerce=AwareDateTime, default=None)
            self._db.insert_object(platform)
            self._memory['CNODCPlatform'][platform.platform_uuid] = platform
        record.metadata['CNODCPlatform'] = platform.platform_uuid

    def _find_platform(self,
                       wmo_id: str | None = None,
                       wigos_id: str | None = None,
                       platform_name: str | None = None,
                       platform_id: str | None = None,
                       in_service_date: AwareDateTime | None = None) -> NODBPlatform | None:
        if 'CNODCPlatform' in self._memory:
            for platform_uuid, platform in self._memory['CNODCPlatform'].items():
                platform: NODBPlatform = platform
                if in_service_date is not None:
                    if platform.service_start_date is not None and platform.service_start_date > in_service_date:
                        continue
                    if platform.service_end_date is not None and platform.service_end_date < in_service_date:
                        continue
                if wmo_id is not None and platform.wmo_id == wmo_id:
                    return platform
                elif wigos_id is not None and platform.wigos_id == wigos_id:
                    return platform
                elif platform_name is not None and platform.platform_name == platform_name:
                    return platform
                elif platform_id is not None and platform.platform_id == platform_id:
                    return platform
        else:
            self._memory['CNODCPlatform'] = {}
        for platform in NODBPlatform.search(db=self._db, wmo_id=wmo_id, limit_fields=("metadata", 'wmo_id', 'wigos_id', 'platform_name', 'platform_id', 'service_start_date', 'service_end_date', 'map_to_uuid',)):
            self._memory['CNODCPlatform'][platform.platform_uuid] = platform
            return platform
        return None


    def _identify_mission(self, record: ocproc2.ParentRecord):
        ...

    """
                platform = nodb.NODBPlatform()
                platform.platform_type = 'glider'
                platform.platform_uuid = str(uuid.uuid4())
                platform.wmo_id = wmoid
                if record.metadata.has_value('PlatformName'):
                    platform.platform_name = record.metadata['PlatformName'].to_string()
                if record.metadata.has_value('PlatformID'):
                    platform.platform_id = record.metadata['PlatformID'].to_string()
                db.insert_object(platform)
                memory['platform_map'][wmoid] = platform.platform_uuid
                record.metadata['CNODCPlatform'] = platform.platform_uuid
    if record.metadata.has_value('CruiseID'):
        cruise_id = record.metadata['CruiseID'].to_string()
        if cruise_id in memory['mission_map']:
            record.metadata['CNODCMission'] = memory['mission_map'][cruise_id]
        else:
            missions = [x for x in nodb.NODBMission.search(
                db=db,
                mission_id=cruise_id,
            )]
            if missions:
                record.metadata['CNODCMission'] = missions[0].mission_uuid
                memory['mission_map'][cruise_id] = missions[0].mission_uuid
            else:
                mission = nodb.NODBMission()
                mission.mission_id = cruise_id
                mission.mission_uuid = str(uuid.uuid4())
                db.insert_object(mission)
                memory['mission_map'][cruise_id] = mission.mission_uuid
                record.metadata['CNODCMission'] = mission.mission_uuid
    """

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

    def create_working_entry(self, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int):
        check = NODBWorkingRecord.find_by_source_info(
            self._db, source_file_uuid, received_date, message_idx, record_idx, key_only=True
        )
        if check is not None:
            return False
        working_record = self.build_nodb_working_entry(record, source_file_uuid, received_date, message_idx, record_idx)
        self._db.insert_object(working_record)
        return True

    def build_nodb_working_entry(self,
                                 record: ocproc2.ParentRecord,
                                 source_file_uuid: str,
                                 received_date: datetime.date,
                                 message_idx: int,
                                 record_idx: int) -> NODBWorkingRecord:
        working_record = NODBWorkingRecord()
        working_record.working_uuid = str(uuid.uuid4())
        working_record.received_date = received_date
        working_record.message_idx = message_idx
        working_record.record_idx = record_idx
        working_record.source_file_uuid = source_file_uuid
        working_record.record = record
        return working_record

    def build_nodb_entry(self,
                         record: ocproc2.ParentRecord,
                         source_file_uuid: str,
                         received_date: datetime.date,
                         message_idx: int,
                         record_idx: int,
                         original_uuid: str = None) -> tuple[NODBObservation, NODBObservationData]:
        self.finalize(record, True)
        obs_data = NODBObservationData()
        obs_data.obs_uuid = original_uuid or str(uuid.uuid4())
        obs_data.received_date = received_date
        record.metadata['CNODCID'] = f"{obs_data.received_date.strftime('%Y%m%d')}/{obs_data.obs_uuid}"
        obs_data.message_idx = message_idx
        obs_data.record_idx = record_idx
        obs_data.source_file_uuid = source_file_uuid
        obs_data.record = record

        obs = NODBObservation()
        obs.obs_uuid = obs_data.obs_uuid
        obs.received_date = obs_data.received_date
        obs.update_from_record(record)

        return obs, obs_data

    def finalize(self, record: ocproc2.BaseRecord, is_top_level: bool = True):
        if is_top_level:
            if not record.metadata.has_value('CNODCLevel'):
                record.metadata.set('CNODCLevel', 'UNKNOWN')
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
