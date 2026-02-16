import datetime
import logging
import typing as t
import uuid

from autoinject import injector

from cnodc import ocproc2 as ocproc2
from cnodc.nodb import structures as structures
from cnodc.ocproc2.ontology import OCProc2Ontology
from cnodc.units import UnitConverter


class NODBRecordManager:

    converter: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self):
        self._log = logging.getLogger("cnodc.nodb.record_manager")

    def create_completed_entry(self, db, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int, memory: dict):
        check = structures.NODBObservationData.find_by_source_info(
            db, source_file_uuid, received_date, message_idx, record_idx, record.metadata.best_value('CNODCLevel', 'UNKNOWN'), key_only=True
        )
        if check is not None:
            return False
        if record.metadata.has_value('CNODCPlatform'):
            self._prune_platform_metadata(db, record, memory)
        if record.metadata.has_value('CNODCMission'):
            self._prune_mission_metadata(db, record, memory)
        obs, obs_data = self.build_nodb_entry(record, source_file_uuid, received_date, message_idx, record_idx)
        db.insert_object(obs)
        db.insert_object(obs_data)
        return True

    def _prune_platform_metadata(self, db, record: ocproc2.ParentRecord, memory: dict):
        if not record.metadata.has_value('CNODCPlatform'):
            return
        platform_uuid = record.metadata['CNODCPlatform'].value
        if 'platform_info' not in memory:
            memory['platform_info'] = {}
        platform = None
        if platform_uuid not in memory['platform_info']:
            platform = structures.NODBPlatform.find_by_uuid(db, platform_uuid)
            if platform:
                memory['platform_info'][platform_uuid] = (platform.metadata, {})
            else:
                memory['platform_info'][platform_uuid] = None
        if memory['platform_info'][platform_uuid] is None:
            return
        unloaded, loaded = memory['platform_info'][platform_uuid]
        changed = False
        for element_name in record.metadata.keys():
            element_group = self.ontology.element_group(element_name)
            if element_group == 'metadata:platform':
                if element_name not in unloaded:
                    unloaded[element_name] = record.metadata[element_name].to_mapping()
                    changed = True
                    loaded[element_name] = record.metadata[element_name]
                    del record.metadata[element_name]
                else:
                    if element_name not in loaded:
                        loaded[element_name] = ocproc2.AbstractElement.build_from_mapping(unloaded[element_name])
                    if loaded[element_name] == record.metadata[element_name]:
                        del record.metadata[element_name]
        if changed:
            if platform is None:
                platform = structures.NODBPlatform.find_by_uuid(db, platform_uuid)
            platform.metadata = unloaded
            db.update_object(platform)


    def _prune_mission_metadata(self, db, record: ocproc2.ParentRecord, memory: dict):
        if not record.metadata.has_value('CNODCMission'):
            return
        mission_uuid = record.metadata['CNODCMission'].value
        if 'mission_info' not in memory:
            memory['mission_info'] = {}
        mission = None
        if mission_uuid not in memory['mission_info']:
            mission = structures.NODBMission.find_by_uuid(db, mission_uuid)
            if mission:
                memory['mission_info'][mission_uuid] = (mission.metadata, {})
            else:
                memory['mission_info'][mission_uuid] = None
        if memory['mission_info'][mission_uuid] is None:
            return
        unloaded, loaded = memory['mission_info'][mission_uuid]
        changed = False
        for element_name in record.metadata.keys():
            element_group = self.ontology.element_group(element_name)
            if element_group == 'metadata:mission':
                if element_name not in unloaded:
                    unloaded[element_name] = record.metadata[element_name].to_mapping()
                    changed = True
                    loaded[element_name] = record.metadata[element_name]
                    del record.metadata[element_name]
                else:
                    if element_name not in loaded:
                        loaded[element_name] = ocproc2.AbstractElement.build_from_mapping(unloaded[element_name])
                    if loaded[element_name] == record.metadata[element_name]:
                        del record.metadata[element_name]
        if changed:
            if mission is None:
                mission = structures.NODBMission.find_by_uuid(db, mission_uuid)
            mission.metadata = unloaded
            db.update_object(mission)

    def create_working_entry(self, db, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int):
        check = structures.NODBWorkingRecord.find_by_source_info(
            db, source_file_uuid, received_date, message_idx, record_idx, key_only=True
        )
        if check is not None:
            return False
        working_record = self.build_nodb_working_entry(record, source_file_uuid, received_date, message_idx, record_idx)
        db.insert_object(working_record)
        return True

    def build_nodb_working_entry(self,
                                 record: ocproc2.ParentRecord,
                                 source_file_uuid: str,
                                 received_date: datetime.date,
                                 message_idx: int,
                                 record_idx: int) -> structures.NODBWorkingRecord:
        working_record = structures.NODBWorkingRecord()
        working_record.working_uuid = str(uuid.uuid4())
        working_record.received_date = received_date
        working_record.message_idx = message_idx
        working_record.record_idx = record_idx
        working_record.source_file_uuid = source_file_uuid
        self._populate_working_observation_data(working_record, record)
        return working_record

    def _populate_working_observation_data(self, working_record: structures.NODBWorkingRecord, record: ocproc2.ParentRecord):
        working_record.record = record

    def build_nodb_entry(self,
                         record: ocproc2.ParentRecord,
                         source_file_uuid: str,
                         received_date: datetime.date,
                         message_idx: int,
                         record_idx: int) -> tuple[structures.NODBObservation, structures.NODBObservationData]:
        self.finalize(record, True)
        obs_data = structures.NODBObservationData()
        obs_data.obs_uuid = str(uuid.uuid4())
        obs_data.received_date = received_date
        record.metadata['CNODCID'] = f"{obs_data.received_date.strftime('%Y%m%d')}/{obs_data.obs_uuid}"
        obs_data.message_idx = message_idx
        obs_data.record_idx = record_idx
        obs_data.source_file_uuid = source_file_uuid
        obs_data.record = record

        obs = structures.NODBObservation()
        obs.obs_uuid = obs_data.obs_uuid
        obs.received_date = obs_data.received_date

        return obs, obs_data

    def finalize(self, record: ocproc2.BaseRecord, is_top_level: bool = True):
        if is_top_level:
            if not record.metadata.has_value('CNODCLevel'):
                record.metadata.set_element('CNODCLevel', 'UNKNOWN')
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
            value.metadata['Quality'] = value.metadata['WorkingQuality'].best_value()
            del value.metadata['WorkingQuality']
        if isinstance(value, ocproc2.MultiElement):
            for v in value.values():
                self._finalize_value(v)
