import datetime
import logging
import uuid

from autoinject import injector

from cnodc import ocproc2 as ocproc2
from cnodc.nodb.observations import NODBWorkingRecord, NODBObservationData, NODBObservation, NODBPlatform, NODBMission
from cnodc.ocproc2.ontology import OCProc2Ontology
from cnodc.science.units import UnitConverter


class NODBRecordManager:

    converter: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self):
        self._log = logging.getLogger("cnodc.nodb.record_manager")

    def create_completed_entry(self, db, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int, memory: dict):
        check = NODBObservationData.find_by_source_info(
            db, source_file_uuid, received_date, message_idx, record_idx, record.metadata.best('CNODCLevel', 'UNKNOWN'), key_only=True
        )
        if check is not None:
            return False
        self._prune_platform_metadata(db, record, memory)
        self._prune_mission_metadata(db, record, memory)
        obs, obs_data = self.build_nodb_entry(record, source_file_uuid, received_date, message_idx, record_idx)
        db.insert_object(obs)
        db.insert_object(obs_data)
        return True

    def _prune_platform_metadata(self, db, record: ocproc2.ParentRecord, memory: dict):
        self._prune_metadata(
            db,
            record,
            memory,
            'CNODCPlatform',
            'metadata:platform',
            NODBPlatform.find_by_uuid
        )

    def _prune_mission_metadata(self, db, record: ocproc2.ParentRecord, memory: dict):
        self._prune_metadata(
            db,
            record,
            memory,
            'CNODCMission',
            'metadata:mission',
            NODBMission.find_by_uuid
        )

    def _prune_metadata(self, db, record: ocproc2.ParentRecord, memory, lookup_key_name, group_name, finder: callable):
        if not record.metadata.has_value(lookup_key_name):
            return
        obj_uuid = record.metadata[lookup_key_name].value
        if lookup_key_name not in memory:
            memory[lookup_key_name] = {}
        mem = memory[lookup_key_name]
        obj = None
        if obj_uuid not in mem:
            obj = finder(db, obj_uuid)
            mem[obj_uuid] = (obj.metadata or {}) if obj else None
        if mem[obj_uuid] is None:
            return
        unloaded = mem[obj_uuid]
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
            if obj is None:
                obj = finder(db, obj_uuid)
            obj.metadata = unloaded
            db.update_object(obj)

    def create_working_entry(self, db, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int):
        check = NODBWorkingRecord.find_by_source_info(
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
                         record_idx: int) -> tuple[NODBObservation, NODBObservationData]:
        self.finalize(record, True)
        obs_data = NODBObservationData()
        obs_data.obs_uuid = str(uuid.uuid4())
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
