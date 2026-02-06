import datetime
import logging
import typing as t
import uuid

from autoinject import injector

from cnodc import ocproc2 as ocproc2
from cnodc.nodb import structures as structures
from cnodc.units import UnitConverter


class NODBRecordManager:

    converter: UnitConverter = None

    @injector.construct
    def __init__(self):
        self._log = logging.getLogger("cnodc.nodb.record_manager")

    def create_completed_entry(self, db, record: ocproc2.ParentRecord, source_file_uuid: str, received_date: datetime.date, message_idx: int, record_idx: int):
        check = structures.NODBObservationData.find_by_source_info(
            db, source_file_uuid, received_date, message_idx, record_idx, record.metadata.best_value('CNODCLevel', 'UNKNOWN'), key_only=True
        )
        if check is not None:
            return False
        obs, obs_data = self.build_nodb_entry(record, source_file_uuid, received_date, message_idx, record_idx)
        db.insert_object(obs)
        db.insert_object(obs_data)
        return True

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
