import datetime
import uuid

from cnodc.ocproc2 import DataRecord, ValueMap
from cnodc.ocproc2.structures import AbstractValue, MultiValue, Value
from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController
from autoinject import injector

from cnodc.units import UnitConverter
from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import BatchPayload


class NODBProcessWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_processor", **kwargs)
        self._processor: t.Optional[NODBProcessor] = None

    def on_start(self):
        self._processor = NODBProcessor(
            processor_uuid=self.process_uuid
        )

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._processor.process_queue_item(item)


class NODBProcessor(PayloadProcessor):

    nodb: NODBController = None
    converter: UnitConverter = None

    NAME = 'nodb_processor'
    VERSION = '1.0'

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            processor_version=NODBProcessor.NAME,
            processor_name=NODBProcessor.VERSION,
            require_type=BatchPayload,
            **kwargs
        )

    def _process(self):
        workflow = self.load_workflow_controller()
        if workflow is None or not workflow.has_more_steps(self._current_payload.current_step):
            self._complete_batch()
        else:
            next_payload = self.create_batch_payload(self._current_payload.batch_uuid, True)
            workflow.queue_step(
                next_payload,
                unique_key=self._current_item.unique_item_name,
                db=self._db
            )

    def _complete_batch(self):
        batch = self.load_batch_from_payload()
        for record in batch.stream_working_records(self._db):
            self._complete_record(record)
        self._db.delete_object(batch)
        self._db.commit()

    def _complete_record(self, working: structures.NODBWorkingRecord):
        record = working.record
        self._finalize_record(record, True)
        obs_data = structures.NODBObservationData()
        obs_data.obs_uuid = str(uuid.uuid4())
        obs_data.received_date = working.received_date
        record.metadata['CNODCID'] = f"{obs_data.received_date.strftime('%Y%m%d')}/{obs_data.obs_uuid}"
        obs_data.message_idx = working.message_idx
        obs_data.record_idx = working.record_idx
        obs_data.source_file_uuid = working.source_file_uuid
        obs_data.record = record
        self._populate_obs_data_from_record(obs_data, record)

        obs = structures.NODBObservation()
        obs.obs_uuid = obs_data.obs_uuid
        obs.received_date = obs_data.received_date
        self._populate_obs_from_record(obs, record)

        self._db.insert_object(obs)
        self._db.insert_object(obs_data)
        self._db.delete_object(working)
        self._db.commit()

    def _finalize_record(self, record: DataRecord, is_top_level: bool = False):
        for key in record.metadata:
            self._finalize_value(record.metadata[key])
        for record in record.iter_subrecords():
            self._finalize_record(record)

    def _finalize_value(self, value: AbstractValue):
        if 'WorkingQuality' in value.metadata:
            value.metadata['Quality'] = value.metadata['WorkingQuality'].best_value()
            del value.metadata['WorkingQuality']
        if isinstance(value, MultiValue):
            for v in value.values():
                self._finalize_value(v)

    def _populate_obs_data_from_record(self, obs_data: structures.NODBObservationData, record: DataRecord):
        obs_data.status = self._extract_enum_value(structures.ObservationStatus, record.metadata, 'CNODCStatus', 'VERIFIED')
        qc_test_names = set(x.test_name for x in record.qc_tests)
        qc_test_info = {}
        for x in qc_test_names:
            best_result = record.latest_test_result(x, True)
            qc_test_info[x] = {
                'version': best_result.test_version,
                'date_run': best_result.test_date,
                'result': best_result.result,
            }
        obs_data.qc_tests = qc_test_info
        if record.metadata.has_value('CNODCDuplicateId') and record.metadata.has_value('CNODCDuplicateDate'):
            try:
                obs_data.duplicate_received_date = datetime.date.fromisoformat(record.metadata.best_value('CNODCDuplicateDate'))
                obs_data.duplicate_uuid = record.metadata.best_value('CNODCDuplicateId')
            except (ValueError, TypeError) as ex:
                self._log.exception(f"Exception while processing duplicate info for {obs_data.obs_uuid}")

    def _populate_obs_from_record(self, obs: structures.NODBObservation, record: DataRecord):
        obs.program_name = record.metadata.best_value('CNODCProgram', None)
        obs.instrument_type = record.metadata.best_value('CNODCInstrumentType', None)
        obs.source_name = record.metadata.best_value('CNODCSource', None)
        obs.mission_name = record.metadata.best_value('CNODCMission', None)
        obs.station_uuid = record.metadata.best_value('CNODCStation', None)
        obs.processing_level = self._extract_enum_value(structures.ProcessingLevel, record.metadata, 'CNODCLevel')
        if record.metadata.has_value('CNODCEmbargoUntil'):
            obs.embargo_date = datetime.datetime.fromisoformat(record.metadata['CNODCEmbargoUntil'].best_value())
        obs.obs_time = self._extract_observation_time(record)
        obs.location = self._extract_location(record)
        obs.surface_parameters = list(set(x for x in record.parameters))
        ref_info = {'min_depth': None, 'max_depth': None, 'profile_params': set()}
        self._extract_subrecord_info(record, ref_info, True)
        obs.profile_parameters = list(ref_info['profile_params'])
        obs.min_depth = ref_info['min_depth']
        obs.max_depth = ref_info['max_depth']
        if obs.location is None or obs.obs_time is None:
            obs.observation_type = structures.ObservationType.OTHER
        elif record.coordinates.has_value('Depth'):
            obs.observation_type = structures.ObservationType.AT_DEPTH
        elif obs.min_depth is None and obs.max_depth is None:
            obs.observation_type = structures.ObservationType.SURFACE
        else:
            obs.observation_type = structures.ObservationType.PROFILE

    def _extract_subrecord_info(self, record: DataRecord, ref_info: dict, first: bool = False):
        if not first:
            ref_info['profile_params'].update(x for x in record.parameters)
        if record.coordinates.has_value('Depth'):
            for val in record.coordinates['Depth'].all_values():
                if (not val.is_empty()) and val.is_numeric():
                    depth_in_m = self._convert_to_unit(val.value, val.metadata.best_value('Units'), 'm')
                    if depth_in_m is None:
                        continue
                    if ref_info['min_depth'] is None or depth_in_m < ref_info['min_depth']:
                        ref_info['min_depth'] = depth_in_m
                    if ref_info['max_depth'] is None or depth_in_m > ref_info['max_depth']:
                        ref_info['max_depth'] = depth_in_m
        for subrecord in record.iter_subrecords():
            self._extract_subrecord_info(subrecord, ref_info)

    def _convert_to_unit(self, value: t.Union[float, int], source_units: str, target_units: str):
        try:
            if source_units is None:
                return value
            return self.converter.convert(value, source_units, target_units)
        except ValueError:
            self._log.exception(f"An exception occurred while converting a value from {source_units} to {target_units}")
            return None

    def _extract_observation_time(self, record: DataRecord):
        iso_time = self._extract_single_value(record.coordinates, 'Time')
        try:
            return datetime.datetime.fromisoformat(iso_time)
        except (ValueError, TypeError):
            self._log.exception(f"An exception occurred while converting a date/time value")
            return None

    def _extract_location(self, record: DataRecord):
        latitude = self._extract_single_value(record.coordinates, 'Latitude')
        longitude = self._extract_single_value(record.coordinates, 'Longitude')
        if latitude is None or longitude is None:
            return None
        if not isinstance(latitude, (float, int)):
            return None
        if not isinstance(longitude, (float, int)):
            return None
        return f'POINT ({round(longitude, 4)} {round(latitude, 4)})'

    def _extract_single_value(self, value_map: ValueMap, value_name: str, default=None):
        if value_name not in value_map:
            return default
        val = value_map[value_name]
        if isinstance(val, Value):
            if val.is_empty():
                return default
            if 'Quality' in val.metadata and val.metadata['Quality'].value in (4, 9):
                return default
            return val.value
        return default

    def _extract_enum_value(self, enum_type, value_map: ValueMap, value_name: str, default=None, default_error=None):
        bv = value_map.best_value(value_name, None)
        if bv is None:
            return default
        try:
            return enum_type(bv).value
        except ValueError:
            self._log.exception(f"An error occurred while extracting an enum value")
            return default_error
