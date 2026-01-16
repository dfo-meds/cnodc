import datetime
import uuid
import cnodc.ocproc2 as ocproc2
from cnodc.process.payload_worker import BatchWorkflowWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.process.queue_worker import QueueItemResult
from cnodc.units import UnitConverter
from cnodc.workflow.workflow import BatchPayload


class NODBFinalizeWorker(BatchWorkflowWorker):

    converter: UnitConverter = None

    def __init__(self, **kwargs):
        super().__init__(
            process_name="finalizer",
            process_version="1_0",
            defaults={
                'queue_name': 'nodb_finalize'
            },
            **kwargs
        )

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        batch = payload.load_batch(self._db)
        for record in batch.stream_working_records(self._db):
            self._complete_record(record)
        self._db.delete_object(batch)
        self._current_item.mark_complete(self._db)
        self._db.commit()
        return QueueItemResult.HANDLED

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

    # TODO: the below code should probably be spun out into its own function somewhere in case it needs to be used elsewhere
    def _finalize_record(self, record: ocproc2.ParentRecord, is_top_level: bool = False):
        # TODO: where values are MultiValued, assign a WorkingQuality to the parent MultiValue based on the best
        # value of the actual values
        for key in record.metadata:
            self._finalize_value(record.metadata[key])
        for record in record.iter_subrecords():
            self._finalize_record(record)

    def _finalize_value(self, value: ocproc2.AbstractElement):
        if 'WorkingQuality' in value.metadata:
            value.metadata['Quality'] = value.metadata['WorkingQuality'].best_value()
            del value.metadata['WorkingQuality']
        if isinstance(value, ocproc2.MultiElement):
            for v in value.values():
                self._finalize_value(v)

    def _populate_obs_data_from_record(self, obs_data: structures.NODBObservationData, record: ocproc2.ParentRecord):
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

    def _populate_obs_from_record(self, obs: structures.NODBObservation, record: ocproc2.ParentRecord):
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

    def _extract_subrecord_info(self, record: ocproc2.BaseRecord, ref_info: dict, first: bool = False):
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

    def _extract_observation_time(self, record: ocproc2.BaseRecord):
        iso_time = self._extract_single_value(record.coordinates, 'Time')
        try:
            return datetime.datetime.fromisoformat(iso_time)
        except (ValueError, TypeError):
            self._log.exception(f"An exception occurred while converting a date/time value")
            return None

    def _extract_location(self, record: ocproc2.BaseRecord):
        latitude = self._extract_single_value(record.coordinates, 'Latitude')
        longitude = self._extract_single_value(record.coordinates, 'Longitude')
        if latitude is None or longitude is None:
            return None
        if not isinstance(latitude, (float, int)):
            return None
        if not isinstance(longitude, (float, int)):
            return None
        return f'POINT ({round(longitude, 4)} {round(latitude, 4)})'

    def _extract_single_value(self, value_map: ocproc2.ElementMap, value_name: str, default=None):
        if value_name not in value_map:
            return default
        val = value_map[value_name].ideal_single_value()
        if val.is_empty():
            return default
        if 'Quality' in val.metadata and val.metadata['Quality'].value in (4, 9):
            return default
        return val.value

    def _extract_enum_value(self, enum_type, value_map: ocproc2.ElementMap, value_name: str, default=None, default_error=None):
        bv = value_map.best_value(value_name, None)
        if bv is None:
            return default
        try:
            return enum_type(bv).value
        except ValueError:
            self._log.exception(f"An error occurred while extracting an enum value")
            return default_error
