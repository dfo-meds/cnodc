
import typing as t

from medsutil.exceptions import CodedError
from pipeman.processing.payload_worker import NewObservationsWorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager, CreationResultType
from pipeman.processing.payloads import NewObservationsPayload

from nodb.observations import NODBBatch, NODBWorkingRecord, DataMode, NODBObservation, NODBObservationData


class StartQCError(CodedError): CODE_SPACE = 'START-QC'


class NODBStartQC(NewObservationsWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="start_qc",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_start_qc',
            'next_queue': 'workflow_continue',
            'qc_workflow_name': None,
            'output_data_mode': '??',
            'output_quality_checks': 0,
            'start_from_best_copy': True
        })

    def skip_record(self, cr_type: CreationResultType) -> bool:
        match cr_type:
            case CreationResultType.MERGE:
                return True
            case CreationResultType.NEW:
                return False
            case CreationResultType.UPDATE:
                return False
            case CreationResultType.COPY_EXISTS:
                return False
            case CreationResultType.DUPLICATE:
                return False
            case _:
                raise ValueError("Unrecognized creation result type")

    def process_payload(self, payload: NewObservationsPayload) -> t.Optional[QueueItemResult]:
        try:
            dm = DataMode(self.get_config("output_data_mode", "??"))
        except ValueError as ex:
            raise StartQCError("Invalid data mode specified", 1000)
        qf = int(self.get_config("output_quality_checks", 0))
        wf_controller = self.get_workflow(self.get_config('qc_workflow_name', None))
        start_from_best = self.get_config('start_from_best_copy', True)
        with NODBRecordManager(self.db) as rm:
            working_uuids = []
            for obs_uuid, obs_date, cr_type in payload.stream_observation_info():
                if self.skip_record(cr_type):
                    continue
                best_options = NODBObservation.find_best_copy(self.db, obs_uuid, obs_date)
                best_obs_data = None
                for check_uuid, check_date in best_options:
                    obs = NODBObservationData.find_by_uuid(self.db, check_uuid, check_date, limit_fields=["obs_uuid", "received_date", "data_mode", "quality_checks"])
                    if best_obs_data is None:
                        best_obs_data = obs
                    if obs is not None and obs.data_mode is dm and obs.quality_checks & qf:
                        break
                else:
                    if start_from_best and best_obs_data is not None:
                        obs_data = best_obs_data
                    else:
                        obs_data = NODBObservationData.find_by_uuid(self.db, obs_uuid, obs_date)
                    if obs_data is None:
                        raise ValueError("Observation data not found")
                    working_uuid = rm.create_working_entry(
                        obs_data.record,
                        obs_data.source_file_uuid,
                        obs_data.received_date,
                        obs_data.message_idx,
                        obs_data.record_idx,
                        data_mode=dm,
                        quality_flags=obs_data.quality_checks | qf
                    )
                    if working_uuid is not None:
                        working_uuids.append(working_uuid)
            if working_uuids:
                batch = NODBBatch()
                self.db.insert_object(batch)
                NODBWorkingRecord.bulk_set_batch_uuid(self.db, batch_uuid=batch.batch_uuid, working_uuids=working_uuids)
                wf_controller.handle_incoming_batch(batch.batch_uuid)
