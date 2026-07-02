
import typing as t

from medsutil.exceptions import CodedError
from pipeman.processing.payload_worker import ObservationGroupWorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager
from pipeman.processing.payloads import NewObservationsPayload

from nodb.observations import ProcessingLevel, NODBBatch, NODBWorkingRecord


class NODBStartQC(ObservationGroupWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="start_qc",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_start_qc',
            'next_queue': 'workflow_continue',
            'processing_level': None,
            'qc_workflow_name': None,
        })

    def process_payload(self, payload: NewObservationsPayload) -> t.Optional[QueueItemResult]:
        try:
            pl = ProcessingLevel(self.get_config('processing_level', None))
        except ValueError as ex:
            raise CodedError('Invalid processing level specified', 1000, code_space='STARTQC') from ex
        wf_controller = self.get_workflow(self.get_config('qc_workflow_name'))
        with NODBRecordManager(self.db) as rm:
            working_uuids = []
            for obs in payload.stream_observations(self.db, key_only=True):
                obs_data = obs.find_observation_data(self.db)
                working_uuid = rm.create_working_entry(
                    obs_data.record,
                    obs_data.source_file_uuid,
                    obs_data.received_date,
                    obs_data.message_idx,
                    obs_data.record_idx,
                    pl
                )
                if working_uuid is not None:
                    working_uuids.append(working_uuid)
            if working_uuids:
                batch = NODBBatch()
                self.db.insert_object(batch)
                NODBWorkingRecord.bulk_set_batch_uuid(self.db, batch_uuid=batch.batch_uuid, working_uuids=working_uuids)
                wf_controller.handle_incoming_batch(batch.batch_uuid)
