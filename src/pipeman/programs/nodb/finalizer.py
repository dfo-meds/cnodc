import typing as t
from nodb.interface import NODBInstance
from pipeman.processing.payload_worker import WorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager
from pipeman.processing.payloads import BatchPayload, SourceFilePayload

from nodb.observations import BatchStatus, NODBWorkingRecord


class NODBFinalizeWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="finalizer",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_finalize',
            'next_queue': 'workflow_continue',
            'merge_queue': 'nodb_merge',
        })

    def process_payload(self, payload: BatchPayload | SourceFilePayload) -> t.Optional[QueueItemResult]:
        if isinstance(payload, BatchPayload):
            batch = payload.load_batch(self.db)
            if batch.status != BatchStatus.COMPLETE:
                self.finalize_records(batch.stream_working_records, payload, "B", payload.batch_uuid)
                batch.status = BatchStatus.COMPLETE
                self.db.update_object(batch)
        elif isinstance(payload, SourceFilePayload):
            source = payload.load_source_file(self.db)
            self.finalize_records(source.stream_working_records, payload, "S", f"{payload.source_uuid}__{payload.received_date}")
        else:
            raise ValueError("Invalid payload type")

    def finalize_records(self,
                         stream: t.Callable[[NODBInstance], t.Iterable[NODBWorkingRecord]],
                         payload: BatchPayload | SourceFilePayload,
                         object_type: str,
                         object_uuid: str):
        with NODBRecordManager(self.db) as rm:
            for working in stream(self.db):
                result = rm.create_completed_entry_from_working_record(
                    working=working,
                )
                if result.merge_with:
                    self.db.create_queue_item(
                        self.get_config("merge_queue", "nodb_merge"),
                        data={
                            "current_uuid": result.obs_uuid,
                            "current_date": result.received_date.isoformat(),
                            "others": result.merge_with,
                            "workflow_name": payload.workflow_name,
                        },
                        correlation_id=payload.correlation_id,
                        tag=payload.tag,
                    )
                self.db.create_temp_finalize_result(
                    object_type,
                    object_uuid,
                    result.obs_uuid,
                    result.received_date,
                    result.action.value
                )
                self.db.delete_object(working)
                self.db.commit()
        next_payload = self.new_observations_payload_from_uuids(
            self.db.stream_temp_finalize_results(object_type, object_uuid)
        )
        if next_payload.observations:
            self.progress_payload(next_payload, prevent_default_progression=True)
        else:
            self.prevent_default_progression()

