
import typing as t

from pipeman.processing.payload_worker import BatchWorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager
from pipeman.processing.payloads import BatchPayload

from nodb.observations import BatchStatus


class NODBFinalizeWorker(BatchWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="finalizer",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_finalize',
            'next_queue': 'workflow_continue',
        })

    def on_start(self):
        super().on_start()

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        batch = payload.load_batch(self.db)
        if batch.status != BatchStatus.COMPLETE:
            with NODBRecordManager(self.db) as rm:
                for working in batch.stream_working_records(self.db):
                    rm.create_completed_entry_from_working_record(
                        working=working,
                    )
                    self.db.delete_object(working)
            batch.status = BatchStatus.COMPLETE
            self.db.update_object(batch)
            self.progress_payload(prevent_default_progression=True)


