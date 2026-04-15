from pipeman.processing.payload_worker import BatchWorkflowWorker
import nodb as nodb
import typing as t

from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager
from pipeman.processing.payloads import BatchPayload


class NODBFinalizeWorker(BatchWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="finalizer",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_finalize',
            'next_queue': 'workflow_continue',
        })
        self._finalizer: t.Optional[NODBRecordManager] = None

    def on_start(self):
        self._finalizer = NODBRecordManager()
        super().on_start()

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        batch = payload.load_batch(self.db)
        memory = {}
        if batch.status != nodb.BatchStatus.COMPLETE:
            for working in batch.stream_working_records(self.db):
                self._finalizer.create_completed_entry_from_working_record(
                    db=self.db,
                    working=working,
                    memory=memory
                )
                self.db.delete_object(working)
            batch.status = nodb.BatchStatus.COMPLETE
            self.db.update_object(batch)
            self.progress_payload(prevent_default_progression=True)


