from cnodc.process.payload_worker import BatchWorkflowWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.process.queue_worker import QueueItemResult
from cnodc.programs.nodb_intake.record_manager import NODBRecordManager
from cnodc.workflow.workflow import BatchPayload


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

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        batch = payload.load_batch(self._db)
        if batch.status != structures.BatchStatus.COMPLETE:
            for working in batch.stream_working_records(self._db):
                self._finalizer.create_completed_entry(
                    self._db,
                    working.record,
                    working.received_date,
                    working.message_idx,
                    working.record_idx,
                    working.source_file_uuid
                )
                self._db.delete_object(working)
            batch.status = structures.BatchStatus.COMPLETE
            self._db.update_object(batch)
            self.progress_payload(prevent_default_progression=True)


