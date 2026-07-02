import datetime
import typing as t

import itertools

from pipeman.processing.payload_worker import BatchWorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager, CreationResultType, NODBCreationResult
from pipeman.processing.payloads import BatchPayload, ObservationPayload

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
            'merge_queue': 'nodb_merge',
            'duplicate_flag_queue': 'nodb_flag_duplicates',
        })

    def on_start(self):
        super().on_start()

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        batch = payload.load_batch(self.db)
        if batch.status != BatchStatus.COMPLETE:
            for_merge: list[NODBCreationResult] = []
            for_flag_others: list[NODBCreationResult] = []
            for_continue: list[NODBCreationResult] = []
            with NODBRecordManager(self.db) as rm:
                for working in batch.stream_working_records(self.db):
                    result = rm.create_completed_entry_from_working_record(
                        working=working,
                    )
                    self.db.delete_object(working)
                    if result.action is CreationResultType.MERGE:
                        for_merge.append(result)
                    elif result.action is CreationResultType.UPDATE:
                        for_flag_others.append(result)
                    else:
                        for_continue.append(result)
            for result in for_flag_others:
                if result.others is None:
                    continue
                for other in result.others:
                    self.db.create_queue_item(
                        self.get_config("duplicate_flag_queue", "nodb_flag_duplicates"),
                        data={
                            "best_uuid": result.obs_uuid,
                            "best_date": result.received_date.isoformat(),
                            "other": other
                        },
                        unique_item_name=other,
                        correlation_id=payload.correlation_id,
                        tag=payload.tag
                    )
            for result in for_merge:
                if result.others is None:
                    continue
                self.db.create_queue_item(
                    self.get_config("merge_queue", "nodb_merge"),
                    data={
                        "current_uuid": result.obs_uuid,
                        "current_date": result.received_date.isoformat(),
                        "others": result.others,
                        "workflow_name": payload.workflow_name,
                        "workflow_step": payload.current_step,
                        "finalize_queue": self._queue_name,
                    },
                    correlation_id=payload.correlation_id,
                    tag=payload.tag,
                )
            batch.status = BatchStatus.COMPLETE
            self.db.update_object(batch)
            if for_continue or for_flag_others:
                no_payload = self.new_observations_payload_from_uuids([
                    (x.obs_uuid, x.received_date, x.action.value)
                    for x in itertools.chain(for_continue, for_flag_others)
                ])
                self.progress_payload(no_payload, prevent_default_progression=True)
            else:
                self.prevent_default_progression()
