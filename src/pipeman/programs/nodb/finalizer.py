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
            'duplicate_flag_queue': 'nodb_add_relationships',
        })

    def on_start(self):
        super().on_start()

    def process_payload(self, payload: BatchPayload | SourceFilePayload) -> t.Optional[QueueItemResult]:
        if isinstance(payload, BatchPayload):
            batch = payload.load_batch(self.db)
            if batch.status != BatchStatus.COMPLETE:
                self.finalize_records(batch.stream_working_records, payload)
                batch.status = BatchStatus.COMPLETE
                self.db.update_object(batch)
                self.prevent_default_progression()
        elif isinstance(payload, SourceFilePayload):
            source = payload.load_source_file(self.db)
            self.finalize_records(source.stream_working_records, payload)
            self.prevent_default_progression()
        else:
            raise ValueError("Invalid payload type")

    def finalize_records(self,
                         stream: t.Callable[[NODBInstance], t.Iterable[NODBWorkingRecord]],
                         payload: BatchPayload | SourceFilePayload):
        with NODBRecordManager(self.db) as rm:
            for working in stream(self.db):
                result = rm.create_completed_entry_from_working_record(
                    working=working,
                )
                for relation_type, relation_list in result.relationships.items():
                    for related_uuid, related_date in relation_list:
                        self.db.create_queue_item(
                            self.get_config("nodb_relationship_queue", "nodb_add_relationships"),
                            data={
                                'relation_type': relation_type.value,
                                "record_uuid": result.obs_uuid,
                                "record_date": result.received_date.isoformat(),
                                "other_uuid": related_uuid,
                                "other_date": related_date.isoformat(),
                            },
                            correlation_id=payload.correlation_id,
                            tag=payload.tag
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
                else:
                    nxt = self.new_observation_payload_from_uuids(
                        result.obs_uuid,
                        result.received_date,
                        result.action
                    )
                    self.progress_payload(nxt)
                self.db.delete_object(working)
                self.db.commit()
