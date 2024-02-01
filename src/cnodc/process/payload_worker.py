from cnodc.nodb import structures
from cnodc.process.queue_worker import QueueWorker, QueueItemResult
import typing as t

from cnodc.util import CNODCError
from cnodc.workflow.workflow import WorkflowPayload, BatchPayload, SourceFilePayload, FilePayload


class PayloadWorker(QueueWorker):

    def __init__(self, process_name: str, process_version: str, require_type: t.Optional = None, **kwargs):
        super().__init__(process_version=process_version, process_name=process_name, **kwargs)
        self._require_type = require_type
        self.current_payload: t.Optional[WorkflowPayload] = None

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[QueueItemResult]:
        payload = WorkflowPayload.build(item)
        if self._require_type is not None and not isinstance(payload, self._require_type):
            raise CNODCError('Payload is not of valid type', 'PAYLOAD', 1000)
        try:
            self.current_payload = payload
            return self.process_payload(payload)
        finally:
            self.current_payload = None

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError

    def batch_payload_from_uuid(self, batch_uuid: str) -> BatchPayload:
        payload = BatchPayload(batch_uuid)
        self.add_payload_metadata(payload)
        return payload

    def batch_payload_from_nodb(self, batch: structures.NODBBatch) -> BatchPayload:
        payload = BatchPayload.from_batch(batch)
        self.add_payload_metadata(payload)
        return payload

    def source_payload_from_nodb(self, source_file: structures.NODBSourceFile) -> SourceFilePayload:
        payload = SourceFilePayload.from_source_file(source_file)
        self.add_payload_metadata(payload)
        return payload

    def add_payload_metadata(self, new_payload: WorkflowPayload):
        if self.current_payload is not None:
            new_payload.copy_details(self.current_payload)
        new_payload.metadata['_source_info'] = (
            self._process_name,
            self._process_version,
            self._process_uuid
        )

    def copy_payload(self, payload: WorkflowPayload):
        payload_copy = payload.clone()
        self.add_payload_metadata(payload_copy)
        return payload_copy


class BatchPayloadWorker(PayloadWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=BatchPayload)

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError


class SourcePayloadWorker(PayloadWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=SourceFilePayload)

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError


class FilePayloadWorker(PayloadWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=FilePayload)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError
