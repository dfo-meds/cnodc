"""
    A payload worker is a queue worker where the queue data structure
    follows one of the workflow payload types (e.g. Batch, SourceFile, File, etc.).
"""
import datetime
import pathlib

from cnodc.nodb import structures
from cnodc.process.queue_worker import QueueWorker, QueueItemResult
import typing as t
from cnodc.util import CNODCError
from cnodc.workflow.workflow import WorkflowPayload, BatchPayload, SourceFilePayload, FilePayload, FileInfo


class WorkflowWorker(QueueWorker):
    """Generic payload worker for any payload type."""

    def __init__(self, process_name: str, process_version: str, require_type: t.Optional = None, **kwargs):
        super().__init__(process_version=process_version, process_name=process_name, **kwargs)
        self._require_type = require_type
        self.current_payload: t.Optional[WorkflowPayload] = None
        self._skip_autoprogress_payload: bool = False

    def progress_queue_item(self,
                            new_payload: t.Optional[WorkflowPayload] = None,
                            next_queue: t.Optional[str] = None,
                            prevent_default_progression: bool = False):
        if next_queue is None:
            next_queue = self.get_config('next_queue', 'workflow_continue')
        if new_payload is None:
            new_payload = self.copy_payload(self.current_payload)
            new_payload.current_step_done = next_queue == "workflow_continue"
        else:
            new_payload.copy_details_from(self.current_payload, next_queue == "workflow_continue")
            self.add_payload_metadata(new_payload)
        new_payload.enqueue(self._db, next_queue)
        if prevent_default_progression:
            self.prevent_default_progression()

    def prevent_default_progression(self):
        self._skip_autoprogress_payload = True

    def autocomplete(self, queue_item):
        super().autocomplete(queue_item)
        if not self._skip_autoprogress_payload:
            self.progress_queue_item()

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[QueueItemResult]:
        """Handles extracting the payload and checking that it is of the correct type"""
        payload = WorkflowPayload.from_queue_item(item)
        if self._require_type is not None and not isinstance(payload, self._require_type):
            raise CNODCError('Payload is not of valid type', 'PAYLOAD', 1000)
        try:
            self._skip_autoprogress_payload = False
            self.current_payload = payload
            return self.process_payload(payload)
        finally:
            self.current_payload = None

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        """Override to add payload logic."""
        raise NotImplementedError

    def batch_payload_from_uuid(self, batch_uuid: str) -> BatchPayload:
        """Create a new payload from a batch UUID."""
        payload = BatchPayload(batch_uuid)
        self.add_payload_metadata(payload)
        return payload

    def batch_payload_from_nodb(self, batch: structures.NODBBatch) -> BatchPayload:
        """Create a new payload from a batch object."""
        payload = BatchPayload.from_batch(batch)
        self.add_payload_metadata(payload)
        return payload

    def source_payload_from_nodb(self, source_file: structures.NODBSourceFile) -> SourceFilePayload:
        """Create a new payload from an NODB source file."""
        payload = SourceFilePayload.from_source_file(source_file)
        self.add_payload_metadata(payload)
        return payload

    def file_payload_from_path(self, path: str, mod_date: t.Optional[datetime.datetime] = None):
        payload = FilePayload.from_path(path, mod_date)
        self.add_payload_metadata(payload)
        return payload

    def add_payload_metadata(self, new_payload: WorkflowPayload):
        """Add the current payload's metadata to the new payload."""
        if self.current_payload is not None:
            new_payload.copy_details_from(self.current_payload)
        new_payload.metadata['_source_info'] = (
            self._process_name,
            self._process_version,
            self._process_uuid
        )

    def copy_payload(self, payload: WorkflowPayload):
        """Create a copy of the given payload with the current payload's metadata."""
        payload_copy = payload.clone()
        self.add_payload_metadata(payload_copy)
        return payload_copy


class BatchWorkflowWorker(WorkflowWorker):
    """Implementation of PayloadWorker that limits payloads to Batch types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=BatchPayload)

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError


class SourceWorkflowWorker(WorkflowWorker):
    """Implementation of PayloadWorker that limits payloads to SourceFile types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=SourceFilePayload)

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError


class FileWorkflowWorker(WorkflowWorker):
    """Implementation of PayloadWorker that limits payloads to File types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=FilePayload)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError

    def download_to_temp_file(self) -> pathlib.Path:
        return self.current_payload.download(self.temp_dir(), halt_flag=self._halt_flag)
