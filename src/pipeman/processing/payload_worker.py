"""
    A payload worker is a queue worker where the queue data structure
    follows one of the workflow payload types (e.g. Batch, SourceFile, File, etc.).
"""
import datetime
import pathlib

from nodb import NODBQueueItem, NODBBatch, NODBSourceFile
from pipeman.processing.queue_worker import QueueWorker, QueueItemResult
import typing as t

from pipeman.exceptions import CNODCError
from pipeman.processing.payloads import WorkflowPayload, FilePayload, SourceFilePayload, BatchPayload, \
    ObservationPayload, Payload


class PayloadWorker[T: Payload](QueueWorker):

    def __init__(self, process_name: str, process_version: str, require_type: t.Optional[type[T]] = None, **kwargs):
        super().__init__(process_version=process_version, process_name=process_name, **kwargs)
        self._require_type: type[Payload] = require_type or Payload
        self._current_payload: t.Optional[T] = None

    @property
    def current_payload(self) -> T:
        if self._current_payload is None:
            raise CNODCError('Payload access when not processing a payload', 'PAYLOAD', 1002)
        return self._current_payload

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        """Handles extracting the payload and checking that it is of the correct type"""
        payload = Payload.from_queue_item(item)
        if self._require_type is not None and not isinstance(payload, self._require_type):
            raise CNODCError('Payload is not of valid type', 'PAYLOAD', 1000)
        self._log.trace('Processing payload %s', payload)
        self._current_payload = payload
        self.before_payload()
        exc = None
        res = None
        try:
            res = self.process_payload(payload)
            return res
        except Exception as ex:
            exc = exc
            raise ex
        finally:
            self.after_payload(res, exc)

    def after_cycle(self):
        super().after_cycle()
        self._current_payload = None

    def before_payload(self):
        self.run_hook('before_payload', payload=self.current_payload)

    def after_payload(self, res: t.Optional[QueueItemResult], ex: t.Optional[Exception] = None):
        self.run_hook('after_payload', payload=self.current_payload, result=res, exception=ex)

    def process_payload(self, payload: T) -> t.Optional[QueueItemResult]: raise NotImplementedError

    def add_payload_metadata(self, new_payload: Payload):
        """Add the current payload's metadata to the new payload."""
        if self.current_payload is not None:
            new_payload.copy_details_from(self.current_payload)
        new_payload.set_metadata('_source_info', self.process_id)

    def copy_payload[W: Payload](self, payload: W) -> W:
        """Create a copy of the given payload with the current payload's metadata."""
        payload_copy = payload.clone()
        self.add_payload_metadata(payload_copy)
        return payload_copy

    def download_to_temp_file(self) -> pathlib.Path:
        if hasattr(self.current_payload, 'download_from_db'):
            return self.current_payload.download_from_db(db=self.db, target_dir=self.temp_dir(), halt_flag=self._halt_flag)
        elif hasattr(self.current_payload, 'download'):
            return self.current_payload.download(target_dir=self.temp_dir(), halt_flag=self._halt_flag)
        else:
            raise CNODCError('Invalid payload type for downloading', 'PAYLOAD', 1001)



class WorkflowWorker[T: WorkflowPayload](PayloadWorker[WorkflowPayload]):
    """Generic payload worker for any payload type."""

    def __init__(self, process_name: str, process_version: str, **kwargs):
        if 'require_type' not in kwargs or not kwargs['require_type']:
            kwargs['require_type'] = WorkflowPayload
        super().__init__(process_version=process_version, process_name=process_name, **kwargs)
        self.set_defaults({
            'next_queue': 'workflow_continue'
        })
        self._skip_autoprogress_payload: bool = False

    def progress_payload(self,
                         new_payload: t.Optional[Payload] = None,
                         next_queue: t.Optional[str] = None,
                         prevent_default_progression: bool = False,
                         complete_step: t.Optional[bool] = None):
        if next_queue is None:
            next_queue = self.get_config('next_queue', 'workflow_continue')
        if new_payload is None and self.current_payload is not None:
            new_payload = self.copy_payload(self.current_payload)
        if new_payload is not None:
            self.add_payload_metadata(new_payload)
            new_payload.current_step_done = next_queue == "workflow_continue" if complete_step is None else complete_step
            self._log.info('Queuing item %s for %s', new_payload, next_queue)
            new_payload.enqueue(self.db, next_queue)
            if prevent_default_progression:
                self.prevent_default_progression()

    def prevent_default_progression(self):
        self._skip_autoprogress_payload = True

    def autocomplete(self, queue_item):
        super().autocomplete(queue_item)
        if not self._skip_autoprogress_payload:
            self._log.debug('Autoprogressing payload')
            self.progress_payload()
        else:
            self._log.debug('Skipping autoprogression')

    def before_cycle(self):
        self._skip_autoprogress_payload = False
        super().before_cycle()

    def after_cycle(self):
        super().after_cycle()
        self._skip_autoprogress_payload = False

    def batch_payload_from_uuid(self, batch_uuid: str) -> BatchPayload:
        """Create a new payload from a batch UUID."""
        payload = BatchPayload(batch_uuid=batch_uuid)
        self.add_payload_metadata(payload)
        return payload

    def batch_payload_from_nodb(self, batch: NODBBatch) -> BatchPayload:
        """Create a new payload from a batch object."""
        payload = BatchPayload.from_batch(batch)
        self.add_payload_metadata(payload)
        return payload

    def source_payload_from_nodb(self, source_file: NODBSourceFile) -> SourceFilePayload:
        """Create a new payload from an NODB source file."""
        payload = SourceFilePayload.from_source_file(source_file)
        self.add_payload_metadata(payload)
        return payload

    def file_payload_from_path(self, path: str, mod_date: t.Optional[datetime.datetime] = None) -> FilePayload:
        payload = FilePayload.from_path(path, mod_date)
        self.add_payload_metadata(payload)
        return payload


class BatchWorkflowWorker(WorkflowWorker[BatchPayload]):
    """Implementation of PayloadWorker that limits payloads to Batch types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=BatchPayload)

    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError  # pragma: no coverage


class SourceWorkflowWorker(WorkflowWorker[SourceFilePayload]):
    """Implementation of PayloadWorker that limits payloads to SourceFile types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=SourceFilePayload)

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError  # pragma: no coverage


class ObservationWorkflowWorker(WorkflowWorker[ObservationPayload]):
    """Implementation of PayloadWorker that limits payloads to SourceFile types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=ObservationPayload)

    def process_payload(self, payload: ObservationPayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError  # pragma: no coverage


class FileWorkflowWorker(WorkflowWorker[FilePayload]):
    """Implementation of PayloadWorker that limits payloads to File types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, require_type=FilePayload)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        raise NotImplementedError  # pragma: no coverage

