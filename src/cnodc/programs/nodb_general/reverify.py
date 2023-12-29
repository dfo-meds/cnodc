from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController, LockType
from autoinject import injector

from cnodc.programs.nodb_general.batcher import NODBInitialVerificationBatchManager
from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import BatchPayload


class NODBReverificationWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_reverify", **kwargs)
        self._verifier: t.Optional[NODBReverifier] = None

    def on_start(self):
        self._verifier = NODBReverifier(
            processor_uuid=self.process_uuid
        )

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._verifier.process_queue_item(item)


class NODBReverifier(PayloadProcessor):

    nodb: NODBController = None

    NAME = 'nodb_reverification'
    VERSION = '1.0'

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            processor_version=NODBReverifier.NAME,
            processor_name=NODBReverifier.VERSION,
            require_type=BatchPayload,
            **kwargs
        )

    def _process(self):
        batch = self.load_batch_from_payload()
        batch_manager = NODBInitialVerificationBatchManager(
            original_payload=self._current_payload,
            db=self._db,
            for_batch_id=batch.batch_uuid,
            halt_flag=self._halt_flag
        )
        for working_record in batch.stream_working_records(self._db, lock_type=LockType.FOR_NO_KEY_UPDATE):
            batch_manager.verify_record(working_record)
        batch_manager.flush_all()
        self._db.delete_object(batch)
