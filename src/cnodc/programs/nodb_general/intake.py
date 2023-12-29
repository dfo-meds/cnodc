from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController, LockType
from autoinject import injector

from cnodc.programs.nodb_general.batcher import NODBInitialVerificationBatchManager
from cnodc.programs.nodb_general.qc import NODBVerificationTestSuite
from cnodc.qc import VerificationTestResult
from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import SourceFilePayload


class NODBVerificationWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_general", **kwargs)
        self._verifier: t.Optional[NODBVerifier] = None

    def on_start(self):
        self._verifier = NODBVerifier(
            processor_uuid=self.process_uuid
        )

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._verifier.process_queue_item(item)


class NODBVerifier(PayloadProcessor):

    nodb: NODBController = None

    NAME = 'nodb_verification'
    VERSION = '1.0'

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            processor_version=NODBVerifier.NAME,
            processor_name=NODBVerifier.VERSION,
            require_type=SourceFilePayload,
            **kwargs
        )

    def _process(self):
        source_file = self.load_source_from_payload()
        batch_manager = NODBInitialVerificationBatchManager(
            original_payload=self._current_payload,
            db=self._db,
            halt_flag=self._halt_flag
        )
        for working_record in source_file.stream_working_records(self._db, lock_type=LockType.FOR_NO_KEY_UPDATE):
            batch_manager.verify_record(working_record)
        batch_manager.flush_all()

