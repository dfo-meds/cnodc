from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController
from autoinject import injector

from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import SourceFilePayload


class NODBVerificationWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_verify", **kwargs)
        self._verifier: t.Optional[NODBVerifier] = None

    def on_start(self):
        self._verifier = NODBVerifier(
            processor_uuid=self.process_uuid
        )

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._verifier.process_queue_item(item)


class NODBVerifier(PayloadProcessor):

    nodb: NODBController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            processor_version="1.0",
            processor_name="nodb_verify",
            require_type=SourceFilePayload,
            **kwargs
        )

    def _process(self):
        pass
