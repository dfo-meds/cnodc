import datetime

from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController, LockType
from autoinject import injector

from cnodc.util import CNODCError


class NODBVerificationWorker(QueueWorker):

    NAME = "nodb_verify"
    VERSION = "1.0"

    def __init__(self, **kwargs):
        super().__init__(log_name=NODBVerificationWorker.NAME, **kwargs)
        self._verifier = None

    def on_start(self):
        self._verifier = NODBVerifier()

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        if 'item_uuid' not in item.data:
            raise CNODCError(f'Missing item_uuid in queue item [{item.queue_uuid}]', 'NODBVERIFY', 1000)
        if 'item_received' not in item.data:
            raise CNODCError(f'Missing item_received in queue item [{item.queue_uuid}]', 'NODBVERIFY', 1001)
        return structures.QueueItemResult.SUCCESS


class NODBVerifier:

    nodb: NODBController = None

    @injector.construct
    def __init__(self):
        pass

    def verify_obs(self, obs_uuid: str, received_date: str, post_processing_queues: t.Optional[list[str]]):
        with self.nodb as db:
            obs_data = structures.NODBObservationData.find_by_uuid(
                 db,
                 obs_uuid,
                 received_date,
                 lock_type=LockType.FOR_NO_KEY_UPDATE
            )
            if not obs_data:
                raise CNODCError(f"No such observation record [{received_date}/{obs_uuid}]", "NODBVERIFY", 1002)
