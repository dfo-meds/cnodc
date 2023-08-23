from autoinject import injector

from cnodc.nodb import NODBWorkingObservation, NODBQCBatch
from cnodc.nodb.proto import NODBDatabaseProtocol, NODBQueueProtocol, LockMode, NODBTransaction
from cnodc.nodb.structures import QualityControlStatus, ObservationWorkingStatus
from cnodc.qc.auto.basic import BasicQualityController


class QualityControlController:

    database: NODBDatabaseProtocol = None
    queues: NODBQueueProtocol = None

    @injector.construct
    def __init__(self, instance: str):
        self.name = "NODB_QCC"
        self.version = "1_0_0"
        self.instance = instance
        self.basic_qc = BasicQualityController(self.instance)

    def start_qc(self, batch_uuid):
        tx = None
        try:
            tx = self.database.start_transaction()
            batch = self.database.load_batch(
                batch_uuid,
                with_lock=LockMode.FOR_NO_KEY_UPDATE,
                tx=tx
            )
            observations = [
                obs
                for obs in self.database.load_working_observations_for_batch(
                    batch.pkey,
                    with_lock=LockMode.FOR_NO_KEY_UPDATE,
                    tx=tx
                )
            ]
            qc_passed = self._apply_followup_basic_qc(observations, tx)
            if qc_passed:
                self._start_qc_process(batch, observations, tx)
            else:
                self._queue_batch_for_review(batch, tx)
        # TODO: handle exceptions
        finally:
            if tx:
                tx.commit()
                tx.close()
                tx = None

    def _apply_followup_basic_qc(self, observations: list[NODBWorkingObservation], tx: NODBTransaction) -> bool:
        result = True
        for obs in observations:
            self.basic_qc.followup_basic_qc(obs, tx)
            result = result and not obs.has_any_qc_code()
        return result

    def _start_qc_process(self, batch: NODBQCBatch, observations: list[NODBWorkingObservation], tx: NODBTransaction):
        pass
        # TODO: handle splitting apart the batch (if necessary) and starting the observations on their journey

    def _queue_batch_for_review(self, batch: NODBQCBatch, tx: NODBTransaction):
        batch.qc_test_status = QualityControlStatus.MANUAL_REVIEW
        batch.working_status = ObservationWorkingStatus.QUEUED
        self.database.save_batch(batch, tx)
        try:
            self.queues.queue_basic_qc_review(batch)
        except Exception as ex:
            batch.working_status = ObservationWorkingStatus.QUEUE_ERROR
            self.database.save_batch(batch, tx)


