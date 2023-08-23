from autoinject import injector

from cnodc.exc import CNODCError
from cnodc.nodb import NODBWorkingObservation, NODBQCBatch
from cnodc.nodb.proto import NODBDatabaseProtocol, NODBQueueProtocol, LockMode, NODBTransaction
from cnodc.nodb.structures import QualityControlStatus, ObservationWorkingStatus, ObservationStatus
from cnodc.qc.auto.basic import BasicQualityController
from .base import BaseController


class QualityControlController(BaseController):

    database: NODBDatabaseProtocol = None
    queues: NODBQueueProtocol = None

    @injector.construct
    def __init__(self, instance: str):
        super().__init__("NODB_QCC", "1_0_0", instance)
        self.basic_qc = BasicQualityController(self.instance)

    def start_qc(self, batch_uuid: str, from_basic: bool = True):
        tx = None
        try:
            tx = self.database.start_transaction()
            batch, observations = self.load_batch_and_obs(batch_uuid, tx)
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
            self.basic_qc.basic_qc_check(obs, tx)
            result = result and not obs.has_any_qc_code()
        return result

    def _start_qc_process(self, batch: NODBQCBatch, observations: list[NODBWorkingObservation], tx: NODBTransaction):
        station_count = set()
        for obs in observations:
            station_count.add(obs.station_uuid)
            self._update_primary_record(obs, tx)
        if len(station_count) > 1:
            by_station_uuid = {}
            for obs in observations:
                if obs.station_uuid not in by_station_uuid:
                    by_station_uuid[obs.station_uuid] = []
                by_station_uuid[obs.station_uuid].append(obs)
            with tx.savepoint("qc_batch_subdivide"):
                batches = [self._create_new_qc_batch(x, by_station_uuid[x], tx) for x in by_station_uuid]
                for b in batches:
                    self._start_qc_for_batch(b, tx)
        elif station_count:
            batch.set_qc_metadata("station_uuid", list(station_count)[0])
            self._start_qc_for_batch(batch, tx)
        else:
            raise CNODCError(f"Batch [{batch.pkey}] does not have any observations or stations but has passed QC",
                             "QC_START", 1001)

    def _update_primary_record(self, obs: NODBWorkingObservation, tx: NODBTransaction):
        primary = self.database.load_observation(
            obs.pkey,
            with_lock=LockMode.FOR_NO_KEY_UPDATE,
            with_data=False,
            tx=tx
        )
        if not primary:
            raise CNODCError(f"Missing primary record for [{obs.pkey}]", "QC_START", 1002)
        if primary.status == ObservationStatus.VERIFIED:
            return
        primary.update_from_working(obs)
        primary.status = ObservationStatus.VERIFIED
        self.database.save_observation(primary, tx)

    def _create_new_qc_batch(self, station_uuid: str, observations: list[NODBWorkingObservation], tx):
        batch = NODBQCBatch()
        batch.set_qc_metadata("station_uuid", station_uuid)
        batch.qc_test_status = QualityControlStatus.PASSED
        batch.qc_process_name = "__basic__"
        batch.qc_current_step = 0
        self.database.save_batch_and_assign(batch, observations, tx)
        return batch

    def _start_qc_for_batch(self, batch: NODBQCBatch, tx: NODBTransaction):
        pass

    def _queue_batch_for_review(self, batch: NODBQCBatch, tx: NODBTransaction):
        batch.qc_test_status = QualityControlStatus.MANUAL_REVIEW
        batch.working_status = ObservationWorkingStatus.QUEUED
        self.database.save_batch(batch, tx)
        try:
            self.queues.queue_basic_qc_review(batch)
        except Exception as ex:
            batch.working_status = ObservationWorkingStatus.QUEUE_ERROR
            self.database.save_batch(batch, tx)


