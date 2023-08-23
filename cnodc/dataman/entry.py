from autoinject import injector

from cnodc.exc import CNODCError
from cnodc.nodb import NODBWorkingObservation, NODBQCBatch
from cnodc.nodb.proto import NODBDatabaseProtocol, NODBQueueProtocol, LockMode, NODBTransaction
from cnodc.nodb.structures import QualityControlStatus, ObservationWorkingStatus, ObservationStatus
from cnodc.qc.auto.basic import BasicQualityController
from .base import BaseController


class NODBEntryController(BaseController):

    database: NODBDatabaseProtocol = None
    queues: NODBQueueProtocol = None

    @injector.construct
    def __init__(self, instance: str):
        super().__init__("NODB_QCC", "1_0_0", instance)
        self.basic_qc = BasicQualityController(self.instance)

    def start_qc(self, batch_uuid: str):
        tx = None
        batch = None
        try:
            tx = self.database.start_transaction()
            batch, observations = self.load_batch_and_obs(batch_uuid, tx)
            new_records = self._apply_followup_basic_qc(observations, tx)
            if new_records:
                if any(x.has_any_qc_code() for x in new_records):
                    self._queue_batch_for_review(batch, tx)
                else:
                    self._start_qc_process(batch, new_records, tx)
        # TODO: handle exceptions
        finally:
            if batch and tx:
                self.database.save_batch(batch, tx=tx)
            if tx:
                tx.commit()
                tx.close()
                tx = None

    def _apply_followup_basic_qc(self, observations: list[NODBWorkingObservation], tx: NODBTransaction) -> list[NODBWorkingObservation]:
        keep = []
        for obs in observations:
            if obs.qc_test_status == QualityControlStatus.UNCHECKED:
                self.basic_qc.basic_qc_check(obs, tx)
            if obs.working_status == ObservationWorkingStatus.ERROR:
                raise CNODCError(f"User flagged a record in the batch as errored", is_recoverable=False)
            elif obs.working_status == ObservationWorkingStatus.DISCARDED:
                obs.qc_batch_id = None
                self._update_primary_record(obs, tx)
                self.database.save_working_observation(obs, tx)
            else:
                keep.append(obs)
        return keep

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
        if obs.working_status == ObservationWorkingStatus.DISCARDED:
            primary.status = ObservationStatus.DISCARDED
        else:
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
        if 'station_uuid' not in batch.qc_metadata:
            raise CNODCError(f"Missing station ID for [{batch.pkey}]", "QC_START", 1003)
        qc_process = self.database.load_qc_for_station(batch.get_qc_metadata("station_uuid"), with_lock=LockMode.FOR_KEY_SHARE,  tx=tx)
        if qc_process and qc_process.has_rt_qc():
            batch.qc_test_status = QualityControlStatus.UNCHECKED
            batch.qc_process_name = qc_process.pkey
            batch.qc_current_step = 0
            batch.working_status = ObservationWorkingStatus.NEW
            self.database.save_batch(batch)
            try:
                self.queues.queue_next_qc(batch)
                batch.working_status = ObservationWorkingStatus.QUEUED
            except Exception as ex:
                batch.working_status = ObservationWorkingStatus.QUEUE_ERROR
                raise ex
            finally:
                self.database.save_batch(batch)

    def _queue_batch_for_review(self, batch: NODBQCBatch, tx: NODBTransaction):
        batch.qc_test_status = QualityControlStatus.MANUAL_REVIEW
        batch.working_status = ObservationWorkingStatus.NEW
        self.database.save_batch(batch, tx)
        try:
            self.queues.queue_basic_qc_review(batch)
            batch.working_status = ObservationWorkingStatus.QUEUED
        except Exception as ex:
            batch.working_status = ObservationWorkingStatus.QUEUE_ERROR
            raise ex
        finally:
            self.database.save_batch(batch, tx)


