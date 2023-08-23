import enum
import typing as t
from contextlib import contextmanager

from .structures import NODBSourceFile, NODBObservation, NODBWorkingObservation, NODBStation, NODBQCBatch, \
    ObservationWorkingStatus, QualityControlStatus, NODBQCProcess
from autoinject import injector


class NODBTransaction(t.Protocol):

    def commit(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def create_savepoint(self, name):
        raise NotImplementedError()

    def rollback_to_savepoint(self, name):
        raise NotImplementedError()

    def release_savepoint(self, name):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    @contextmanager
    def savepoint(self, name):
        try:
            self.create_savepoint(name)
            yield self
        except Exception as ex:
            self.rollback_to_savepoint(name)
            raise ex
        finally:
            self.release_savepoint(name)


class LockMode(enum.Enum):

    NO_LOCK = 0
    FOR_UPDATE = 1
    FOR_NO_KEY_UPDATE = 2
    FOR_SHARE = 3
    FOR_KEY_SHARE = 4


@injector.injectable_global
class NODBDatabaseProtocol(t.Protocol):

    @contextmanager
    def start_transaction(self) -> NODBTransaction:
        raise NotImplementedError()

    def errored_source_file_exists(self, source_file_uuid: str, message_idx: int, tx: NODBTransaction = None) -> bool:
        raise NotImplementedError()

    def find_primary_observation_by_source(self, source_file_uuid: str, message_idx: int, record_idx: int, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None):
        raise NotImplementedError()

    def find_working_observations_by_source(self, source_file_uuid: str, basic_only: bool = True, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False) -> t.Iterable[tuple[int, int, t.Optional[NODBWorkingObservation]]]:
        # Full working obs but only if in basic initial QC still (i.e. UNVERIFIED)
        raise NotImplementedError()

    def find_stations(self, wmo_id: t.Optional[str] = None, wigos_id: t.Optional[str] = None, station_id: t.Optional[str] = None, station_name: t.Optional[str] = None, tx: NODBTransaction = None, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False) -> dict[str, NODBStation]:
        raise NotImplementedError()

    def load_batch(self, batch_uuid: str, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Optional[NODBQCBatch]:
        raise NotImplementedError()

    def load_working_observations_for_batch(self, batch_uuid: str, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Iterable[NODBWorkingObservation]:
        raise NotImplementedError()

    def load_observation(self, obs_uuid, with_data: bool = True, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Optional[NODBObservation]:
        raise NotImplementedError()

    def load_working_observation(self, primary_obs_uuid, with_data: bool = True, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Optional[NODBWorkingObservation]:
        raise NotImplementedError()

    def load_station(self, station_uuid: str, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Optional[NODBStation]:
        raise NotImplementedError

    def load_source_file(self, source_file_uuid: str, with_lock: LockMode = LockMode.NO_LOCK, no_wait: bool = False, tx: NODBTransaction = None) -> t.Optional[NODBSourceFile]:
        raise NotImplementedError()

    def load_qc_for_station(self, station_uuid: str, tx: NODBTransaction = None) -> t.Optional[NODBQCProcess]:
        raise NotImplementedError()

    def save_source_file(self, source_file: NODBSourceFile, tx: NODBTransaction = None):
        raise NotImplementedError()

    def save_observation(self, obs: NODBObservation, tx: NODBTransaction = None):
        raise NotImplementedError()

    def save_working_observation(self, obs: NODBWorkingObservation, tx: NODBTransaction = None):
        raise NotImplementedError()

    def save_batch(self, batch: NODBQCBatch, tx: NODBTransaction = None):
        raise NotImplementedError()

    def save_batch_and_assign(self, batch: NODBQCBatch, obs_list: list[NODBWorkingObservation], tx: NODBTransaction):
        with tx.savepoint("qc_batch_assignment"):
            self.save_batch(batch, tx=tx)
            for obs in obs_list:
                obs.working_status = ObservationWorkingStatus.BATCH
                obs.qc_batch_id = batch.pkey
                obs.qc_current_step = None
                self.save_working_observation(obs, tx)


@injector.injectable_global
class NODBQueueProtocol(t.Protocol):

    def queue_source_file_download(self, source_file: NODBSourceFile):
        raise NotImplementedError()

    def queue_source_file_decode_error(self, source_file: NODBSourceFile):
        raise NotImplementedError()

    def queue_basic_qc_review(self, batch: NODBQCBatch):
        raise NotImplementedError()

    def queue_basic_qc_process(self, batch: NODBQCBatch):
        raise NotImplementedError()


