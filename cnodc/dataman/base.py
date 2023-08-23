from cnodc.exc import CNODCError
from cnodc.nodb import NODBDatabaseProtocol, NODBWorkingObservation, NODBQCBatch
from autoinject import injector

from cnodc.nodb.proto import NODBTransaction, LockMode
from cnodc.nodb.structures import ObservationWorkingStatus


class BaseController:

    database: NODBDatabaseProtocol = None

    @injector.construct
    def __init__(self, name, version, instance):
        self.name = name
        self.version = version
        self.instance = instance

    def load_batch_and_obs(self, batch_uuid: str, tx: NODBTransaction) -> tuple[NODBQCBatch, list[NODBWorkingObservation]]:
        batch = self.database.load_batch(
            batch_uuid,
            with_lock=LockMode.FOR_NO_KEY_UPDATE,
            tx=tx
        )
        if not batch:
            raise CNODCError(f"Batch [{batch_uuid}] not found", "NODB_BASE", 1000)
        if batch.working_status != ObservationWorkingStatus.QUEUED:
            raise CNODCError(f"Batch [{batch_uuid}] is not in a QUEUED state", "NODB_BASE", 1001, True)
        observations = []
        for obs in self.database.load_working_observations_for_batch(
            batch.pkey,
            with_lock=LockMode.FOR_NO_KEY_UPDATE,
            tx=tx
        ):
            if obs.working_status != ObservationWorkingStatus.BATCH:
                raise CNODCError(f"Observation [{obs.pkey}] is not in a BATCH state", "NODB_BASE", 1002, True)
            observations.append(obs)
        batch.working_status = ObservationWorkingStatus.IN_PROGRESS
        return batch, observations
