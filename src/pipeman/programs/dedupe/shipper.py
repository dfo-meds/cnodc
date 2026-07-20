import typing as t

from medsutil.exceptions import CodedError
from nodb.queue import NODBQueueItem
from pipeman.processing.queue_worker import QueueItemResult, QueueWorker

from nodb.observations import NODBObservation, NODBWorkingRecord, NODBObservationRelationship, \
    ObservationRelationshipType
from pipeman.programs.dedupe.dedupe import RelationshipAction


class RelationshipError(CodedError): CODE_SPACE = "NODB-SHIP"


class NODBRelationshipUpdater(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="relationship_updater",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_add_relationships',
        })

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        if "record_uuid" not in item.data:
            raise RelationshipError("Missing best_uuid", 1000)
        if "record_date" not in item.data:
            raise RelationshipError("Missing best_date", 1001)
        if "other_uuid" not in item.data:
            raise RelationshipError("Missing other_uuid", 1002)
        if "other_date" not in item.data:
            raise RelationshipError("Missing other_date", 1003)
        if "relation_type" not in item.data:
            raise RelationshipError("Missing relation_type", 1004)
        self.update_relationship(
            str(item.data["record_uuid"]),
            str(item.data["record_date"]),
            str(item.data["other_uuid"]),
            str(item.data["other_date"]),
            str(item.data["relation_type"]),
        )

    def update_relationship(self, record_uuid: str, record_date: str, other_uuid: str, other_date: str, relation_type: str):
        try:
            rel_action = RelationshipAction(relation_type)
        except ValueError:
            raise RelationshipError("Invalid relationship type", 2000)
        if rel_action in (RelationshipAction.MERGE, RelationshipAction.REVIEW_MERGE):
            raise RelationshipError("Invalid relationship type for shipping", 2001)

        obs = NODBObservation.find_by_uuid(self.db, record_uuid, record_date, key_only=True)
        if obs is None:
            raise RelationshipError("Invalid record observation UUID or date", 2002)
        other_obs = NODBObservation.find_by_uuid(self.db, other_uuid, other_date, key_only=True)
        if other_obs is None:
            raise RelationshipError("Invalid other observation UUID or date", 2003, is_transient=True)

        self._update_relationship(obs, other_obs, rel_action)

    def _update_relationship(self, obs: NODBObservation, other_obs: NODBObservation, action: RelationshipAction):
        match action:
            case RelationshipAction.MARK_DUPLICATE:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=obs.obs_uuid,
                    left_received_date=obs.received_date,
                    right_obs_uuid=other_obs.obs_uuid,
                    right_received_date=other_obs.received_date,
                    relationship_type=ObservationRelationshipType.IS_DUPLICATE
                )
            case RelationshipAction.MARK_OTHER_DUPLICATE:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=other_obs.obs_uuid,
                    left_received_date=other_obs.received_date,
                    right_obs_uuid=obs.obs_uuid,
                    right_received_date=obs.received_date,
                    relationship_type=ObservationRelationshipType.IS_DUPLICATE
                )
            case RelationshipAction.MARK_THIS_BETTER:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=obs.obs_uuid,
                    left_received_date=obs.received_date,
                    right_obs_uuid=other_obs.obs_uuid,
                    right_received_date=other_obs.received_date,
                    relationship_type=ObservationRelationshipType.BETTER_QUALITY
                )
            case RelationshipAction.MARK_OTHER_BETTER:
                relationship = NODBObservationRelationship(
                    left_obs_uuid=other_obs.obs_uuid,
                    left_received_date=other_obs.received_date,
                    right_obs_uuid=obs.obs_uuid,
                    right_received_date=obs.received_date,
                    relationship_type=ObservationRelationshipType.BETTER_QUALITY
                )
            case _:
                raise RelationshipError(f"Unrecognized relationship action: {action}", 3000)
        if not relationship.exists(self.db):
            self.db.insert_object(relationship)