import typing as t

from medsutil.exceptions import CodedError
from nodb.queue import NODBQueueItem
from pipeman.processing.queue_worker import QueueItemResult, QueueWorker

from nodb.observations import NODBObservation, NODBWorkingRecord


class NODBDuplicateUpdater(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="duplicate_updater",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_flag_duplicates',
        })

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        if "best_uuid" not in item.data:
            raise CodedError("Missing best_uuid", 1000, code_space="FLAG-DUPES")
        if "best_date" not in item.data:
            raise CodedError("Missing best_date", 1001, code_space="FLAG-DUPES")
        if "other" not in item.data:
            raise CodedError("Missing list of others", 1002, code_space="FLAG-DUPES")
        obs = NODBObservation.find_by_uuid(
            self.db,
            t.cast(str, item.data.get("best_uuid")),
            t.cast(str, item.data.get("best_date")),
            key_only=True
        )
        if obs is None:
            raise CodedError("Invalid best observation uuid/date", 1003, code_space="FLAG-DUPES")
        for other_obs_code in t.cast(list[str], item.data.get("other")):
            if '/' not in other_obs_code:
                raise CodedError("Invalid other observation code", 1004, code_space="FLAG-DUPES")
            other_date, other_uuid = other_obs_code.split("/", maxsplit=1)
            other_obs = NODBObservation.find_by_uuid(self.db, other_uuid, other_date, key_only=True)
            if other_obs is None:
                working = NODBWorkingRecord.find_by_uuid(self.db, other_uuid, key_only=True)
                if working is None:
                    raise CodedError("Invalid other observation", 1005, code_space="FLAG-DUPES")
                else:
                    raise CodedError("Other observation has not yet been inserted", 1006, code_space="FLAG-DUPES", is_transient=True)
            other_obs_data = other_obs.find_observation_data(self.db)
            if other_obs_data is None:
                raise CodedError("Missing other observation data", 1007, code_space="FLAG-DUPES")
            other_obs_data.duplicate_uuid = obs.obs_uuid
            other_obs_data.duplicate_received_date = obs.received_date
            self.db.update_object(other_obs_data)
