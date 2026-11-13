
from autoinject import injector

from nodb.queue import NODBQueueItem
from pipeman.processing.queue_worker import QueueItemResult, QueueWorker
from pipeman.programs.dmd import dmd


class DMDMetadataPushWorker(QueueWorker):
    metadb: dmd.DataManagerController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="dmd_metadata_pusher", process_version="1.0", **kwargs
        )
        self.set_defaults(
            {
                "queue_name": "dmd_metadata_push",
            }
        )

    def process_queue_item(self, item: NODBQueueItem) -> QueueItemResult | None:
        self.metadb.upsert_dataset(item.data)
