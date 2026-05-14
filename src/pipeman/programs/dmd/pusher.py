import typing as t

from autoinject import injector

from nodb import NODBQueueItem
from pipeman.processing.queue_worker import QueueWorker, QueueItemResult
from pipeman.programs.dmd import dmd as dmd


class DMDMetadataPushWorker(QueueWorker):

    metadb: dmd.DataManagerController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="dmd_metadata_pusher",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'dmd_metadata_push',
        })

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        self.metadb.upsert_dataset(item.data)
