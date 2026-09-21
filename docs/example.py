import typing as t

from medsutil.ocproc2 import ParentRecord
from nodb.queue import NODBQueueItem
from pipeman.processing.payload_worker import BatchWorkflowWorker
from pipeman.processing.payloads import BatchPayload
from pipeman.processing.queue_worker import QueueItemResult


class ExampleWorker(BatchWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="some_unique_name",    # A unique name to identify this code
            process_version="1.0",              # version number
            **kwargs                            # additional parameters passed on start-up
        )

    # called when the worker is first built
    def on_start(self): ...

    # called after a queue item has been loaded from the database
    def before_queue_item(self, item: NODBQueueItem): ...

    # called to process a queue item and its payload
    def process_payload(self, payload: BatchPayload) -> t.Optional[QueueItemResult]: ...

    # called after a queue item has been processed and the result saved to the database
    def after_queue_item(self, queue_item: NODBQueueItem, exception: Exception | None): ...

    # called when the worker is shutting down
    def on_exit(self, exception: Exception = None): ...

    # call this to renew the lock
    def renew_item(self): ...

    # call this to check if we need to stop
    # will raise HaltInterrupt if we do
    def breakpoint(self): ...
