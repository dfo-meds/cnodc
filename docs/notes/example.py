import typing as t

from nodb.queue import NODBQueueItem
from pipeman.processing.payload_worker import BatchWorkflowWorker
from pipeman.processing.payloads import BatchPayload
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.processing.scheduled_task import ScheduledTask


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


class ExampleTask(ScheduledTask):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="some_unique_name",    # A unique name to identify this code
            process_version="1.0",              # version number
            **kwargs                            # additional parameters passed on start-up
        )

        # you can set a delay (this is 3 hours after last run finished)
        # schedule_mode: from_completion
        # delay_seconds: 10800

        # or from last start (but only one runs at a time)
        # schedule_mode: from_start
        # delay_seconds: 10800

        # or using linux cron
        # schedule_mode: cron
        # cron: 12 19 * * *  # this is every night at (approximately) 19:12:00

    # called when the worker is first built
    def on_start(self): ...

    # called when the worker is shutting down
    def on_exit(self, exception: Exception = None): ...

    # called at the appropriate time
    def execute(self): ...

    # call this to check if we need to stop
    # will raise HaltInterrupt if we do
    def breakpoint(self): ...
