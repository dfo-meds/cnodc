"""
    Queue-based workers work off a database queue. Queue items are stored in the
    nodb_queues table and organized by queue_name, then processed by priority (descending)
    and then created date.

    Queue items have a "unique_item_name" which, when non-null, will prevent two items
    with the same value for that field from being locked at the same time.
"""
from cnodc.process.base import BaseWorker
from cnodc.util import CNODCError, HaltInterrupt
from autoinject import injector
import uuid
from cnodc.nodb import NODBController, NODBControllerInstance
import cnodc.nodb.structures as structures
import typing as t
from cnodc.nodb.controller import NODBError
import enum


class QueueItemResult(enum.Enum):
    """Represents the result of processing a queue item (used to update the status)"""

    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    RETRY = 'RETRY'
    HANDLED = 'HANDLED'


class QueueWorker(BaseWorker):
    """Execute a process on every queue item."""

    nodb: NODBController = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_name = None
        self._app_id = None
        self._current_delay_time = None
        self._current_item: t.Optional[structures.NODBQueueItem] = None
        self.set_defaults({
            "queue_name": None,
            "delay_time_seconds": 0.25,
            "retry_delay_seconds": 0,
            "delay_factor": 2,
            "max_delay_time_seconds": 128,
            'deprioritize_failures': True
        })
        self._db: t.Optional[NODBControllerInstance] = None

    def _run(self):
        """Grab and process a queue item repeatedly"""
        if not self.get_config("queue_name"):
            raise CNODCError("No queue specified for a queue worker")
        self._current_delay_time = self.get_config("delay_time_seconds")
        self._app_id = str(uuid.uuid4())
        while self.continue_loop():
            with self.nodb as db:
                try:
                    # This lets us re-use the database connection later instead of making a second one.
                    self._db = db
                    if not self._process_next_queue_item():
                        self.responsive_sleep(self._delay_time())
                finally:
                    self._db = None

    def _process_next_queue_item(self) -> bool:
        """Process a queue item and return True if there was an item otherwise False."""
        self._log.debug(f"Checking for new items in [{self.get_config('queue_name')}]")
        self._current_item = None
        try:
            # Get an item
            self._current_item = self._db.fetch_next_queue_item(
                queue_name=self.get_config("queue_name"),
                app_id=self._app_id
            )
            # Run the process on the item
            if self._current_item is not None:
                self.before_item()
                self._current_delay_time = self.get_config("delay_time_seconds")
                self._process_result(self._current_item, self.process_queue_item(self._current_item))
                return True
            return False
        except CNODCError as ex:
            # NB: NODB errors require us to rollback so that we can fix them.
            if isinstance(ex, NODBError):
                self._db.rollback()
            # Recoverable errors may be fixable later, so we requeue the item for retrying
            if ex.is_recoverable:
                self._process_result(self._current_item, QueueItemResult.RETRY, ex)
                return True
            # Non-recoverable mean the entire item should fail
            else:
                self._process_result(self._current_item, QueueItemResult.FAILED, ex)
        except (KeyboardInterrupt, HaltInterrupt) as ex:
            self._process_result(self._current_item, QueueItemResult.RETRY)
            self._log.exception(f"Processing halt reqeusted")
            raise ex
        except Exception as ex:
            self._process_result(self._current_item, QueueItemResult.FAILED, ex)
        finally:
            self.after_item()
            self._current_item = None

    def _process_result(self, queue_item: structures.NODBQueueItem, result: t.Optional[QueueItemResult], ex: Exception = None):
        """Handle the result of calling the queue processing function."""
        if queue_item is not None:
            if ex is not None:
                self._log.exception(f"An exception occurred while processing {queue_item.queue_uuid}: {str(ex)}")
            if result is None or result == QueueItemResult.SUCCESS:
                queue_item.mark_complete(self._db)
                self.on_success(queue_item)
                after = self.after_success
            elif result == QueueItemResult.HANDLED:
                self.on_success(queue_item)
                after = self.after_success
            elif result == QueueItemResult.FAILED:
                queue_item.mark_failed(self._db)
                self.on_failure(queue_item)
                after = self.after_failure
            else:
                queue_item.release(
                    self._db,
                    release_in_seconds=self.get_config("retry_delay_seconds"),
                    reduce_priority=self.get_config('deprioritize_failures')
                )
                self.on_retry(queue_item)
                after = self.after_retry
            self._db.commit()
            after(queue_item)
        elif ex is not None:
            self._log.exception(f"An exception occurred while retrieving a queue item: {str(ex)}")

    def on_retry(self, queue_item: structures.NODBQueueItem):
        """Override to add logic when an item is about to be released to be retried."""
        pass

    def on_failure(self, queue_item: structures.NODBQueueItem):
        """Override to add logic when an item is about to be marked as a failure."""
        pass

    def on_success(self, queue_item: structures.NODBQueueItem):
        """Override to add logic when an item is about to be marked as a success."""
        pass

    def after_retry(self, queue_item: structures.NODBQueueItem):
        """Override to add logic after an object has been released to be retried (i.e. after commit)."""
        pass

    def after_failure(self, queue_item: structures.NODBQueueItem):
        """Override to add logic after an object has been marked as a failure (i.e. after commit)."""
        pass

    def after_success(self, queue_item: structures.NODBQueueItem):
        """Override to add logic after an object has been marked as a success (i.e. after commit)."""
        pass

    def _delay_time(self) -> float:
        """Calculate the delay time"""
        curr_time = self._current_delay_time
        self._current_delay_time *= self.get_config("delay_factor")
        _max_time = self.get_config("delay_max_time_seconds")
        if self._current_delay_time >= _max_time:
            self._current_delay_time = _max_time
        return curr_time

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[QueueItemResult]:
        """Handle a specific queue item."""
        raise NotImplementedError
