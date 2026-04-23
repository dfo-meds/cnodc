"""
    Queue-based workers work off a database queue. Queue items are stored in the
    nodb_queues table and organized by queue_name, then processed by priority (descending)
    and then created date.

    Queue items have a "unique_item_name" which, when non-null, will prevent two items
    with the same value for that field from being locked at the same time.
"""
import uuid
import typing as t
import enum

from autoinject import injector

from pipeman.processing.base_worker import BaseWorker

import nodb as nodb_
from pipeman.exceptions import CNODCError
from medsutil.exceptions import HaltInterrupt, CodedError
from nodb import NODBQueueItem


class QueueItemResult(enum.Enum):
    """Represents the result of processing a queue item (used to update the status)"""

    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    RETRY = 'RETRY'
    HANDLED = 'HANDLED'


class QueueWorker(BaseWorker):
    """Execute a process on every queue item."""

    nodb: nodb_.NODB = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults({
            "queue_name": None,
            "delay_time_seconds": 0.25,
            "retry_delay_seconds": 0,
            "delay_factor": 2,
            "max_delay_time_seconds": 128,
            'deprioritize_failures': False,
            'allow_queue_item_config': True,
        })
        self.add_events(['before_queue_item', 'after_queue_item', 'on_success', 'on_failure', 'on_retry' ,'after_success', 'after_failure', 'after_retry'])
        self._queue_name = None
        self._app_id = None
        self._current_delay_time = None
        self._current_item: t.Optional[NODBQueueItem] = None
        self._db: t.Optional[nodb_.NODBInstance] = None

    @property
    def db(self) -> nodb_.NODBInstance:
        if self._db is None:
            raise CNODCError('Access to db when not processing queue item', 'QUEUE', 2000)
        return self._db

    def _run_once(self) -> float:
        with self.nodb as db:
            try:
                self._db = db
                if not self._process_next_queue_item():
                    return self._delay_time()
                else:
                    self._current_delay_time = self.get_config("delay_time_seconds")
                    return 0
            finally:
                self._db = None

    def _fetch_next_queue_item(self) -> NODBQueueItem | None:
        self._log.debug('Checking for queue items in %s', self._queue_name)
        return self.db.fetch_next_queue_item(
            queue_name=self._queue_name,
            app_id=self._app_id
        )

    def _process_next_queue_item(self) -> bool:
        """Process a queue item and return True if there was an item otherwise False."""
        self._current_item = None
        exc: Exception | None = None
        self.before_cycle()
        try:
            # Get an item
            self._current_item = self._fetch_next_queue_item()
            # Run the process on the item
            if self._current_item is not None:
                self._log.debug("found item [%s]", self._current_item.queue_uuid)
                if self.get_config('allow_queue_item_config', True):
                    self.set_cycle_config(self._current_item.get_worker_config(self.process_name, self.process_version))
                else:
                    self.set_cycle_config({})
                self.before_queue_item(self._current_item)
                self._process_result(self._current_item, self.process_queue_item(self._current_item))
                return True
        except CodedError as ex:
            exc = ex
            # NB: NODB errors require us to rollback so that we can fix them.
            if isinstance(ex, nodb_.NODBError):
                self.db.rollback()
            # Recoverable errors may be fixable later, so we requeue the item for retrying
            if ex.is_transient:
                self._process_result(self._current_item, QueueItemResult.RETRY, ex)
            # Non-recoverable mean the entire item should fail
            else:
                self._process_result(self._current_item, QueueItemResult.FAILED, ex)
        except Exception as ex:
            exc = ex
            self._process_result(self._current_item, QueueItemResult.FAILED, ex)
        except (KeyboardInterrupt, HaltInterrupt) as ex:
            exc = ex
            self._process_result(self._current_item, QueueItemResult.RETRY)
            self._log.critical(f"HaltInterrupt detected")
            raise
        finally:
            if self._current_item is not None:
                self.after_queue_item(self._current_item, exc)
            self.after_cycle()
            self._current_item = None
        return False

    def autocomplete(self, queue_item):
        self._log.trace('Autocompleting queue item [%s]', queue_item.queue_uuid)
        queue_item.mark_complete(self.db)

    def renew_item(self):
        if self._current_item is not None:
            self._log.trace('Renewing queue item [%s]', self._current_item.queue_uuid)
            self._current_item.renew(self.db)

    def _process_result(self, queue_item: t.Optional[NODBQueueItem], result: t.Optional[QueueItemResult], ex: Exception = None):
        """Handle the result of calling the queue processing function."""
        self._log.trace('Result of processing [%s] is [%s]', queue_item.queue_uuid if queue_item else None, result)
        if queue_item is not None:
            if ex is not None:
                self._log.exception(f"An exception occurred while processing [%s]: %s: %s", queue_item.queue_uuid, type(ex), str(ex))
            if result is None or result == QueueItemResult.SUCCESS:
                self.autocomplete(queue_item)
                self.on_success(queue_item)
                after = self.after_success
            elif result == QueueItemResult.HANDLED:
                self.on_success(queue_item)
                after = self.after_success
            elif result == QueueItemResult.FAILED:
                queue_item.mark_failed(self.db)
                self.on_failure(queue_item, ex)
                after = self.after_failure
            else:
                queue_item.release(
                    self.db,
                    release_in_seconds=self.get_config("retry_delay_seconds"),
                    reduce_priority=self.get_config('deprioritize_failures')
                )
                self.on_retry(queue_item, ex)
                after = self.after_retry
            self.db.commit()
            after(queue_item, ex)
        elif ex is not None:
            self._log.exception(f"An exception occurred while retrieving a queue item: %s: %s", type(ex), str(ex))

    def before_queue_item(self, queue_item: NODBQueueItem):
        self.run_hook('before_queue_item', queue_item=queue_item)

    def after_queue_item(self, queue_item: NODBQueueItem, exception: Exception | None):
        self.run_hook('after_queue_item', queue_item=queue_item, exception=exception)

    def on_retry(self, queue_item: NODBQueueItem, exception: Exception | None):
        """Override to add logic when an item is about to be released to be retried."""
        self.run_hook('on_retry', queue_item=queue_item, exception=exception)

    def on_failure(self, queue_item: NODBQueueItem, exception: Exception | None):
        """Override to add logic when an item is about to be marked as a failure."""
        self.run_hook('on_failure', queue_item=queue_item, exception=exception)

    def on_success(self, queue_item: NODBQueueItem):
        """Override to add logic when an item is about to be marked as a success."""
        self.run_hook('on_success', queue_item=queue_item)

    def after_retry(self, queue_item: NODBQueueItem, exception: Exception | None):
        """Override to add logic after an object has been released to be retried (i.e. after commit)."""
        self.run_hook('after_retry', queue_item=queue_item, exception=exception)

    def after_failure(self, queue_item: NODBQueueItem, exception: Exception | None):
        """Override to add logic after an object has been marked as a failure (i.e. after commit)."""
        self.run_hook('after_failure', queue_item=queue_item, exception=exception)

    def after_success(self, queue_item: NODBQueueItem, _):
        """Override to add logic after an object has been marked as a success (i.e. after commit)."""
        self.run_hook('after_success', queue_item=queue_item)

    def on_start(self):
        self._queue_name = self.get_config('queue_name', None)
        if not self._queue_name:
            raise CNODCError("No queue specified for a queue worker", 'QUEUE-WORKER', 1000)
        self._current_delay_time = self.get_config("delay_time_seconds", 0.25)
        self._app_id = str(uuid.uuid4())
        super().on_start()

    def _delay_time(self) -> float:
        """Calculate the delay time"""
        curr_time = self._current_delay_time
        self._current_delay_time *= self.get_config("delay_factor")
        _max_time = self.get_config("max_delay_time_seconds")
        if self._current_delay_time >= _max_time:
            self._current_delay_time = _max_time
        self._log.trace("Next delay time is [%s]", self._current_delay_time)
        return curr_time

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        """Handle a specific queue item."""
        raise NotImplementedError  # pragma: no coverage
