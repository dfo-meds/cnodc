from .base import BaseProcess
from cnodc.util import CNODCError
from autoinject import injector
import uuid
from cnodc.nodb import NODBController, NODBControllerInstance
import cnodc.nodb.structures as structures
import time
import typing as t


class QueueWorker(BaseProcess):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_name = None
        self._app_id = None
        self._current_delay_time = None

    @injector.inject
    def _run(self, nodb: NODBController = None):
        self._queue_name = self.get_config("queue_name", None)
        self._current_delay_time = self.get_config("delay_time_seconds", 0.25)
        self._app_id = str(uuid.uuid4())
        if not self._queue_name:
            raise CNODCError("No queue specified for a queue worker")
        with nodb as db:
            while self.check_continue():
                queue_item = None
                try:
                    queue_item = db.fetch_next_queue_item(self._queue_name, self._app_id)
                    if queue_item is not None:
                        self.is_working.set()
                        self._process_result(db, queue_item, self._process_queue_item(db, queue_item))
                        self.is_working.clear()
                        self._current_delay_time = self.get_config("delay_time_seconds", 0.25)
                    else:
                        time.sleep(self._delay_time())
                except CNODCError as ex:
                    if ex.is_recoverable:
                        if queue_item:
                            self._process_result(db, queue_item, structures.QueueItemResult.RETRY)
                            self._log.exception(f"Recoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                        else:
                            self._log.exception("Recoverable exception occurred during queue item processing")
                    else:
                        if queue_item:
                            self._process_result(db, queue_item, structures.QueueItemResult.FAILED)
                            self._log.exception(f"Unrecoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                        raise ex
                except Exception as ex:
                    if queue_item:
                        self._process_result(db, queue_item, structures.QueueItemResult.FAILED)
                        self._log.exception(f"Unrecoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                    raise ex

    def _process_result(self, db: NODBControllerInstance, queue_item: structures.NODBQueueItem, result: t.Optional[structures.QueueItemResult]):
        if result is None or result == structures.QueueItemResult.SUCCESS:
            db.mark_queue_item_complete(queue_item)
        elif result == structures.QueueItemResult.FAILED:
            db.mark_queue_item_failed(queue_item)
        else:
            db.release_queue_item(queue_item, self.get_config("retry_delay_seconds", 5))
        db.commit()

    def _delay_time(self) -> float:
        curr_time = self._current_delay_time
        self._current_delay_time *= self.get_config("delay_factor", 2)
        _max_time = self.get_config("max_delay_time_seconds", 16)
        if self._current_delay_time >= _max_time:
            self._current_delay_time = _max_time
        return curr_time

    def _process_queue_item(self, db: NODBControllerInstance, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        pass
