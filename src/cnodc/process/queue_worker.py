from .base import BaseProcess
from cnodc.util import CNODCError, HaltInterrupt
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
        self.set_defaults({
            "queue_name": None,
            "delay_time_seconds": 0.25,
            "retry_delay_seconds": 0,
            "delay_factor": 2,
            "max_delay_time_seconds": 128,
        })
        self._db: t.Optional[NODBControllerInstance] = None

    @injector.inject
    def _run(self, nodb: NODBController = None):
        if not self.get_config("queue_name"):
            raise CNODCError("No queue specified for a queue worker")
        self._current_delay_time = self.get_config("delay_time_seconds")
        self._app_id = str(uuid.uuid4())
        with nodb as db:
            try:
                self._db = db
                while self.continue_loop():
                    queue_item = None
                    try:
                        queue_item = db.fetch_next_queue_item(self.get_config("queue_name"), self._app_id)
                        if queue_item is not None:
                            self.is_working.set()
                            self._process_result(queue_item, self.process_queue_item(queue_item))
                            self.is_working.clear()
                            self._current_delay_time = self.get_config("delay_time_seconds")
                        else:
                            time.sleep(self._delay_time())
                    except CNODCError as ex:
                        if ex.is_recoverable:
                            if queue_item:
                                self._process_result(queue_item, structures.QueueItemResult.RETRY)
                                self._log.exception(f"Recoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                            else:
                                self._log.exception("Recoverable exception occurred during queue item processing")
                        else:
                            if queue_item:
                                self._process_result(queue_item, structures.QueueItemResult.FAILED)
                                self._log.exception(f"Unrecoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                            raise ex
                    except (KeyboardInterrupt, HaltInterrupt) as ex:
                        self._process_result(queue_item, structures.QueueItemResult.RETRY)
                        self._log.exception(f"Processing halt reqeusted")
                        break
                    except Exception as ex:
                        if queue_item:
                            self._process_result(queue_item, structures.QueueItemResult.FAILED)
                            self._log.exception(f"Unrecoverable exception occurred during queue item processing [{queue_item.queue_uuid}]")
                        raise ex
            finally:
                self._db = None

    def _process_result(self, queue_item: structures.NODBQueueItem, result: t.Optional[structures.QueueItemResult]):
        if result is None or result == structures.QueueItemResult.SUCCESS:
            self._db.mark_queue_item_complete(queue_item)
        elif result == structures.QueueItemResult.FAILED:
            self._db.mark_queue_item_failed(queue_item)
        else:
            self._db.release_queue_item(queue_item, self.get_config("retry_delay_seconds"))
        self._db.commit()

    def _delay_time(self) -> float:
        curr_time = self._current_delay_time
        self._current_delay_time *= self.get_config("delay_factor")
        _max_time = self.get_config("max_delay_time_seconds")
        if self._current_delay_time >= _max_time:
            self._current_delay_time = _max_time
        return curr_time

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        raise NotImplementedError()
