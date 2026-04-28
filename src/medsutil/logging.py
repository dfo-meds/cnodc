import os
import queue
import logging
import multiprocessing
import signal
from logging.handlers import QueueHandler

import zrlog
from autoinject import injector

import medsutil.json as json
from medsutil.email import EmailController


class EmailHandler(logging.Handler):

    emails: EmailController = None

    @injector.construct
    def __init__(self, level=logging.NOTSET, subject_line=None, immediate: bool = False):
        super().__init__(level)
        self.immediate = immediate
        self._subject_formatter = logging.Formatter(subject_line or "Event - %(levelname)s")

    def emit(self, record: logging.LogRecord):
        self.emails.send_email(
            subject=self._subject_formatter.format(record),
            to_emails=self.emails.admin_emails,
            message_txt=self.format(record),
            immediate=self.immediate
        )


class MultiprocessingQueueListener(multiprocessing.Process):

    def __init__(self, q: queue.Queue, logging_config: str):
        super().__init__(daemon=True)
        self._logging_config = logging_config
        self._queue = q
        self._end_flag = multiprocessing.Event()

    def _noop(self, *args, **kwargs): ...

    def run(self):
        for sig in ('SIGINT', 'SIGTERM', 'SIGBREAK', 'SIGQUIT'):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), self._noop)
        import logging.config as lc
        zrlog.init_logging()
        lc.dictConfig(json.load_dict(self._logging_config))

        root = logging.getLogger()
        while not self._end_flag.is_set():
            try:
                record = self._queue.get(False, 0.5)
                root.handle(record)
            except queue.Empty:
                ...

    def stop(self):
        self._end_flag.set()
        self.join()


def init_subprocess_handler(q: multiprocessing.Queue,
                            logging_config: dict) -> MultiprocessingQueueListener:
    mql = MultiprocessingQueueListener(q, json.dumps(logging_config))
    mql.start()
    return mql


def init_as_subprocess(queue: multiprocessing.Queue):
    for logger_name, logger in logging.root.manager.loggerDict.items():
        if isinstance(logger, logging.Logger) and logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)
    for handler in logging.getLogger().handlers:
        logging.getLogger().removeHandler(handler)
    logging.getLogger().addHandler(MultiprocessingQueueHandler(queue))


class MultiprocessingQueueHandler(QueueHandler):

    def __init__(self, queue, level=logging.NOTSET):
        super().__init__(queue)
        self.setLevel(level)

    def prepare(self, record):
        r = super().prepare(record)
        r.process_id = os.getpid()
        for x in dir(record):
            if x.startswith('_'):
                continue
            if hasattr(r, x):
                continue
        return r
