import datetime
import functools
import logging
import pathlib
import shutil
import threading
import traceback
import uuid
import typing as t
from cnodc.nodb import NODBQueueItem, NODBUploadWorkflow
from cnodc.processing.control.base import BaseWorker
from cnodc.util import HaltFlag
from cnodc.util.awaretime import AwareDateTime
from helpers.db_mock import DummyNODB


class WorkflowTestResult(logging.Handler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_events: list[tuple[datetime.datetime, str, str, dict]] = []
        self.logs: list[logging.LogRecord] = []
        self._log = logging.getLogger('workflow.test_result')

    def start(self):
        logging.getLogger().addHandler(self)
        self.setLevel(logging.DEBUG)

    def close(self):
        logging.getLogger().removeHandler(self)

    def emit(self, record):
        self.logs.append(record)

    def pretty_print(self):
        entries = []
        for log in self.logs:
            ts = datetime.datetime.fromtimestamp(log.created).astimezone()
            s = f"[{log.levelname.upper()} {ts.strftime('%H:%M:%S.%f')} {log.name}] {log.msg}"
            if log.exc_info and log.exc_info[2]:
                s += f"\n{'='*50}\n"
                s += "".join(traceback.format_tb(log.exc_info[2]))
                s += f"\n{log.exc_info[0].__name__}: {str(log.exc_info[1])}\n"
                s += f"{'='*50}"

            entries.append((ts, s))
        entries.sort(key=lambda x: x[0])
        for entry in entries:
            print(entry[1])

    def update_worker_config(self, wc: dict):
        wc = wc.copy()
        self._register_worker_hook(wc, 'on_start')
        self._register_worker_hook(wc, 'on_exit')
        self._register_worker_hook(wc, 'before_cycle')
        self._register_worker_hook(wc, 'after_cycle')
        self._register_worker_hook(wc, 'before_queue_item')
        self._register_worker_hook(wc, 'after_queue_item')
        self._register_worker_hook(wc, 'on_retry')
        self._register_worker_hook(wc, 'on_failure')
        self._register_worker_hook(wc, 'on_success')
        self._register_worker_hook(wc, 'after_retry')
        self._register_worker_hook(wc, 'after_failure')
        self._register_worker_hook(wc, 'after_success')
        self._register_worker_hook(wc, 'before_message')
        self._register_worker_hook(wc, 'before_record')
        self._register_worker_hook(wc, 'after_decode_success')
        self._register_worker_hook(wc, 'after_decode_error')
        return wc

    def _register_worker_hook(self, wc, hook_name):
        hook_key = f'hook_{hook_name}'
        cb = functools.partial(self._add_worker_event, event_name=hook_name)
        if hook_key not in wc:
            wc[hook_key] = [cb]
        elif isinstance(wc[hook_key], (str, callable)):
            wc[hook_key] = [cb, wc[hook_key]]
        else:
            wc[hook_key].append(cb)

    def _add_worker_event(self, worker: BaseWorker, event_name: str, **kwargs):
        self.worker_events.append((AwareDateTime.utcnow(), worker._process_name, event_name, kwargs))
        self._log.debug(f'{worker._process_name}: {event_name}({', '.join(f'{k}: {kwargs[k]}' for k in kwargs)})')


class MockWorkflow:

    def __init__(self, nodb: DummyNODB, halt_flag=None, end_flag=None):
        self.nodb = nodb
        self.halt_flag = halt_flag or HaltFlag(threading.Event())
        self.end_flag = end_flag or HaltFlag(threading.Event())
        self._worker_info: list[tuple[type[BaseWorker], dict]] = []
        self._workers: list[BaseWorker] = []
        self._last_items: t.Optional[set[str]] = None

    def reset(self):
        with self.nodb as db:
            db.reset()
        self._worker_info = []
        self._workers = []
        self._last_items = None

    def add_workflow(self,
                     workflow_name: str,
                     working_dir: t.Union[str,dict],
                     processing_steps: list[str],
                     validation: str = None,
                     additional_dirs: list[t.Union[str, dict]] = None):
        wf = NODBUploadWorkflow()
        wf.workflow_name = workflow_name
        wf.set_config({
            'label': {'und': 'TEST'},
            'working_target': {
                'directory': working_dir,
            } if isinstance(working_dir, str) else working_dir,
            'additional_targets': [
                {'directory': x} if isinstance(x, str) else x
                for x in additional_dirs or []
            ],
            'processing_steps': {
                f'step{idx}': {'order': idx, 'name': x}
                for idx, x in enumerate(processing_steps)
            },
            'validation': validation,
        })
        with self.nodb as db:
            db.insert_object(wf)

    def add_worker(self, worker_cls: type[BaseWorker], worker_config: dict = None):
        self._worker_info.append((worker_cls, worker_config or {}))

    def test_file(self, source_file: pathlib.Path, target_directory: pathlib.Path):
        shutil.copy(source_file, target_directory / source_file.name)
        return self._run_test()

    def test_queue_item(self, queue_item: NODBQueueItem) -> WorkflowTestResult:
        with self.nodb as db:
            db.insert_object(queue_item)
            db.commit()
        return self._run_test()

    def _run_test(self):
        test_result = WorkflowTestResult()
        test_result.start()
        self._build_workers(test_result)
        while self._have_items_changed():
            self._run_all_workers_once()
        test_result.close()
        return test_result

    def _run_all_workers_once(self):
        for worker in self._workers:
            worker.run_once()

    def _have_items_changed(self):
        current_items = set()
        with self.nodb as db:
            for item in db.table(NODBQueueItem.TABLE_NAME):
                current_items.add(f"{item.queue_name}_{item.queue_uuid}_{item.status.value}")
        if self._last_items is None or current_items.difference(self._last_items):
            self._last_items = current_items
            return True
        else:
            return False

    def _build_workers(self, test_result: WorkflowTestResult):
        for worker_cls, worker_config in self._worker_info:
            worker = worker_cls(
                _process_uuid=str(uuid.uuid4()),
                _config=test_result.update_worker_config(worker_config),
                _halt_flag=self.halt_flag,
                _end_flag=self.end_flag
            )
            if hasattr(worker, 'nodb'):
                worker.nodb = self.nodb
            self._workers.append(worker)



