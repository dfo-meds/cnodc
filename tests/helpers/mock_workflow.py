import datetime
import functools
import logging
import math
import pathlib
import shutil
import threading
import traceback
import uuid
import typing as t
from unittest import SkipTest

import zrlog

from nodb import NODBQueueItem, NODBUploadWorkflow, NODB
from pipeman.processing.base_worker import BaseWorker
from medsutil.halts import HaltFlag
from medsutil.awaretime import AwareDateTime
from tests.helpers.base_test_case import BaseTestCase
import dataclasses


@dataclasses.dataclass
class WorkerEvent:
    event_time: datetime.datetime
    process_name: str
    process_version: str
    process_id: str
    event_name: str
    keyword_args: dict


class WorkflowTestResult(logging.Handler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_events: list[WorkerEvent] = []
        self.logs: list[logging.LogRecord] = []
        self._log = zrlog.get_logger('workflow.test_result')

    def start(self):
        logging.getLogger().addHandler(self)
        self.setLevel(logging.DEBUG)

    def close(self):
        logging.getLogger().removeHandler(self)

    def emit(self, record):
        self.logs.append(record)

    def _recursive_traceback(self, e: BaseException):
        s = ''
        if e.__cause__ is not None:
            s += self._recursive_traceback(e.__cause__)
            s += '\n the above was a direct cause of the following exception:\n'
        if e.__traceback__:
            s += ''.join(traceback.format_tb(e.__traceback__))
        s += f"\n{e.__class__.__name__}: {str(e)}"
        return s

    def pretty_print(self):
        entries: list[tuple[datetime.datetime, str, int]] = []
        for idx, log in enumerate(self.logs):
            ts = datetime.datetime.fromtimestamp(log.created).astimezone()
            s = f"[{ts.strftime('%H:%M:%S.%f')}] {log.getMessage()} [{log.name}:{log.levelname.upper()}]"
            if log.exc_info and log.exc_info[1]:
                s += f"\n{'='*50}\n"
                s += self._recursive_traceback(log.exc_info[1])
                s += f"{'='*50}"
            entries.append((ts, s, idx))
        for idx, event in enumerate(self.worker_events):
            entries.append((event.event_time ,f"[{event.event_time.strftime('%H:%M:%S.%f')}] {event.event_name} [{event.process_name} {event.process_version}]", idx))
        zeros = math.ceil(math.log10(len(entries)))
        def _sort(a: tuple[datetime.datetime, str, int]) -> int:
            return int(a[0].timestamp() * (10 ** zeros)) + a[2]
        entries.sort(key=_sort)
        for entry in entries:
            print(entry[1])

    def update_worker_config(self, worker: BaseWorker, wc: dict):
        wc = wc.copy()
        for event_name in worker.possible_events():
            self._register_worker_hook(wc, event_name)
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

    def _add_worker_event(self, worker: BaseWorker, *args, event_name: str, **kwargs):
        self._log.info(f"Saw event [%s] from [%s %s]", event_name, worker.process_name, worker.process_version)
        self.worker_events.append(WorkerEvent(
            event_time=AwareDateTime.utcnow(),
            process_name=worker.process_name,
            process_version=worker.process_version,
            process_id=worker.process_uuid,
            event_name=event_name,
            keyword_args=kwargs
        ))


class MockWorkflow:

    def __init__(self, nodb: NODB, halt_flag=None, end_flag=None):
        self.nodb = nodb
        self.halt_flag = halt_flag or HaltFlag(threading.Event())
        self.end_flag = end_flag or HaltFlag(threading.Event())
        self._worker_info: list[tuple[type[BaseWorker], dict]] = []
        self._workers: list[BaseWorker] = []
        self._last_items: t.Optional[set[str]] = None
        self._log = zrlog.get_logger("cnodc.testing.mock_workflow")

    def add_workflow(self,
                     workflow_name: str,
                     working_dir: t.Union[str,dict],
                     processing_steps: list[t.Union[str, dict]],
                     validation: str = None,
                     additional_dirs: list[t.Union[str, dict]] = None):
        wf = NODBUploadWorkflow(workflow_name=workflow_name)
        wf.set_config({
            'label': {'und': 'TEST'},
            'working_target': {
                'directory': working_dir,
            } if isinstance(working_dir, str) else working_dir,
            'additional_targets': [
                {'directory': x} if isinstance(x, str) else x
                for x in additional_dirs or []
            ],
            'steps': {
                f'step{idx}': {'order': idx, 'name': x} if not isinstance(x, dict) else x
                for idx, x in enumerate(processing_steps)
            },
            'validation': validation,
            'accept_user_filename': True,
        })
        with self.nodb as db:
            check_wf = NODBUploadWorkflow.find_by_name(db, workflow_name)
            if check_wf is not None:
                db.delete_object(check_wf)
                db.commit()
            db.insert_object(wf)
            db.commit()

    def add_worker(self, worker_cls: type[BaseWorker], worker_config: dict = None):
        self._worker_info.append((worker_cls, worker_config or {}))

    def test_file(self, source_file: pathlib.Path, target_directory: pathlib.Path, max_cycles: int = None):
        shutil.copy(source_file, target_directory / source_file.name)
        return self._run_test(max_cycles)

    def test_queue_item(self, queue_item: NODBQueueItem, max_cycles: int = None) -> WorkflowTestResult:
        with self.nodb as db:
            db.insert_object(queue_item)
            db.commit()
        return self._run_test(max_cycles)

    def _run_test(self, max_cycles: int = None):
        max_cycles = max_cycles or 50
        test_result = WorkflowTestResult()
        test_result.start()
        self._build_workers(test_result)
        while self._have_items_changed() and max_cycles > 0:
            self._run_all_workers_once()
            max_cycles -= 1
        test_result.close()
        return test_result

    def _run_all_workers_once(self):
        for worker in self._workers:
            worker.run_once()

    def _have_items_changed(self):
        current_items = set()
        with self.nodb as db:
            current_items.update('__'.join(item) for item in db.load_queue_items())
        if self._last_items:
            self._log.notice('Current items [%s]', current_items.difference(self._last_items))
        else:
            self._log.notice('Current items [%s]', current_items.difference(set()))
        if self._last_items is None or current_items.difference(self._last_items):
            self._last_items = current_items
            return True
        else:
            return False

    def _build_workers(self, test_result: WorkflowTestResult):
        for worker_cls, worker_config in self._worker_info:
            worker = worker_cls(
                _process_uuid=str(uuid.uuid4()),
                _config={},
                _halt_flag=self.halt_flag,
                _end_flag=self.end_flag
            )
            worker._config = test_result.update_worker_config(worker, worker_config)
            if hasattr(worker, 'nodb'):
                worker.nodb = self.nodb
            self._workers.append(worker)


class BaseWorkflowTestCase(BaseTestCase):

    def assertEventDidOccur(self, process_name: str, event_name: str, msg: str = None):
        for x in self.workflow_result.worker_events:
            if x.event_name == event_name and x.process_name == process_name:
                return x
        raise self.failureException(msg or f"Event {process_name}:{event_name} not found")


    def assertEventDidNotOccur(self, process_name: str, event_name: str, msg: str = None):
        for x in self.workflow_result.worker_events:
            if x.event_name == event_name and x.process_name == process_name:
                raise self.failureException(msg or f"Event {process_name}:{event_name} found unexpectedly!")

    @classmethod
    def setUpClass(cls):
        try:
            super().setUpClass()
            workflow = MockWorkflow(cls.real_nodb)
            cls.workflow_result = cls.build_and_run_workflow(workflow)
        except Exception as ex:
            zrlog.get_logger('test_glider_decode').exception(f'An error occurred while processing this workflow')
            raise SkipTest('an error occurred during start')

    @classmethod
    def build_and_run_workflow(cls, workflow: MockWorkflow) -> WorkflowTestResult: ...
