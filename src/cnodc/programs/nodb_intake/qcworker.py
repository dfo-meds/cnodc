import datetime
import enum
import hashlib
import uuid

from cnodc.nodb import LockType
from cnodc.process.payload_worker import WorkflowWorker
import typing as t
import cnodc.nodb.structures as structures
from cnodc.process.queue_worker import QueueItemResult
from cnodc.qc.base import BaseTestSuite, QCTestRunner
from cnodc.util import dynamic_object, CNODCError
from cnodc.workflow.workflow import BatchPayload, WorkflowPayload, SourceFilePayload
import cnodc.ocproc2 as ocproc2


class BatchOutcome(enum.Enum):

    NEXT_QUEUE = 'N'
    REVIEW_QUEUE = 'R'


class NODBQCWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="qc_worker",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'qc_tests': [],
            'max_batch_size': None,
            'max_buffer_size': None,
            'target_buffer_size': None,
            'recheck_queue': None,
            'next_queue': "workflow_continue",
            'review_queue': 'nodb_manual_review',
        })
        self._test_runner = self._build_test_runner(self.get_config('qc_tests', []))

    def _build_test_runner(self, qc_tests: list[dict]) -> QCTestRunner:
        tests = []
        for qc_test_def in qc_tests:
            kwargs = (qc_test_def['kwargs'] or {}) if 'kwargs' in qc_test_def else {}
            kwargs['test_runner_id'] = self._process_uuid
            tests.append(dynamic_object(qc_test_def['class'])(**kwargs))
        return QCTestRunner(tests)

    def submit_existing_batch(self, batch_id: str, batch_outcome: BatchOutcome, group_key: t.Optional[str] = None):
        payload = self.batch_payload_from_uuid(batch_id)
        queue_name = self.get_config('next_queue')
        if batch_outcome == BatchOutcome.REVIEW_QUEUE:
            payload.set_followup_queue(queue_name)
            payload.set_metadata('current-qc-tests', self._test_runner.test_names())
            payload.set_metadata('recheck-queue', self.get_config('recheck_queue') or self.get_config('queue_name'))
            payload.set_metadata('escalation-queue', self.get_config('escalation_queue', None))
            payload.set_metadata('descalation-queue', self.get_config('review_queue'))
            queue_name = self.get_config('review_queue')
        else:
            payload.set_followup_queue(None)
            payload.set_metadata('escalation-queue', None)
            payload.set_metadata('descalation-queue', None)
            payload.set_metadata('recheck-queue', None)
            payload.set_metadata('current-qc-test', None)
        payload.set_unique_key(group_key)
        payload.enqueue(self._db, queue_name)

    def submit_batch(self, working_uuids: list[str], batch_outcome: BatchOutcome, group_key: t.Optional[str] = None):
        batch = structures.NODBBatch()
        batch.batch_uuid = str(uuid.uuid4())
        batch.status = structures.BatchStatus.QUEUED
        self._db.insert_object(batch)
        structures.NODBWorkingRecord.bulk_set_batch_uuid(self._db, working_uuids, batch.batch_uuid)
        self.submit_existing_batch(batch.batch_uuid, batch_outcome, group_key)

    def _build_batcher(self, payload: WorkflowPayload):
        kwargs = {
            'batch_submitter': self,
            'max_batch_size': self.get_config('max_batch_size'),
            'max_buffer_size': self.get_config('max_buffer_size'),
            'target_buffer_size': self.get_config('target_buffer_size')
        }
        if (not self._test_runner.station_invariant) or isinstance(payload, SourceFilePayload):
            return StationResultBatcher(**kwargs)
        else:
            return SimpleResultBatcher(**kwargs)

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        batcher = self._build_batcher(payload)
        if isinstance(payload, BatchPayload):
            batch = payload.load_batch(self._db)
            self._process_records(batch.stream_working_records(
                self._db,
                lock_type=LockType.FOR_NO_KEY_UPDATE,
                order_by=self._test_runner.working_sort_by
            ), batcher)
            batcher.flush_all()
            if batcher.remove_original_batch:
                self._db.delete_object(batch)
            self._current_item.mark_complete(self._db)
            self._db.commit()
            return QueueItemResult.HANDLED
        elif isinstance(payload, SourceFilePayload):
            source = payload.load_source_file(self._db)
            self._process_records(source.stream_working_records(
                self._db,
                lock_type=LockType.FOR_NO_KEY_UPDATE,
                order_by=self._test_runner.working_sort_by
            ), batcher)
            batcher.flush_all()
            self._current_item.mark_complete(self._db)
            self._db.commit()
            return QueueItemResult.HANDLED
        else:
            raise CNODCError(f'Invalid payload type for QC processing (must be batch or source file, found {payload.__class__.__name__})')

    def _process_records(self, records: t.Iterable[structures.NODBWorkingRecord], separator):
        for wr, dr, outcome, is_modified in self._test_runner.process_batch(records):
            if is_modified:
                self._update_working_record(wr, dr)
            separator.add_result(wr, dr, outcome)

    def _update_working_record(self,
                               working_record: structures.NODBWorkingRecord,
                               data_record: ocproc2.ParentRecord):
        if data_record.metadata.has_value('CNODCStation'):
            working_record.station_uuid = data_record.metadata.best_value('CNODCStation')
        if data_record.coordinates.has_value('Time'):
            try:
                working_record.obs_time = datetime.datetime.fromisoformat(
                    data_record.coordinates.best_value('Time'))
            except (TypeError, ValueError):
                working_record.obs_time = None
        if data_record.coordinates.has_value('Latitude') and data_record.coordinates.has_value('Longitude'):
            try:
                wkt = f'POINT ({str(float(data_record.coordinates.best_value("Longitude")))} {str(float(data_record.coordinates.best_value("Latitude")))})'
                working_record.location = wkt
            except (ValueError, TypeError):
                working_record.location = None
        working_record.set_metadata('qc_tests', list(set(x.test_name for x in data_record.qc_tests)))
        working_record.record = data_record


class BaseResultBatcher:

    def __init__(self,
                 batch_submitter: NODBQCWorker,
                 max_batch_size: int = None,
                 max_buffer_size: int = None,
                 target_buffer_size: int = None,
                 remove_original_batch: bool = False):
        self.remove_original_batch = remove_original_batch
        self._submitter = batch_submitter
        self._max_batch_size = max_batch_size if max_batch_size is not None and max_batch_size > 0 else None
        self._max_total_size = max_buffer_size if max_buffer_size is not None and max_buffer_size > 0 else None
        self._target_total_size = target_buffer_size if target_buffer_size is not None and target_buffer_size > 0 else self._max_total_size

    def _determine_batch_outcome(self, outcome: ocproc2.QCResult) -> BatchOutcome:
        if outcome == ocproc2.QCResult.MANUAL_REVIEW:
            return BatchOutcome.REVIEW_QUEUE
        return BatchOutcome.NEXT_QUEUE

    def _generate_unique_group(self, record: ocproc2.ParentRecord) -> t.Optional[str]:
        if record.metadata.has_value('CNODCStation'):
            return record.metadata.best_value('CNODCStation')
        if record.metadata.has_value('CNODCStationCandidates'):
            return '\x1F'.join(record.metadata.best_value('CNODCStationCandidates'))
        if record.metadata.has_value('CNODCStationString'):
            return record.metadata.best_value('CNODCStationString')
        return None

    def add_result(self, working: structures.NODBWorkingRecord, record: ocproc2.ParentRecord, outcome: ocproc2.QCResult):
        raise NotImplementedError

    def flush_all(self):
        raise NotImplementedError


class SimpleResultBatcher(BaseResultBatcher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs, remove_original_batch=False)
        self._batch_ids = {}

    def add_result(self, working: structures.NODBWorkingRecord, record: ocproc2.ParentRecord, outcome: ocproc2.QCResult):
        if working.qc_batch_id is None:
            raise ValueError('missing batch id')
        batch_outcome = self._determine_batch_outcome(outcome)
        if working.qc_batch_id not in self._batch_ids:
            self._batch_ids[working.qc_batch_id] = [batch_outcome, self._generate_unique_group(record)]
        elif batch_outcome == BatchOutcome.REVIEW_QUEUE:
            self._batch_ids[working.qc_batch_id][0] = batch_outcome

    def flush_all(self):
        for existing_id in self._batch_ids:
            self._submitter.submit_existing_batch(
                batch_id=existing_id,
                group_key=self._batch_ids[existing_id][1],
                batch_outcome=self._batch_ids[existing_id][0]
            )
        self._batch_ids = {}


class StationResultBatcher(BaseResultBatcher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs, remove_original_batch=True)
        self._result_batches = {}
        self._current_total = 0

    def add_result(self, working: structures.NODBWorkingRecord, record: ocproc2.ParentRecord, outcome: ocproc2.QCResult):
        group_key = self._generate_unique_group(record)
        target_queue = self._determine_batch_outcome(outcome)
        batch_key = self._generate_batch_key(group_key)
        if batch_key not in self._result_batches:
            self._result_batches[batch_key] = [[], group_key, target_queue, 0]
        elif target_queue == BatchOutcome.REVIEW_QUEUE:
            self._result_batches[batch_key][2] = target_queue
        self._result_batches[batch_key][0].append(working.working_uuid)
        self._result_batches[batch_key][3] += 1
        self._current_total += 1
        if self._max_batch_size is not None and self._result_batches[batch_key][3] >= self._max_batch_size:
            self.flush(batch_key)
        self._check_auto_flush()

    def _generate_batch_key(self, group_key: t.Optional[str]):
        x = {
            'group_key': group_key or '',
        }
        return hashlib.md5('\x1F'.join(f'{y}={x[y]}' for y in x).encode('utf-8', 'replace')).hexdigest()

    def _check_auto_flush(self):
        if self._max_total_size is not None and self._current_total >= self._max_total_size:
            for bn in self._flush_queue():
                self.flush(bn)
                if self._current_total < self._target_total_size:
                    break

    def _flush_queue(self) -> t.Iterable[str]:
        options = []
        for bn in list(self._result_batches.keys()):
            if self._result_batches[bn][1] is None:
                yield bn
            else:
                options.append((bn, self._result_batches[bn][3]))
        options.sort(key=lambda x: x[1], reverse=True)
        for bn, _ in options:
            yield bn

    def flush_all(self):
        for bn in list(self._result_batches.keys()):
            self.flush(bn)

    def flush(self, batch_name: str):
        self._submitter.submit_batch(
            working_uuids=self._result_batches[batch_name][0],
            group_key=self._result_batches[batch_name][1],
            batch_outcome=self._result_batches[batch_name][2]
        )
        self._current_total -= self._result_batches[batch_name][3]
        del self._result_batches[batch_name]
