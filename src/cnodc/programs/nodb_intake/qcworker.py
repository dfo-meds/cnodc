import datetime
import enum
import hashlib
import uuid

from cnodc.nodb import LockType, NODBControllerInstance
from cnodc.process.queue_worker import QueueWorker
import typing as t
from cnodc.qc.base import SingleRecordTestSuite
import cnodc.nodb.structures as structures
from cnodc.util import dynamic_object
from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import BatchPayload, WorkflowPayload, SourceFilePayload
import cnodc.ocproc2.structures as ocproc2


class QCWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name='cnodc.qc_worker', **kwargs)
        self._processor: t.Optional[QCProcessor] = None

    def on_start(self):
        self._processor = QCProcessor(
            test_suite=self.get_config('qc_test_suite_class'),
            test_suite_kwargs=self.get_config('qc_test_suite_kwargs', {}),
            batcher_kwargs={
                'max_batch_size': self.get_config('max_batch_size', None),
                'max_buffer_size': self.get_config('max_buffer_size', None),
                'target_buffer_size': self.get_config('target_buffer_size', None)
            },
            submitter_kwargs={
                'next_queue': self.get_config('success_queue'),
                'failure_queue': self.get_config('failure_queue'),
                'review_queue': self.get_config('review_queue')
            },
            use_source_file=self.get_config('input_is_source_file', False),
            processor_uuid=self.process_uuid,
        )

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        self._processor.process_queue_item(item)
        return None


class QCProcessor(PayloadProcessor):

    def __init__(self,
                 test_suite: str,
                 test_suite_kwargs: dict,
                 batcher_kwargs: dict,
                 submitter_kwargs: dict,
                 processor_id: str,
                 use_source_file: bool = False,
                 **kwargs):
        self._test_suite: SingleRecordTestSuite = dynamic_object(test_suite)(**test_suite_kwargs, test_runner_id=processor_id)
        super().__init__(
            require_type=BatchPayload if not use_source_file else SourceFilePayload,
            process_name=self._test_suite.test_name,
            process_version=self._test_suite.test_version,
            processor_uuid=processor_id,
            **kwargs
        )
        self._use_source_file = use_source_file
        self._batcher_kwargs = batcher_kwargs or {}
        self._submitter_kwargs = submitter_kwargs or {}

    def _process(self):
        self._test_suite.set_db_instance(self._db)
        submitter = NODBBatchSubmitter(self._db, self._current_payload, **self._submitter_kwargs)
        separator = ResultBatcher(submitter, **self._batcher_kwargs)
        if not self._use_source_file:
            batch = self.load_batch_from_payload()
            self._process_records(batch.stream_working_records(self._db, lock_type=LockType.FOR_NO_KEY_UPDATE), separator)
            separator.flush_all()
            self._db.delete_object(batch)
            self._current_item.mark_complete(self._db)
            self._db.commit()
        else:
            source = self.load_source_from_payload()
            self._process_records(
                source.stream_working_records(self._db, lock_type=LockType.FOR_NO_KEY_UPDATE),
                separator
            )
            separator.flush_all()

    def _process_records(self, records: t.Iterable[structures.NODBWorkingRecord], separator):
        for wr, dr, outcome, is_modified in self._test_suite.process_batch(records):
            if is_modified:
                self._update_working_record(wr, dr)
            separator.add_result(wr.working_uuid, dr, outcome)

    def _update_working_record(self,
                               working_record: structures.NODBWorkingRecord,
                               data_record: ocproc2.DataRecord):
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


class BatchOutcome(enum.Enum):

    NEXT_QUEUE = 'N'
    REVIEW_QUEUE = 'R'


class NODBBatchSubmitter:

    def __init__(self,
                 db: NODBControllerInstance,
                 last_payload: WorkflowPayload,
                 next_queue: str = None,
                 review_queue: str = None,
                 failure_queue: str = None,
                 next_step_queue: str = 'nodb_continue'):
        self._db = db
        self._next_step = next_step_queue
        self._previous_payload = last_payload
        self._next_queue = next_queue
        self._failure_queue = failure_queue
        self._review_queue = review_queue

    def submit_batch(self, working_uuids: list[str], batch_outcome: BatchOutcome, group_key: t.Optional[str] = None):
        batch = structures.NODBBatch()
        batch.batch_uuid = str(uuid.uuid4())
        batch.status = structures.BatchStatus.QUEUED
        self._db.insert_object(batch)
        structures.NODBWorkingRecord.bulk_set_batch_uuid(self._db, working_uuids, batch.batch_uuid)
        queue_name = self._get_queue_name(batch_outcome)
        payload = BatchPayload(batch.batch_uuid)
        self._previous_payload.update_for_propagation(payload)
        if queue_name is not None:
            self._db.create_queue_item(
                queue_name=queue_name,
                subqueue_name=self._get_subqueue_name(),
                data=payload.to_map(),
                unique_item_key=group_key
            )
            self._db.commit()
        else:
            if group_key is not None:
                payload.headers['forward-unique-item-key'] = group_key
            elif 'unique-item-key' in payload.headers:
                del payload.headers['unique-item-key']
            self._db.create_queue_item(
                queue_name=self._next_step,
                data=payload.to_map(),
            )
            self._db.commit()

    def _get_subqueue_name(self) -> t.Optional[str]:
        if 'manual-subqueue' in self._previous_payload.headers:
            return self._previous_payload.headers['manual-subqueue'] or None
        return None

    def _get_queue_name(self, outcome: BatchOutcome) -> t.Optional[str]:
        if outcome == BatchOutcome.NEXT_QUEUE:
            return self._next_queue
        elif outcome == BatchOutcome.REVIEW_QUEUE:
            return self._review_queue
        else:
            raise ValueError('Invalid batch outcome type')


class ResultBatcher:

    def __init__(self,
                 batch_submitter: NODBBatchSubmitter,
                 max_batch_size: int = None,
                 max_buffer_size: int = None,
                 target_buffer_size: int = None):
        self._submitter = batch_submitter
        self._result_batches = {}
        self._current_total = 0
        self._max_batch_size = max_batch_size if max_batch_size is not None and max_batch_size > 0 else 100
        self._max_total_size = max_buffer_size if max_buffer_size is not None and max_buffer_size > 0 else 1000000
        self._target_total_size = target_buffer_size if target_buffer_size is not None and target_buffer_size > 0 else 250000

    def add_result(self, working_uuid: str, record: ocproc2.DataRecord, outcome: ocproc2.QCResult):
        group_key = self._generate_unique_group(record)
        target_queue = self._target_queue(outcome)
        batch_key = self._generate_batch_key(group_key)
        if batch_key not in self._result_batches:
            self._result_batches[batch_key] = [[], group_key, target_queue, 0]
        elif target_queue == BatchOutcome.REVIEW_QUEUE:
            self._result_batches[batch_key][2] = target_queue
        self._result_batches[batch_key][0].append(working_uuid)
        self._result_batches[batch_key][3] += 1
        self._current_total += 1
        if self._result_batches[batch_key][3] >= self._max_batch_size:
            self.flush(batch_key)
        self._check_auto_flush()

    def _generate_batch_key(self, group_key: t.Optional[str]):
        return hashlib.md5(group_key.encode('utf-8', 'replace')).hexdigest()

    def _target_queue(self, outcome: ocproc2.QCResult) -> BatchOutcome:
        if outcome == ocproc2.QCResult.MANUAL_REVIEW:
            return BatchOutcome.REVIEW_QUEUE
        return BatchOutcome.NEXT_QUEUE

    def _generate_unique_group(self, record: ocproc2.DataRecord) -> t.Optional[str]:
        if record.metadata.has_value('CNODCStation'):
            return record.metadata.best_value('CNODCStation')
        if record.metadata.has_value('CNODCStationCandidates'):
            return '\x1F'.join(record.metadata.best_value('CNODCStationCandidates'))
        if record.metadata.has_value('CNODCStationString'):
            return record.metadata.best_value('CNODCStationString')
        return None

    def _check_auto_flush(self):
        if self._current_total >= self._max_total_size:
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
