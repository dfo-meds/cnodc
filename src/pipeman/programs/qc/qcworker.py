import enum
import hashlib
import uuid
import typing as t

from medsutil.exceptions import CodedError
from nodb.interface import LockType, NODBInstance
from nodb.observations import NODBWorkingRecord, NODBBatch, BatchStatus
from pipeman.processing.payload_worker import WorkflowWorker
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.qc.base import QCTestRunner, ResultBatcher
from medsutil.dynamic import dynamic_object
from pipeman.exceptions import CNODCError
from pipeman.processing.payloads import WorkflowPayload, SourceFilePayload, BatchPayload
import medsutil.ocproc2 as ocproc2


class NODBQCWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="qc_worker",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'qc_tests': {},
            'next_queue': "workflow_continue",
            'review_queue': 'nodb_manual_review',
            'error_queue': 'nodb_qc_errors',
            'recheck_queue': None,
            'escalation_queue': None,
        })

    def _build_test_runner(self):
        return QCTestRunner(
            self.db,
            self.process_full_id,
            self._queue_new_batch,
            [
                (
                    dynamic_object(x.get("class_name", "")),
                    x.get("args", None),
                    x.get("kwargs", None)
                ) if isinstance(x, dict) else (
                    dynamic_object(x),
                    None,
                    None
                )
                for x in self.get_config('qc_tests')
            ]
        )

    def _queue_new_batch(self,
                         db: NODBInstance,
                         new_batch_uuid: str,
                         outcome: int):
        bp = self.batch_payload_from_uuid(new_batch_uuid)
        bp.metadata['recheck_queue'] = None
        bp.metadata['next_queue'] = None
        bp.metadata['error_queue'] = None
        bp.metadata['escalation_queue'] = None
        if outcome == ResultBatcher.RESULT_NEXT:
            bp.enqueue(db, self.get_config("next_queue"))
        elif outcome == ResultBatcher.RESULT_REVIEW:
            bp.metadata['recheck_queue'] = self.get_config("recheck_queue", self.get_config("queue_name", None))
            bp.metadata['next_queue'] = self.get_config("next_queue", None)
            bp.metadata['error_queue'] = self.get_config("error_queue", None)
            bp.metadata['escalation_queue'] = self.get_config("escalation_queue", None)
            bp.enqueue(db, self.get_config("review_queue"))
        else:
            bp.enqueue(db, self.get_config("error_queue"))

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        if isinstance(payload, BatchPayload):
            batch = payload.load_batch(self.db)
            runner = self._build_test_runner()
            runner.qc_batch(batch)
            self._current_item.mark_complete(self.db)
            self.db.commit()
            self.prevent_default_progression()
            return QueueItemResult.HANDLED
        elif isinstance(payload, SourceFilePayload):
            source = payload.load_source_file(self.db)
            runner = self._build_test_runner()
            runner.qc_source_file(source)
            self._current_item.mark_complete(self.db)
            self.db.commit()
            self.prevent_default_progression()
            return QueueItemResult.HANDLED
        else:
            raise CodedError(
                f'Invalid payload type for QC processing (must be batch or source file, found {payload.__class__.__name__})',
                1000,
                code_space='QC-WORKER'
            )
