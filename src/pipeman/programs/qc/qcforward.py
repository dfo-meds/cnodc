import typing as t

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import RecordAction, QCTestRunInfo, QCResult
from pipeman.processing.payload_worker import WorkflowWorker
from pipeman.processing.payloads import Payload, stream_payload_working_records
from pipeman.processing.queue_worker import QueueItemResult


class QCForwardWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="qc_forwarder",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            "queue_name": "qc_forward"
        })

    def process_payload(self, payload: Payload) -> t.Optional[QueueItemResult]:
        for working_record in stream_payload_working_records(self.db, payload):
            actions = working_record.metadata.get("proposed_actions", None)
            if actions is None:
                continue
            loaded_actions = [
                RecordAction.from_map(t.cast(dict, action)) for action in t.cast(list, actions)
            ]
            record = working_record.record
            qc_result = QCTestRunInfo(
                test_name="manual_qc",
                test_version="1.0",
                test_date=AwareDateTime.now(),
                result=QCResult.MANUAL_PASS,
                test_tags=["GTSPP 5.1"],
                applied_actions=loaded_actions
            )
            record.qc_tests.append(qc_result)
            for action in loaded_actions:
                action.apply(record)
            working_record.delete_metadata("proposed_actions")
            working_record.record = record
            self.db.update_object(working_record)
            self.db.commit()
        self.progress_payload(payload, next_queue=payload.followup_queue, prevent_default_progression=True)

