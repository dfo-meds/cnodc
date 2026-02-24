from cnodc.processing.workers.payload_worker import WorkflowWorker
from cnodc.processing.workers.queue_worker import QueueItemResult
from cnodc.processing.workflow.payloads import WorkflowPayload
import typing as t


class WorkflowProgressWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="progressor",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'workflow_continue'
        })

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        workflow = payload.load_workflow(self._db, self._halt_flag)
        if workflow.has_more_steps(payload.current_step):
            next_payload = self.copy_payload(payload)
            next_payload.current_step_done = True
            workflow.queue_step(
                next_payload,
                db=self._db
            )
            self.prevent_default_progression()
