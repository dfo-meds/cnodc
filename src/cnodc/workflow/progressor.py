from cnodc.process.payload_worker import WorkflowWorker
from cnodc.process.queue_worker import QueueItemResult
from cnodc.workflow.workflow import WorkflowPayload
from cnodc.util.exceptions import CNODCError
import typing as t
from cnodc.nodb import structures


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
        if workflow is None:
            raise CNODCError(f"Invalid workflow", "PROGRESSOR", 1000, is_recoverable=False)
        elif workflow.has_more_steps(payload.current_step):
            next_payload = self.copy_payload(payload)
            next_payload.current_step_done = True
            workflow.queue_step(
                next_payload,
                db=self._db
            )
            self.prevent_default_progression()
