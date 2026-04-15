from pipeman.processing.payload_worker import WorkflowWorker
from pipeman.processing.payloads import WorkflowPayload


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

    def process_payload(self, payload: WorkflowPayload):
        workflow = payload.load_workflow(self.db, self._halt_flag)
        if workflow.has_more_steps(payload.current_step):
            next_payload = self.copy_payload(payload)
            next_payload.current_step_done = True
            workflow.queue_step(
                next_payload,
                db=self.db
            )
        else:
            self._log.debug('No next step after %s:%s', workflow.name, payload.current_step)
        self.prevent_default_progression()
