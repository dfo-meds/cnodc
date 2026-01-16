from cnodc.process.payload_worker import WorkflowWorker
from cnodc.process.queue_worker import QueueItemResult
from cnodc.workflow.workflow import WorkflowPayload
import typing as t
from cnodc.nodb import structures


class WorkflowProgressWorker(WorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="progressor",
            process_version="1_0",
            defaults={
                'queue_name': 'nodb_continue'
            },
            **kwargs
        )

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
        workflow = payload.load_workflow(self._db, self._halt_flag)
        if workflow is None:
            # TODO: error
            pass
        elif workflow.has_more_steps(payload.current_step):
            next_payload = self.copy_payload(payload)
            next_payload.current_step += 1
            workflow.queue_step(
                next_payload,
                db=self._db
            )
            return QueueItemResult.SUCCESS
        else:
            return QueueItemResult.SUCCESS
