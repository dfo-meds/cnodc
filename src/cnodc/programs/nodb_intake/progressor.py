from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
from cnodc.workflow.processor import PayloadProcessor
import typing as t


class NODBProgressWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_progressor", **kwargs)
        self.set_defaults({
            'queue_name': 'nodb_continue'
        })
        self._process_controller: t.Optional[ProgressController] = None

    def on_start(self):
        self._process_controller = ProgressController()

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._process_controller.process_queue_item(item)


class ProgressController(PayloadProcessor):

    def __init__(self):
        super().__init__()

    def _process(self):
        workflow = self.load_workflow_controller()
        if workflow is None:
            # TODO: error
            pass
        elif workflow.has_more_steps(self._current_payload.current_step):
            self._current_payload.current_step += 1
            workflow.queue_step(
                self._current_payload,
                db=self._db,
                unique_key=(
                    self._current_payload.headers['forward-unique-item-key']
                    if 'forward-unique-item-key' in self._current_payload.headers else
                    None
                )
            )
            self._current_item.mark_complete(self._db)
            self._db.commit()
        else:
            # TODO: item complete
            pass
