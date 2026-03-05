from cnodc.nodb import QueueStatus, NODBUploadWorkflow, NODBQueueItem
from cnodc.processing.workflow.payloads import BatchPayload, WorkflowPayload
from cnodc.processing.workflow.progressor import WorkflowProgressWorker
from core import BaseTestCase
from processing.helpers import WorkerTestCase


class TestProgressor(WorkerTestCase):

    def test_invalid_workflow(self):
        bp = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step1', current_step_done=False)
        item = self.worker_controller.payload_to_queue_item(bp)
        with self.assertLogs('cnodc.worker.progressor', 'ERROR'):
            self.worker_controller.test_queue_worker(
                WorkflowProgressWorker,
                {},
                item
            )
        self.assertEqual(item.status, QueueStatus.UNLOCKED)

    def test_valid_workflow(self):
        wf = NODBUploadWorkflow(is_new=True)
        wf.workflow_name = 'test'
        wf.configuration = {
            'working_target': { 'directory': self.temp_dir / 'hello', },
            "processing_steps": {
                'step1': { 'name': 'step1a', 'order': 1, },
                'step2': { 'name': 'step2a', 'order': 2, },
            },
        }
        self.db.insert_object(wf)
        bp = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step1', current_step_done=True)
        bp.metadata['5'] = '4'
        item = self.worker_controller.payload_to_queue_item(bp)
        self.assertEqual(len(self.db.table(NODBQueueItem.TABLE_NAME)), 0)
        self.worker_controller.test_queue_worker(WorkflowProgressWorker, {}, item)
        self.assertEqual(item.status, QueueStatus.COMPLETE)
        self.assertEqual(len(self.db.table(NODBQueueItem.TABLE_NAME)), 1)
        item2 = self.db.fetch_next_queue_item('step2a')
        self.assertIsNotNone(item2)
        bp2 = WorkflowPayload.from_queue_item(item2)
        self.assertEqual(bp2.metadata['5'], '4')
        self.assertEqual(bp2.current_step, 'step2')
        self.assertFalse(bp2.current_step_done)

    def test_valid_workflow_last(self):
        wf = NODBUploadWorkflow(is_new=True)
        wf.workflow_name = 'test'
        wf.configuration = {
            'working_target': { 'directory': self.temp_dir / 'hello', },
            "processing_steps": {
                'step1': { 'name': 'step1a', 'order': 1, },
                'step2': { 'name': 'step2a', 'order': 2, },
            },
        }
        self.db.insert_object(wf)
        bp = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step2', current_step_done=True)
        bp.metadata['5'] = '4'
        item = self.worker_controller.payload_to_queue_item(bp)
        self.assertEqual(len(self.db.table(NODBQueueItem.TABLE_NAME)), 0)
        self.worker_controller.test_queue_worker(WorkflowProgressWorker, {}, item)
        self.assertEqual(item.status, QueueStatus.COMPLETE)
        self.assertEqual(len(self.db.table(NODBQueueItem.TABLE_NAME)), 1)

