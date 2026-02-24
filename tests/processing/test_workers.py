import threading

from cnodc.processing import BatchWorkflowWorker
from cnodc.processing.workflow.payloads import SourceFilePayload, ObservationPayload
from cnodc.util import HaltFlag, CNODCError
from core import BaseTestCase


class TestBatchWorker(BaseTestCase):


    def test_payload_types(self):
        pass
