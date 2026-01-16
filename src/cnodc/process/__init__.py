"""
    This module provides the tools to run one or more worker processes in a consistent configurable
    method.

    Worker processes typically will inherit from one of these sub-classes:

    - ScheduledTask (for repeat jobs on a schedule)
    - SourcePayloadWorker (for queue-based workers that operate on source payloads)
    - FilePayloadWorker (same, but file payloads)
    - BatchPayloadWorker (same, but batch payloads)
    - PayloadWorker (same, multiple payload types)
    - QueueWorker (non-payload based queue workers)

    Two options for running workers are provided:
    - ProcessController runs workers as sub-processes using the multiprocessing module
    - SingleProcessController runs a single worker without threading or multiprocessing

    A threaded controller might be beneficial in the future, but typically multiprocessing
    should give better results.
"""

from .base import BaseWorker
from .scheduled_task import ScheduledTask
from .queue_worker import QueueWorker, QueueItemResult
from .payload_worker import WorkflowWorker, SourceWorkflowWorker, FileWorkflowWorker, BatchWorkflowWorker
from .single import SingleProcessController
from .multiprocess import ProcessController
