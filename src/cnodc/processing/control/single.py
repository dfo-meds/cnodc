"""Provides a single-threaded process controller that only runs one process."""
import threading

from cnodc.processing.control.base import BaseController, BaseProcess
from cnodc.util import CNODCError


class _SingleProcessRunner(BaseProcess):

    def __init__(self, **kwargs):
        super().__init__(**kwargs, end_flag=threading.Event())
        self._worker = None

    def _build_and_run(self):
        self._worker = super()._build_and_run()

    def start(self):
        pass  # pragma: no coverage

    def is_alive(self) -> bool:
        return False


class SingleProcessController(BaseController):
    """Single-threaded process controller"""

    def __init__(self, process_name: str, **kwargs):
        super().__init__(
            process_runner=_SingleProcessRunner,
            _no_start=True,
            log_name='cnodc.single_process',
            halt_flag=threading.Event(),
            **kwargs
        )
        self._process_name = process_name
        self._process = None

    def run(self):
        """Load and run the named process."""
        if self._process_name in self._process_info:
            self._process = self._process_info[self._process_name].run_one()
        else:
            raise CNODCError(f'Process [{self._process_name}] not defined in configuration file', 'SINGLECTRL', 1000)
