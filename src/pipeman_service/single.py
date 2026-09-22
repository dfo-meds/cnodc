""" Provides a single-threaded process controller that only runs one process.

    .. caution:: This runner is designed to be used in TESTING only. Do not use in production.
"""
import threading

from medsutil.exceptions import CodedError
from pipeman_service.controller import BaseController, BaseRunner


class _SingleProcessRunner(BaseRunner):
    """ A single-process runner that only runs one process. """

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
    """ Single-threaded process controller

        .. caution:: TESTING ONLY
    """

    def __init__(self, process_name: str, **kwargs):
        super().__init__(
            process_creator=_SingleProcessRunner,
            _no_start=True,
            log_name='pipeman_service.sp_controller',
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
            raise CodedError(f'Process [{self._process_name}] not defined in configuration file', 1000, code_space='PS-SINGLE')
