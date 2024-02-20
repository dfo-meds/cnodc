"""Provides a single-threaded process controller that only runs one process."""
import threading
import uuid

from cnodc.process.base import BaseController, _ThreadingHaltFlag
from cnodc.util import dynamic_object


class SingleProcessController(BaseController):
    """Single-threaded process controller"""

    def __init__(self, process_name: str, **kwargs):
        super().__init__(
            log_name='cnodc.single_process',
            halt_flag=threading.Event(),
            **kwargs
        )
        self._process_name = process_name
        self._process_info = {}

    def _register_process(self,
                          process_name: str,
                          process_cls: str,
                          quota: int,
                          config: dict):
        self._process_info[process_name] = (process_cls, quota, config)

    def _deregister_process(self,
                            process_name: str):
        if process_name in self._process_info:
            del self._process_info

    def _registered_process_names(self) -> list[str]:
        return list(self._process_info.keys())

    def run(self):
        """Load and run the named process."""
        if self._process_name in self._process_info:
            process = dynamic_object(self._process_info[self._process_name][0])(
                _process_uuid=str(uuid.uuid4()),
                _halt_flag=_ThreadingHaltFlag(self._halt_flag),
                _end_flag=_ThreadingHaltFlag(self._halt_flag),
                _config=self._process_info[self._process_name][2]
            )
            process.run()
        else:
            self._log.error(f'Process [{self._process_name}] not defined in configuration file')
