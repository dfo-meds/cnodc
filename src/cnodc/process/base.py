import multiprocessing as mp
import typing as t
import zrlog
from zrlog.logger import ImprovedLogger
from autoinject import injector
from cnodc.nodb import NODBController
from cnodc.storage import StorageController
from cnodc.util import HaltFlag


class _SubprocessHaltFlag(HaltFlag):

    def __init__(self, shutdown: mp.Event, halt: mp.Event):
        self._shutdown = shutdown
        self._halt = halt

    def _should_continue(self, raise_ex: bool = True) -> bool:
        return not (self._shutdown.is_set() or self._halt.is_set())


class BaseProcess(mp.Process):

    def __init__(self, log_name: str, halt_flag: mp.Event, config: dict = None):
        super().__init__(daemon=True)
        self._log_name = log_name
        self._log: t.Optional[ImprovedLogger] = None
        self._config = config or {}
        self._defaults = {}
        self._halt: mp.Event = halt_flag
        self.is_working: mp.Event = mp.Event()
        self._shutdown: mp.Event = mp.Event()
        self.halt_flag: HaltFlag = _SubprocessHaltFlag(self._shutdown, self._halt)
        self.cnodc_id = None

    def get_config(self, key, default = None):
        if key in self._config and self._config[key] is not None:
            return self._config[key]
        elif key in self._defaults and self._defaults[key] is not None:
            return self._defaults[key]
        return default

    def set_defaults(self, values: dict):
        self._defaults.update(values)

    def run(self) -> None:
        try:
            self._log = zrlog.get_logger(self._log_name)
            self.on_start()
            self._run()
        except Exception as ex:
            self._log.error(f"{ex.__class__.__name__}: {str(ex)}")
            self._log.exception(ex)
        finally:
            self.on_complete()

    def on_start(self):
        pass

    def on_complete(self):
        pass

    def _run(self):
        pass
