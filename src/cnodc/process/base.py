import multiprocessing as mp
import typing as t
import zrlog
from zrlog.logger import ImprovedLogger


class BaseProcess(mp.Process):

    def __init__(self, log_name: str, halt_flag: mp.Event, config: dict = None):
        super().__init__(daemon=True)
        self._log_name = log_name
        self._log: t.Optional[ImprovedLogger] = None
        self.config = config or {}
        self.halt_flag: mp.Event = halt_flag
        self.is_working: mp.Event = mp.Event()
        self.shutdown: mp.Event = mp.Event()
        self.cnodc_id = None

    def get_config(self, key, default=None):
        if key in self.config and not self.config[key] is None:
            return self.config[key]
        return default

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

    def check_continue(self):
        return not (self.shutdown.is_set() or self.halt_flag.is_set())

    def on_start(self):
        pass

    def on_complete(self):
        pass

    def _run(self):
        pass
