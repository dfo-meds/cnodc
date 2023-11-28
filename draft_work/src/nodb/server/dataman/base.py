import zrlog
from zrlog.logger import ImprovedLogger
from cnodc.util import HaltFlag
from cnodc.exc import CNODCError


class BaseController:

    def __init__(self, name: str, version: str, instance: str, instance_no: int, halt_flag: HaltFlag, run_delay: float = 1):
        self.name: str = name
        self.version: str = version
        self.instance: str = instance
        self.instance_no: int = instance_no
        self.halt_flag: HaltFlag = halt_flag
        self.run_delay: float = run_delay
        self.log: ImprovedLogger = zrlog.get_logger(f"cnodc.controllers.{name}")

    def run(self):
        if not self.check_config():
            raise CNODCError(f"Invalid configuration for [{self.name}]", "CTRL", 1000)
        self.init()
        while not self.halt_flag.check_continue(False):
            self._run()
            self.halt_flag.sleep(self.run_delay)
        self.cleanup()

    def check_config(self) -> bool:
        return True

    def init(self):
        pass

    def cleanup(self):
        pass

    def _run(self):
        raise NotImplementedError()
