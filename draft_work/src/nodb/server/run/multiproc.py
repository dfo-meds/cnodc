# WIP: not ready
import multiprocessing as mp
from cnodc.util import HaltFlag
from cnodc.exc import CNODCHalt


class ProcessHaltEvent(HaltFlag):

    def __init__(self, event: mp.Event):
        self._event: mp.Event = event

    def check_continue(self, raise_ex: bool = True) -> bool:
        if self._event.is_set():
            if raise_ex:
                raise CNODCHalt
            else:
                return True
        return False

    def sleep(self, time_seconds: float):
        self._event.wait(time_seconds)


class ProcessRunner(mp.Process):

    def __init__(self, controller_cls: type, instance_name: str, halt_event: mp.Event):
        super().__init__(daemon=True)
        self._controller_cls = controller_cls
        self._name = instance_name
        self._event = halt_event

    def run(self):
        instance = self._controller_cls(self._name, ProcessHaltEvent(self._event))
        instance.run()


class ProcessController:

    def __init__(self):
        self._processes = {}
        self._halt_event = mp.Event()

    def launch(self, controller_cls, instance_name):
        self._processes[instance_name] = ProcessRunner(controller_cls, instance_name, self._halt_event)
        self._processes[instance_name].start()

    def halt(self):
        self._halt_event.set()
        for pname in self._processes:
            self._processes[pname].join()

