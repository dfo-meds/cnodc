from cnodc.exc import CNODCHalt
from cnodc.util import dynamic_class, HaltFlag
import threading
import zrlog


class ThreadHaltEvent(HaltFlag):

    def __init__(self, event: threading.Event):
        self._event: threading.Event = event

    def check(self, raise_ex: bool = True) -> bool:
        if self._event.is_set():
            if raise_ex:
                raise CNODCHalt
            else:
                return True
        return False

    def sleep(self, time_seconds: float):
        self._event.wait(time_seconds)


class ThreadRunner:

    def __init__(self, instance_name: str):
        self._instance_name = instance_name
        self._threads: dict[str, threading.Thread] = {}
        self._event = threading.Event()
        self._log = zrlog.get_logger("cnodc.run.threads")
        self._next_thread_no = 1

    def launch_all(self, controller_classes: dict[str, int]):
        join_threads = True
        try:
            for controller_cls in controller_classes:
                copies = int(controller_classes[controller_cls])
                cls = dynamic_class(controller_cls)
                if copies < 1:
                    self._log.warning(f"Controller class [{controller_cls}] set to [{copies}] copies, skipping")
                    continue
                for i in range(0, copies):
                    self.launch(cls, f"{self._instance_name}--{self._next_thread_no}", i)
                    self._next_thread_no += 1
            while not self._event.is_set():
                self._event.wait(1)
                if not any(self._threads[x].is_alive() for x in self._threads):
                    self._event.set()
                    self._log.warning(f"No more threads alive, exiting")
                    break
        except KeyboardInterrupt:
            join_threads = False
            self._log.critical("Closing without finishing threads")
        finally:
            if join_threads:
                for thread_name in self._threads:
                    if self._threads[thread_name].is_alive():
                        self._log.info(f"Waiting for [{thread_name}] to finish...")
                        self._threads[thread_name].join()

    def launch(self, controller_cls: type, instance_name: str, instance_no: int):
        self._log.info(f"Starting [{controller_cls.__name__}], instance [{instance_name}]")
        self._threads[instance_name] = _ThreadController(controller_cls, instance_name, instance_no, self._event)
        self._threads[instance_name].start()

    def request_halt(self):
        self._event.set()


class _ThreadController(threading.Thread):

    def __init__(self, controller_cls: type, instance_name: str, instance_no: int, halt_event: threading.Event):
        super().__init__(daemon=True)
        self._cls = controller_cls
        self._name = instance_name
        self._event = halt_event
        self._no = instance_no

    def run(self):
        controller = self._cls(self._name, self._no, ThreadHaltEvent(self._event))
        controller.run()
