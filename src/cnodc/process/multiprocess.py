"""Controller for multiple processes based on the multiprocessing library."""
import json
import signal
import uuid
import multiprocessing as mp
import time

from cnodc.process.base import BaseController
from cnodc.util import dynamic_object, HaltFlag


class _SubprocessHaltFlag(HaltFlag):
    """Halt flag based on multiprocessing.Event."""

    def __init__(self, global_halt: mp.Event):
        self._global = global_halt

    def _should_continue(self) -> bool:
        return not self._global.is_set()


class _ProcessRunner(mp.Process):
    """Implementation of a process that runs a worker class."""

    def __init__(self, worker_cls: str, process_uuid: str, halt_flag: mp.Event, config_json: str, signals: str, *args, **kwargs):
        self._worker_cls = worker_cls
        self._process_uuid = process_uuid
        self._halt_flag = halt_flag
        self._signals = signals
        self._end_flag = mp.Event()
        self._config_json = config_json
        self.is_working = mp.Event()
        super().__init__(*args, **kwargs)

    def shutdown(self):
        """Request that the process running shutdown as soon as it is finished the current item."""
        self._end_flag.set()

    def run(self):
        """Create and run the worker."""
        for sig in self._signals.split("\n"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), self._handle_signal)
        worker = dynamic_object(self._worker_cls)(
            _process_uuid=self._process_uuid,
            _config=json.loads(self._config_json),
            _halt_flag=_SubprocessHaltFlag(self._halt_flag),
            _end_flag=_SubprocessHaltFlag(self._end_flag)
        )
        worker.run()

    def _handle_signal(self, *args, **kwargs):
        pass


class _ProcessSet:
    """Represents a set of processes all running the same worker."""

    def __init__(self, worker_cls: str, target_count: int, config: dict, signals: set[str], global_halt: mp.Event):
        self._halt_flag = global_halt
        self._quota = target_count
        self._signals = signals
        self._active_processes: dict[str, _ProcessRunner] = {}
        self._config = config
        self._is_active = True
        self._worker_cls = worker_cls

    def set_quota(self, new_count: int):
        """Set the target number of workers to have running."""
        self._quota = new_count if new_count > 1 else 1

    def deactivate(self):
        """Stop all workers"""
        self._is_active = False
        self.shutdown_all()

    def activate(self):
        """Start all workers"""
        self._is_active = True

    def shutdown_all(self):
        """Request all workers stop as soon as possible."""
        for x in self._active_processes:
            self._active_processes[x].shutdown()

    def is_active(self) -> bool:
        """Check if any processes are still active."""
        self._reap()
        return len(self._active_processes) > 0

    def set_config(self, config: dict):
        """Set the configuration for the workers and restarts them if the configuration has changed."""
        if not isinstance(config, dict):
            raise ValueError("must be dict")
        reset_all = config != self._config
        self._config = config
        if reset_all:
            self.shutdown_all()

    def reap_and_sow(self):
        """Clear workers that have errored and start enough workers to meet the quota."""
        self._reap()
        self._sow()

    def _reap(self):
        """Clear workers that have errored"""
        for x in list(self._active_processes.keys()):
            if not self._active_processes[x].is_alive():
                del self._active_processes[x]

    def _sow(self):
        """Start new workers as needed"""
        if self._is_active:
            current = len(self._active_processes)
            while current < self._quota:
                proc_id = str(uuid.uuid4())
                self._active_processes[proc_id] = _ProcessRunner(
                    worker_cls=self._worker_cls,
                    process_uuid=proc_id,
                    halt_flag=self._halt_flag,
                    signals="\n".join(self._signals),
                    config_json=json.dumps(self._config)
                )
                self._active_processes[proc_id].start()
                current += 1


class ProcessController(BaseController):
    """Controller for running multiple workers based on the multiprocessing library.

        Workers are defined in a configuration file that includes which class to
        load, how many workers to run, and their configuration.

        The controller also defines a flag file that, if set, will cause the controller
        to reload all of the configuration from the original file and update all of the
        running processes. This is useful to make configuration changes on the fly without
        restarting the entire system.
    """

    def __init__(self, **kwargs):
        super().__init__(
            log_name="cnodc.multi_process",
            halt_flag=mp.Event(),
            **kwargs
        )
        self._process_info: dict[str, _ProcessSet] = {}

    def _register_process(self, process_name: str, process_cls_name: str, count: int = 1, config: dict = None):
        if process_name not in self._process_info:
            self._log.debug(f"Registering process {process_name}")
            self._process_info[process_name] = _ProcessSet(process_cls_name, count, config or {}, self._signals, self._halt_flag)
        else:
            self._log.debug(f"Updating process {process_name}")
            self._process_info[process_name].set_quota(count)
            self._process_info[process_name].set_config(config or {})
            self._process_info[process_name].activate()

    def _deregister_process(self, process_name: str):
        if process_name in self._process_info:
            self._log.debug(f"Deactivating process {process_name}")
            self._process_info[process_name].deactivate()

    def _registered_process_names(self) -> list[str]:
        return list(self._process_info.keys())

    def run(self):
        """Run loop, will constantly check for configuration changes and ensure process sets are running until
            the halt flag is set.
        """
        try:
            while not self._halt_flag.is_set():
                if self._check_reload():
                    self._reload_config()
                self.reap_and_sow()
                time.sleep(1)
        finally:
            self.terminate_all()
            self.wait_for_all()

    def terminate_all(self):
        """Request every process stop immediately."""
        self._log.debug(f"Requesting all processes to halt")
        for process_name in self._process_info:
            self._process_info[process_name].deactivate()

    def wait_for_all(self):
        """Wait until all processes have stopped."""
        complete = False
        while not complete:
            complete = True
            for process_name in self._process_info:
                if self._process_info[process_name].is_active():
                    complete = False
            if not complete:
                time.sleep(0.05)

    def reap_and_sow(self):
        """Run reap_and_sow() for every process set."""
        for process_name in self._process_info:
            self._process_info[process_name].reap_and_sow()
