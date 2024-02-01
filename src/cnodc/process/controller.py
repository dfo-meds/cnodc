import math
import statistics
import uuid
import multiprocessing as mp
import time
import pathlib
import signal
import yaml
import zrlog

from cnodc.util import dynamic_object, HaltFlag
from .base import BaseWorker


class _SubprocessHaltFlag(HaltFlag):

    def __init__(self, global_halt: mp.Event):
        self._global = global_halt

    def _should_continue(self) -> bool:
        return not self._global.is_set()


class ProcessRunner(mp.Process):

    def __init__(self, worker_cls: str, process_uuid: str, halt_flag: mp.Event, config: dict, *args, **kwargs):
        self._worker_cls = worker_cls
        self._process_uuid = process_uuid
        self._halt_flag = halt_flag
        self._end_flag = mp.Event()
        self._config = config
        self.is_working = mp.Event()
        super().__init__(*args, **kwargs)

    def shutdown(self):
        self._end_flag.set()

    def run(self):
        worker = dynamic_object(self._worker_cls)(
            _process_uuid=self._process_uuid,
            _config=self._config,
            _halt_flag=_SubprocessHaltFlag(self._halt_flag),
            _end_flag=_SubprocessHaltFlag(self._end_flag)
        )
        worker.run()


class ProcessSet:

    def __init__(self, worker_cls: str, target_count: int, config: dict, global_halt: mp.Event):
        self._halt_flag = global_halt
        self._target_count = target_count
        self._active_processes: dict[str, ProcessRunner] = {}
        self._config = config
        self._is_active = True
        self._worker_cls = worker_cls

    def set_target_count(self, new_count: int):
        self._target_count = new_count if new_count > 1 else 1

    def deactivate(self):
        self._is_active = False

    def activate(self):
        self._is_active = True

    def shutdown_all(self):
        for x in self._active_processes:
            self._active_processes[x].shutdown()

    def wait_for_all(self) -> bool:
        self._reap()
        return len(self._active_processes) == 0

    def set_config(self, config: dict):
        if not isinstance(config, dict):
            raise ValueError("must be dict")
        reset_all = config != self._config
        self._config = config
        if reset_all:
            self.shutdown_all()

    def reap_and_sow(self):
        self._reap()
        self._sow()

    def _reap(self):
        for x in list(self._active_processes.keys()):
            if not self._active_processes[x].is_alive():
                del self._active_processes[x]

    def _sow(self):
        if self._is_active:
            current = len(self._active_processes)
            while current < self._target_count:
                proc_id = str(uuid.uuid4())
                self._active_processes[proc_id] = ProcessRunner(
                    worker_cls=self._worker_cls,
                    process_uuid=proc_id,
                    halt_flag=self._halt_flag,
                    config=self._config
                )
                self._active_processes[proc_id].start()
                current += 1


class ProcessController:

    def __init__(self,
                 config_file: pathlib.Path,
                 flag_file: pathlib.Path):
        self._config_file = config_file
        self._flag_file = flag_file
        self._loaded = False
        self._process_info: dict[str, ProcessSet] = {}
        self._halt_flag = mp.Event()
        self._break_count = 0
        self._log = zrlog.get_logger("cnodc.processctrl")

    def register_process(self, process_name: str, process_cls_name: str, count: int = 1, config: dict = None):
        if process_name not in self._process_info:
            self._log.debug(f"Registering process {process_name}")
            self._process_info[process_name] = ProcessSet(process_cls_name, count, config or {}, self._halt_flag)
        else:
            self._log.debug(f"Updating process {process_name}")
            self._process_info[process_name].set_target_count(count)
            self._process_info[process_name].set_config(config or {})
            self._process_info[process_name].activate()

    def deregister_process(self, process_name: str):
        if process_name in self._process_info:
            self._log.debug(f"Deactivating process {process_name}")
            self._process_info[process_name].deactivate()

    def _register_halt_signal(self, sig_name):
        if hasattr(signal, sig_name):
            signal.signal(getattr(signal, sig_name), self._handle_halt)

    def _handle_halt(self, sig_num, frame):
        self._log.info(f"Signal {sig_num} caught")
        self._halt_flag.set()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical(f"Critical halt")
            raise KeyboardInterrupt()

    def loop(self):
        self._log.debug("Registering halt signals")
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        try:
            while not self._halt_flag.is_set():
                if self.check_reload():
                    self.reload_config()
                self.reap_and_sow()
                time.sleep(1)
        finally:
            self.terminate_all()
            self.wait_for_all()

    def check_reload(self):
        if not self._loaded:
            return True
        if self._flag_file.exists():
            self._log.debug(f"Flag file detected, reloading config")
            self._flag_file.unlink(True)
            return True
        return False

    def reload_config(self):
        self._log.notice(f"Reloading configuration from disk")
        with open(self._config_file, "r") as h:
            config = yaml.safe_load(h) or {}
            if not isinstance(config, dict):
                self._log.error(f"Config file [{self._config_file}] does not contain a yaml dictionary")
                return
            deregister_list = list(self._process_info.keys())
            for process_name in config:
                if 'class_name' not in config[process_name]:
                    self._log.error(f"Process [{process_name}] is missing a class name")
                    continue
                self.register_process(
                    process_name,
                    config[process_name]['class_name'],
                    int(config[process_name]['count']) if 'count' in config[process_name] and config[process_name]['count'] > 0 else 1,
                    config[process_name]['config'] if 'config' in config[process_name] else {}
                )
                if process_name in deregister_list:
                    deregister_list.remove(process_name)
            for process_name in deregister_list:
                self.deregister_process(process_name)
            self._loaded = True

    def terminate_all(self):
        self._log.debug(f"Requesting all processes to halt")
        for process_name in self._process_info:
            self._process_info[process_name].shutdown_all()

    def wait_for_all(self):
        complete = False
        while not complete:
            complete = True
            for process_name in self._process_info:
                if not self._process_info[process_name].wait_for_all():
                    complete = False
            if not complete:
                time.sleep(0.05)

    def reap_and_sow(self):
        for process_name in self._process_info:
            self._process_info[process_name].reap_and_sow()
