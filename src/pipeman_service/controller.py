"""Provides base tools for process workers"""
import copy
import os
import queue
from dataclasses import dataclass

import signal
import time
import typing as t
import uuid

import yaml
import zrlog
import pathlib

import json
import medsutil.types as ct

if t.TYPE_CHECKING:
    from medsutil.halts import HaltFlag

@dataclass
class ProcessInfo:
    process_name: str
    quota: int
    server_name: str
    json_config: str
    worker_cls: str
    halt_flag: ct.SupportsEvent
    signals: str
    no_start: bool
    process_index: int | None = None
    process_uuid: str | None = None
    logging_queue: queue.Queue | None = None


class _ProcessProtocol(t.Protocol):
    @property
    def halt_flag(self) -> ct.SupportsEvent: ...
    @property
    def end_flag(self) -> ct.SupportsEvent: ...
    def start(self): ...
    def run(self): ...
    def shutdown(self): ...
    def is_alive(self) -> bool: ...

class _WorkerProtocol(t.Protocol):
    def run(self): ...

class _WorkerCreatorProtocol(t.Protocol):
    def __call__(self,
                 _process_uuid: str,
                 _config: dict[str, ct.SupportsNativeJson],
                 _halt_flag: HaltFlag,
                 _end_flag: HaltFlag): ...

class _ProcessCreatorProtocol(t.Protocol):
    def __call__(self,
                 process_name: str,
                 process_idx: int,
                 worker_cls: str,
                 process_uuid: str,
                 halt_flag: ct.SupportsEvent,
                 signals: str,
                 config_json: ct.JsonDictString,
                 logging_queue=None) -> _ProcessProtocol: pass


class BaseProcess:

    def __init__(self, *args, proc_info: ProcessInfo, end_flag: ct.SupportsEvent, **kwargs):
        self._end_flag = end_flag
        self._proc_info = proc_info
        self._log = None
        super().__init__(*args, **kwargs)

    @property
    def halt_flag(self):
        return self._proc_info.halt_flag

    @property
    def end_flag(self):
        return self._end_flag

    def _noop_signals(self):
        for sig in self._proc_info.signals.split("\n"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), self._handle_signal)

    def _handle_signal(self, *args, **kwargs): ...

    def _build_and_run(self):
        from medsutil.halts import HaltFlag
        from medsutil.dynamic import dynamic_object
        self._log.trace('Building and running worker class')
        worker = dynamic_object(self._proc_info.worker_cls)(
            _process_uuid=self._proc_info.process_uuid,
            _config=json.loads(self._proc_info.json_config),
            _halt_flag=HaltFlag(self.halt_flag),
            _end_flag=HaltFlag(self.end_flag),
        )
        worker.run()
        return worker

    def shutdown(self):
        """Request that the process running shutdown as soon as it is finished the current item."""
        if self._log:
            self._log.trace('Shutdown requested')
        self._end_flag.set()

    def run(self):
        """Create and run the worker."""
        self._noop_signals()
        self.setup()
        self._log = zrlog.get_logger(f'cnodc.process.{self._proc_info.process_name}[{self._proc_info.process_index}]')
        self._build_and_run()

    def setup(self):
        ...




class _ProcessSet:
    """Represents a set of processes all running the same worker."""

    def __init__(self, process_cls, process_info: ProcessInfo):
        self._process_cls = process_cls
        self._info = process_info
        self._active_processes: dict[str, _ProcessProtocol] = {}
        self._is_active = True
        self._log = zrlog.get_logger(f'cnodc.process_set.{process_info.process_name}')
        self._idx = -1

    def _build_process(self) -> tuple[_ProcessProtocol, str]:
        proc_id = f"{self._info.server_name}_{self._info.process_name}_{self._idx}"
        x = copy.copy(self._info)
        x.process_index = self._idx
        x.process_uuid = proc_id
        obj = self._process_cls(proc_info=x)
        self._idx += 1
        return obj, proc_id

    def set_quota(self, new_count: int):
        """Set the target number of workers to have running."""
        self._info.quota = new_count if new_count > 1 else 1

    def deactivate(self):
        """Stop all workers"""
        self._is_active = False
        self.shutdown_all()

    def activate(self):
        """Start all workers"""
        self._is_active = True

    def shutdown_all(self):
        """Request all workers stop as soon as possible."""
        self._log.trace('Shutting down all active processes')
        for x in self._active_processes:
            self._active_processes[x].shutdown()

    def is_active(self, _no_reap: bool = False) -> bool:
        """Check if any processes are still active."""
        if not _no_reap:
            self._reap()
        return len(self._active_processes) > 0

    def is_activated(self) -> bool:
        return self._is_active

    def set_config(self, config: dict):
        """Set the configuration for the workers and restarts them if the configuration has changed."""
        if not isinstance(config, dict):
            raise ValueError("must be dict")
        config_str = json.dumps(config)
        reset_all = config_str != self._info.json_config
        self._info.json_config = config_str
        if reset_all:
            self.shutdown_all()

    def run_one(self):
        proc, _ = self._build_process()
        proc.run()
        return proc

    def reap_and_sow(self):
        """Clear workers that have errored and start enough workers to meet the quota."""
        self._reap()
        self._sow()

    def _reap(self):
        """Clear workers that have errored"""
        for x in list(self._active_processes.keys()):
            if not self._active_processes[x].is_alive():
                self._log.info('Removing dead process')
                del self._active_processes[x]

    def _sow(self):
        """Start new workers as needed"""
        if self._is_active:
            current = len(self._active_processes)
            while current < self._info.quota:
                self._log.info('Starting new process')
                proc, proc_uuid = self._build_process()
                self._active_processes[proc_uuid] = proc
                proc.start()
                current += 1


def original_signal_handler(*args, **kwargs):
    raise KeyboardInterrupt

class BaseController:
    """Base class for controllers"""

    def __init__[T: _ProcessProtocol](self,
                 process_creator: _ProcessCreatorProtocol | type[T],
                 log_name: str,
                 halt_flag: ct.SupportsEvent,
                 config_file: t.Optional[pathlib.Path] = None,
                 config_file_dir: t.Optional[pathlib.Path] = None,
                 flag_file: t.Optional[pathlib.Path] = None,
                 server_name: t.Optional[str] = None,
                 _no_start: bool = False,
                 socket_port: int = 9173,
                 logging_queue=None):
        self._log = zrlog.get_logger(log_name)
        self._server_name: str | None = server_name
        self._break_count = 0
        self._halt_flag = halt_flag
        self._kill_requested: bool = False
        self._shutdown_requested: bool = False
        self._reload_requested: bool = False
        self._config_file = config_file
        self._config_dir = config_file_dir
        self._flag_file = flag_file
        self._signals = set()
        self._loaded = False
        self._process_runner_cls = process_creator
        self._process_info: dict[str, _ProcessSet] = {}
        self._no_start = _no_start
        self._logging_queue = logging_queue

        from medsutil.servicecmd import ServiceCommandManager
        self._manager = ServiceCommandManager(socket_port, self._handle_command)
        self._base_kwargs = {
            "halt_flag": self._halt_flag,
            "no_start": self._no_start,
            "logging_queue": self._logging_queue,
        }

    def _register_process(self,
                          process_name: str,
                          process_cls_name: str,
                          count: int = 1,
                          config: dict[str, ct.SupportsExtendedJson] = None):
        if self._server_name is None:
            self._server_name = str(uuid.uuid4())
        if process_name not in self._process_info:
            self._log.debug("Registering process %s", process_name)
            self._process_info[process_name] = _ProcessSet(self._process_runner_cls, ProcessInfo(
                process_name=process_name,
                worker_cls=process_cls_name,
                quota=count,
                json_config=json.dumps(config or {}),
                server_name=str(self._server_name),
                **self._base_kwargs
            ))
        else:
            self._log.debug("Updating process %s", process_name)
            self._process_info[process_name].set_quota(count)
            self._process_info[process_name].set_config(config or {})
            self._process_info[process_name].activate()

    def _deregister_process(self, process_name: str):
        if process_name in self._process_info:
            self._log.debug("Deactivating process %s", process_name)
            self._process_info[process_name].deactivate()

    def _registered_process_names(self) -> list[str]:
        return list(self._process_info.keys())

    def _check_reload(self):
        """Check if the configuration needs to be reloaded"""
        if not self._loaded:
            return True
        if self._flag_file is not None and self._flag_file.exists():
            self._log.debug("Flag file detected, reloading configuration")
            self._flag_file.unlink(True)
            self._reload_requested = False
            return True
        if self._reload_requested:
            self._reload_requested = False
            return True
        return False

    def _register_halt_signal(self, sig_name):
        """Register a halt signal"""
        if hasattr(signal, sig_name):
            self._log.debug("Registering signal %s", sig_name)
            signal.signal(getattr(signal, sig_name), self._handle_halt)
            self._signals.add(sig_name)

    def cleanup(self):
        for sig_name in self._signals:
            signal.signal(getattr(signal, sig_name), original_signal_handler)
        self._signals = set()
        self._manager.cleanup()

    def _handle_halt(self, sig_num, frame):
        """Handle a halt signal"""
        self._log.info("Signal %s caught", sig_num)
        self._halt_flag.set()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical("Critical halt")
            raise KeyboardInterrupt

    def _populate_config_from_file(self, config: dict[str, ct.SupportsNativeJson], file: t.Union[str, pathlib.Path]):
        self._log.debug('Loading configuration from %s', file)
        with open(file, "r", encoding="utf-8") as h:
            data = yaml.safe_load(h)
            if isinstance(data, dict):
                config.update(data)
            else:
                self._log.error("Process configuration file [%s] does not contain a YAML dictionary", file)

    def _load_config(self):
        config = {}
        if self._config_dir is not None:
            if not self._config_dir.exists():
                self._log.error(f"Config directory [%s] specified but does not exist", self._config_dir)
            elif not self._config_dir.is_dir():
                self._log.error(f"Config directory [%s] is not a directory", self._config_dir)
            else:
                self._log.debug("Reading configuration from YAML files in %s", self._config_dir)
                if self._server_name is None:
                    self._server_name = self._config_dir.name
                work_dirs = [str(self._config_dir)]
                while work_dirs:
                    for file in os.scandir(work_dirs.pop()):
                        if file.name.lower().endswith(".yaml"):
                            self._populate_config_from_file(config, file.path)
                        elif file.is_dir():
                            work_dirs.append(file.path)
        if self._config_file is not None:
            if not self._config_file.exists():
                self._log.error('Config file [%s] specified but does not exist', self._config_file)
            else:
                if self._server_name is None:
                    self._server_name = self._config_file.stem
                self._populate_config_from_file(config, self._config_file)
        if not config:
            from pipeman.exceptions import CNODCError
            raise CNODCError("No processes specified", "PROCESSCTRL", 1001)
        return config

    def _validate_config(self, process_name: str, config: dict) -> tuple[bool, dict[str, ct.SupportsExtendedJson] | None, int | None]:
        from medsutil.dynamic import dynamic_object
        from pipeman.exceptions import CNODCError
        if 'class_name' not in config[process_name]:
            self._log.error("Process [%s] is missing a class name", process_name)
            return False, None, None
        try:
            cls = dynamic_object(config[process_name]['class_name'])
            if not hasattr(cls, 'run'):
                raise CNODCError('Invalid worker class, no run method', 'PROCESSCTRL', 1000)
        except Exception as ex:
            self._log.exception('Process [%s] has an invalid class name', process_name)
            return False, None, None
        # Validate configuration
        proc_config = {}
        if 'config' in config[process_name] and config[process_name]['config']:
            if isinstance(config[process_name]['config'], dict):
                proc_config = config[process_name]['config']
            else:
                self._log.warning("Process [%s] does not define a dictionary for its configuration, ignoring configuration", process_name)
        # Validate count
        count = 1
        if 'count' in config[process_name] and config[process_name]['count']:
            try:
                count = max(int(config[process_name]['count']), 1)
            except (TypeError, ValueError):
                self._log.warning("Processing [%s] has a non-integer value for the quota [%s], defaulting to 1", process_name, config[process_name]['count'])
        return True, proc_config, count

    def _reload_config(self):
        """Reload configuration from disk."""
        self._log.trace('Reloading process configuration')
        config = self._load_config()
        # We will remove keys from this list as we see them and deregister all the ones we didn't see
        deregister_list = self._registered_process_names()
        for process_name in config:
            # Remove the process name immediately, so we don't stop them if there is an error processing the definition
            if process_name in deregister_list:
                deregister_list.remove(process_name)
            result, proc_config, count = self._validate_config(process_name, config)
            if result is None or count is None:
                continue
            # Register the process
            self._register_process(
                process_name,
                config[process_name]['class_name'],
                count,
                proc_config,
            )
        # Deregister processes not found in current list
        for process_name in deregister_list:
            self._deregister_process(process_name)
        self._loaded = True

    def reload_check(self):
        if self._check_reload():
            self._reload_config()

    def startup(self):
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        self._base_kwargs['signals'] = "\n".join(self._signals)
        self.reload_check()
        self._manager.setup()

    def start(self):
        """Method to register all signals and start the process."""
        try:
            self.startup()
            self.run()
        finally:
            self.cleanup()

    def run(self):
        """Run loop, will constantly check for configuration changes and ensure process sets are running until
            the halt flag is set.
        """
        self._log.info('Starting processes')
        try:
            while not (self._halt_flag.is_set() or self._shutdown_requested):
                if self._check_reload():
                    self._reload_config()
                self.reap_and_sow()
                self._manager.check()
                if self._kill_requested:
                    raise KeyboardInterrupt
        finally:
            if not self._kill_requested:
                self._log.info('Closing processes')
                self.deactivate_all()
                self.wait_for_all()
                self._log.info('Complete, exiting')
            else:
                self._log.critical("Kill requested, some processes may not have gracefully exited")

    def _handle_command(self, message: bytes, address):
        match message:
            case b'interrupt':
                self._halt_flag.set()
                return b'0'
            case b'shutdown':
                self._shutdown_requested = True
                return b'0'
            case b'reload':
                self._reload_requested = True
                return b'0'
            case b'kill':
                self._halt_flag.set()
                self._kill_requested = True
                return b'0'
            case _:
                return b'1'

    def deactivate_all(self):
        """Request every process stop immediately."""
        self._log.trace(f"Requesting all processes to halt")
        for process_name in self._process_info:
            self._process_info[process_name].deactivate()

    def wait_for_all(self, max_time=0) -> bool:
        """Wait until all processes have stopped."""
        complete = False
        start_time = time.monotonic()
        while not complete:
            complete = True
            for process_name in self._process_info:
                if self._process_info[process_name].is_active():
                    complete = False
            elapsed = time.monotonic() - start_time
            if elapsed > max_time:
                return complete
            if not complete:
                time.sleep(0.05)
        return complete

    def reap_and_sow(self):
        """Run reap_and_sow() for every process set."""
        self._log.trace('Requesting all processes to reap and sow')
        for process_name in self._process_info:
            self._process_info[process_name].reap_and_sow()



