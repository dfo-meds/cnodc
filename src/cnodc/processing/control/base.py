"""Provides base tools for process workers"""
import collections.abc
import functools
import os
import signal
import tempfile
import time
import typing
import typing as t
import uuid

import yaml
import zrlog
import pathlib
from zrlog.logger import ImprovedLogger
from autoinject import injector


from cnodc.storage import BaseStorageHandle, StorageController
from cnodc.util import HaltFlag, dynamic_object, CNODCError, gzip_with_halt, ungzip_with_halt, DynamicObjectLoadError
import json

from cnodc.util.protocols import EventProtocol


class _ProcessProtocol(typing.Protocol):

    def __init__(self, worker_cls: str, process_uuid: str, halt_flag: EventProtocol, end_flag: EventProtocol, signals: str, config_json: str): pass
    def start(self): pass
    def run(self): pass
    def shutdown(self): pass
    def is_alive(self) -> bool: pass


class BaseProcess:

    def __init__(self, process_name: str, process_idx: int, worker_cls: str, process_uuid: str, halt_flag: EventProtocol, end_flag: EventProtocol, signals: str, config_json: str, **kwargs):
        super().__init__(**kwargs)
        self._process_name = process_name
        self._process_idx = process_idx
        self._log = zrlog.get_logger(f'cnodc.process.{process_name}[{process_idx}]')
        self._worker_cls = worker_cls
        self._process_uuid = process_uuid
        self._halt_flag = halt_flag
        self._end_flag = end_flag
        self._signals = signals
        self._config_json = config_json

    def _noop_signals(self):
        for sig in self._signals.split("\n"):
            if hasattr(signal, sig):
                self._log.trace(f'Registering subprocess signal', sig)
                signal.signal(getattr(signal, sig), self._handle_signal)

    def _handle_signal(self, *args, **kwargs): pass

    def _build_and_run(self):
        self._log.trace('Building and running worker class')
        worker = dynamic_object(self._worker_cls)(
            _process_uuid=self._process_uuid,
            _config=json.loads(self._config_json),
            _halt_flag=HaltFlag(self._halt_flag),
            _end_flag=HaltFlag(self._end_flag),
        )
        worker.run()
        return worker

    def shutdown(self):
        """Request that the process running shutdown as soon as it is finished the current item."""
        self._log.trace('Shutdown requested')
        self._end_flag.set()

    def run(self):
        """Create and run the worker."""
        self._noop_signals()
        self._build_and_run()


class _ProcessSet:
    """Represents a set of processes all running the same worker."""

    def __init__(self,
                 process_name: str,
                 process_cls: callable,
                 worker_cls: str,
                 target_count: int,
                 config: dict,
                 signals: set[str],
                 halt_flag: EventProtocol,
                 no_start: bool = False):
        self._process_cls = process_cls
        self._process_name = process_name
        self._halt_flag = halt_flag
        self._quota = target_count
        self._signals = signals
        self._active_processes: dict[str, _ProcessProtocol] = {}
        self._config = config
        self._is_active = True
        self._worker_cls = worker_cls
        self._no_start = no_start
        self._log = zrlog.get_logger(f'cnodc.process_set.{process_name}')
        self._idx = -1

    def _build_process(self) -> tuple[_ProcessProtocol, str]:
        proc_uuid = str(uuid.uuid4())
        self._idx += 1
        return self._process_cls(
            process_name=self._process_name,
            process_idx = self._idx,
            process_uuid=proc_uuid,
            worker_cls=self._worker_cls,
            halt_flag=self._halt_flag,
            signals="\n".join(self._signals),
            config_json=json.dumps(self._config)
        ), proc_uuid

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
        reset_all = config != self._config
        self._config = config
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
            while current < self._quota:
                self._log.info('Starting new process')
                proc, proc_uuid = self._build_process()
                self._active_processes[proc_uuid] = proc
                proc.start()
                current += 1


def original_signal_handler(*args, **kwargs):
    raise KeyboardInterrupt

class BaseController:
    """Base class for controllers"""

    def __init__(self,
                 process_runner: callable,
                 log_name: str,
                 halt_flag,
                 config_file: t.Optional[pathlib.Path] = None,
                 config_file_dir: t.Optional[pathlib.Path] = None,
                 flag_file: t.Optional[pathlib.Path] = None,
                 _no_start: bool = False):
        self._log = zrlog.get_logger(log_name)
        self._break_count = 0
        self._halt_flag = halt_flag
        self._config_file = config_file
        self._config_dir = config_file_dir
        self._flag_file = flag_file
        self._signals = set()
        self._loaded = False
        self._process_runner_cls = process_runner
        self._process_info: dict[str, _ProcessSet] = {}
        self._no_start = _no_start

    def _register_process(self, process_name: str, process_cls_name: str, count: int = 1, config: dict = None):
        if process_name not in self._process_info:
            self._log.debug("Registering process %s", process_name)
            self._process_info[process_name] = _ProcessSet(
                process_name,
                self._process_runner_cls,
                process_cls_name,
                count,
                config or {},
                self._signals,
                self._halt_flag,
                no_start=self._no_start
            )
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

    def _handle_halt(self, sig_num, frame):
        """Handle a halt signal"""
        self._log.info("Signal %s caught", sig_num)
        self._halt_flag.set()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical("Critical halt")
            raise KeyboardInterrupt

    def _populate_config_from_file(self, config: dict, file: t.Union[str, pathlib.Path]):
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
                work_dirs = [self._config_dir]
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
                self._populate_config_from_file(config, self._config_file)
        if not config:
            raise CNODCError("No processes specified", "PROCESSCTRL", 1001)
        return config

    def _validate_config(self, process_name: str, config: dict) -> tuple[bool, t.Optional[dict], t.Optional[int]]:
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
            if not result:
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

    def start(self):
        """Method to register all signals and start the process."""
        try:
            self._register_halt_signal("SIGINT")
            self._register_halt_signal("SIGTERM")
            self._register_halt_signal("SIGBREAK")
            self._register_halt_signal("SIGQUIT")
            self.reload_check()
            self.run()
        finally:
            self.cleanup()

    def run(self):
        """Run loop, will constantly check for configuration changes and ensure process sets are running until
            the halt flag is set.
        """
        self._log.info('Starting processes')
        try:
            while not self._halt_flag.is_set():
                if self._check_reload():
                    self._reload_config()
                self.reap_and_sow()
                time.sleep(1)
        finally:
            self._log.info('Closing processes')
            self.deactivate_all()
            self.wait_for_all()
            self._log.info('Complete, exiting')

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



class SaveData:
    """Represents data that should be saved to the hard drive while running a process.
        Note that save data may be wiped if running on a virtual machine, so process workers
        cannot rely on the data being there and should have sensible default behaviour.

        Save data must be compatible with JSON objects.
    """

    def __init__(self, save_file: t.Union[str, pathlib.Path, None]):
        self._save_file = pathlib.Path(save_file) if isinstance(save_file, str) else save_file
        self._file_loaded: bool = False
        self._save_failed: bool = False
        self._values = {}

    def __getitem__(self, item):
        self.load_file()
        return self._values[item]

    def __setitem__(self, key, value):
        self.load_file()
        self._values[key] = value

    def __contains__(self, key):
        self.load_file()
        return key in self._values

    def get(self, item, default=None):
        self.load_file()
        if item in self._values:
            return self._values[item]
        return default

    def load_file(self):
        """Ensure that the file is loaded, if it exists."""
        if not self._file_loaded:
            self._file_loaded = True
            if self._save_file and self._save_file.exists():
                try:
                    with open(self._save_file, "r") as h:
                        self._values = json.loads(h.read()) or {}
                except OSError as ex:
                    zrlog.get_logger("cnodc.save_file").exception("Exception while opening save file")

    def save_file(self):
        """Save the data to disk."""
        if self._save_file is not None and self._file_loaded and not self._save_failed:
            try:
                with open(self._save_file, "w") as h:
                    h.write(json.dumps(self._values))
            except OSError as ex:
                zrlog.get_logger("cnodc.save_file").exception("Exception while saving save data")
                self._save_failed = True


class BaseWorker:
    """Base class for all workers, provides common utility functions.

        A worker generally is started by one of the controllers and runs until the controller
        tells it to stop. The controller handles any parallelism such that the worker class
        is always instantiated inside the thread/process that will be running it (i.e. you can
        assume that __init__() and run() are called from the same thread/process).

        The controller will ensure that the run() method is called to start it. The worker should
        then loop until one of the halt or end flags are set. The halt flag being set indicates
        an urgent need to end the worker (e.g. the system is shutting down, the user has requested
        the controller stop ASAP). The end flag being set indicates a less urgent need to end the
        worker (e.g. the configuration needs to be reloaded). Worker processes should terminate as
        soon as possible while maintaining a consistent state when the halt flag is set and at the
        next reasonable point when the end flag is set (e.g. after the current item/execution is complete).

        The run() method, by default, calls on_start() and then _run(). Once _run() completes, it then
        calls on_complete() regardless of if _run() raised an error or note. Errors are logged and then
        the run() function returns. The controller classes interpret the run() function returning as
        the process ending and the appropriate actions are taken (i.e. the thread/subprocess will be
        closed).

        Sub-classes should typically implement the _run() method with a loop that uses the continue_loop()
        function to check if it should continue, e.g.:

        ```
          def __run(self):
            while self.continue_loop():
              # do stuff
              pass
        ```

        This will ensure the _run function returns appropriately. It should also catch and handle any errors
        that would not prevent the worker from continuing onto the next iteration.
    """

    def __init__(self,
                 process_name: str,
                 process_version: str,
                 _process_uuid: str,
                 _halt_flag: HaltFlag,
                 _end_flag: HaltFlag,
                 _config: dict = None):
        self._halt_flag = _halt_flag
        self._end_flag = _end_flag
        self._process_uuid = _process_uuid
        self._process_name = process_name
        self._process_version = process_version
        self._config = _config or {}
        self._cycle_config = {}
        self._defaults = {
            'save_file': None,
        }
        self._log: t.Optional[ImprovedLogger] = zrlog.get_logger(f"cnodc.worker.{process_name.lower()}")
        zrlog.set_extras({
            'process_uuid': self._process_uuid,
            'process_name': self._process_name,
            'process_version': self._process_version,
        })
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        self._save_data: t.Optional[SaveData] = None
        self._hook_cache = {}
        self._events: list[str] = ['on_start', 'before_cycle', 'after_cycle', 'on_exit']

    def add_events(self, events: list[str]):
        self._events.extend(events)

    @property
    def process_name(self):
        return self._process_name

    @property
    def process_uuid(self):
        return self._process_uuid

    @property
    def process_version(self):
        return self._process_version

    def possible_events(self) -> list[str]:
        return self._events

    def run_hook(self, hook_name, **kwargs):
        self._log.trace(f'Hook %s fired', hook_name)
        for hook_callable in self._build_hooks(hook_name):
            hook_callable(worker=self, **kwargs)

    @functools.cache
    def _build_hooks(self, hook_name: str) -> list[collections.abc.Callable]:
        hook_callables = []
        hooks = self.get_merged_config(f'hook_{hook_name}')
        if hooks:
            for hook_call in hooks:
                if isinstance(hook_call, str):
                    try:
                        hook_callables.append(dynamic_object(hook_call))
                    except DynamicObjectLoadError:
                        self._log.exception(f"Error loading hook [{hook_call}] for [{hook_name}]")
                else:
                    hook_callables.append(hook_call)
        return hook_callables

    def breakpoint(self):
        """ Check if we need to break. """
        self._log.trace('Breakpoint')
        self._halt_flag.breakpoint()

    def responsive_sleep(self, time_seconds: float, max_delay: float = 1.0):
        """Sleep for a given amount of time, with regular wake-ups to check the halt/end flags."""
        if time_seconds <= 0:
            return
        elif time_seconds < (2 * max_delay):
            self._log.trace('Sleeping for [%s] seconds', time_seconds)
            time.sleep(time_seconds)
        else:
            st = time.monotonic()
            et = st
            while (et - st) < time_seconds:
                self._log.trace('Sleeping for [%s] seconds', time_seconds)
                time.sleep(min(max_delay, max(time_seconds - (et - st), 0.01)))
                et = time.monotonic()
                if not self.continue_loop():
                    break

    def continue_loop(self):
        """Check if the halt or end flags are set (True if neither are). """
        return self._halt_flag.check_continue(False) and self._end_flag.check_continue(False)

    def get_merged_config(self, key) -> list:
        values = []
        for d in (self._cycle_config, self._config, self._defaults):
            if key in d and d[key]:
                if isinstance(d[key], (list, tuple, set)):
                    values.extend(d[key])
                else:
                    values.append(d[key])
        self._log.trace(f"Merged  config setting from cycle [%s=%s]", key, values)
        return values

    def get_config(self, key, default=None, merge: bool = False):
        """Get a configuration setting by name, or the given default value if it isn't present."""
        if key in self._cycle_config and self._cycle_config[key] is not None:
            self._log.trace(f"Config setting from cycle [%s=%s]", key, self._cycle_config[key])
            return self._cycle_config[key]
        if key in self._config and self._config[key] is not None:
            self._log.trace(f"Config setting from config [%s=%s]", key, self._config[key])
            return self._config[key]
        elif key in self._defaults and self._defaults[key] is not None:
            self._log.trace(f"Config setting from defaults [%s=%s]", key, self._defaults[key])
            return self._defaults[key]
        else:
            self._log.trace(f"Config setting from method default [%s=%s]", key, default)
            return default

    def set_cycle_config(self, values: dict):
        self._cycle_config = values

    def set_defaults(self, values: dict):
        """Set the default values for the configuration settings."""
        self._defaults.update(values)

    @property
    def process_id(self):
        return f"{self.process_name}:{self.process_version}:{self.process_uuid}"

    def run(self) -> None:
        """Run the worker process with appropriate error handling."""
        exc = None
        try:
            self._log.debug('Starting process %s', self.process_id)
            self.on_start()
            self._log.debug(f'Process %s is running', self.process_id)
            self._run()
        except Exception as ex:
            exc = ex
            self._log.exception(f"{ex.__class__.__name__}: {str(ex)}")
        finally:
            self._log.debug(f'Cleaning up %s', self.process_id)
            self.on_exit(exc)
            self._log.debug(f'Process %s complete', self.process_id)

    def run_once(self):
        exc = None
        try:
            self.on_start()
            self._run_once()
        except Exception as ex:
            exc = ex
            self._log.exception(f"{ex.__class__.__name__}: {str(ex)}")
        finally:
            self.on_exit(exc)

    def _run(self):
        """Override this method with a loop to process items."""
        while self.continue_loop():
            sleep_time = self._run_once()
            self.responsive_sleep(sleep_time)

    def _run_once(self) -> float:
        raise NotImplementedError

    def on_start(self):
        """Override this method to provide functionality prior to _run() being called."""
        self.run_hook('on_start')

    def on_exit(self, exception: Exception = None):
        """Override this method for clean-up after _run() is called."""
        if self._save_data is not None:
            self._save_data.save_file()
        self.run_hook('on_exit', exception=exception)

    def before_cycle(self):
        """Override this method to be called before each item is processed."""
        self.run_hook('before_cycle')

    def after_cycle(self):
        """Override this method to be called after each item is processed."""
        self.run_hook('after_cycle')
        if self._temp_dir is not None:
            self._log.trace('Cleaning up temp directory')
            self._temp_dir.cleanup()
            self._temp_dir = None

    @property
    def save_data(self):
        if self._save_data is None:
            self._save_data = SaveData(self.get_config('save_file'))
            self._save_data.load_file()
        return self._save_data

    def temp_dir(self) -> pathlib.Path:
        """Get a temporary directory that will be cleaned up after the current item."""
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        return pathlib.Path(self._temp_dir.name)

    @injector.inject
    def get_handle(self, file_path, raise_ex: bool = False, storage: StorageController = None) -> t.Optional[BaseStorageHandle]:
        return storage.get_handle(
            file_path=file_path,
            raise_ex=raise_ex,
            halt_flag=self._halt_flag
        )

    def gzip_local_file(self, source_file, destination_file):
        gzip_with_halt(source_file, destination_file, halt_flag=self._halt_flag)   # pragma: no coverage

    def ungzip_local_file(self, source_file, destination_file):
        ungzip_with_halt(source_file, destination_file, halt_flag=self._halt_flag)   # pragma: no coverage

