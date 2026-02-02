"""Provides base tools for process workers"""
import os
import signal
import tempfile
import threading
import time
import typing as t

import yaml
import zrlog
import pathlib
from zrlog.logger import ImprovedLogger
from cnodc.util import HaltFlag, dynamic_object, CNODCError
import json


class _ThreadingHaltFlag(HaltFlag):
    """A halt flag based on threading.Event"""

    def __init__(self, global_halt: threading.Event):
        self._global = global_halt

    def _should_continue(self) -> bool:
        return not self._global.is_set()


class _NoHaltFlag(HaltFlag):
    """A non-operational halt flag that never halts"""

    def __init__(self):
        pass

    def _should_continue(self) -> bool:
        return True


class BaseController:
    """Base class for controllers"""

    def __init__(self,
                 log_name: str,
                 halt_flag,
                 config_file: t.Optional[pathlib.Path] = None,
                 config_file_dir: t.Optional[pathlib.Path] = None,
                 flag_file: t.Optional[pathlib.Path] = None):
        self._log = zrlog.get_logger(log_name)
        self._break_count = 0
        self._halt_flag = halt_flag
        self._config_file = config_file
        self._config_dir = config_file_dir
        self._flag_file = flag_file
        self._loaded = False

    def _check_reload(self):
        """Check if the configuration needs to be reloaded"""
        if not self._loaded:
            return True
        if self._flag_file is not None and self._flag_file.exists():
            self._log.debug(f"Flag file detected, reloading configuration")
            self._flag_file.unlink(True)
            return True
        return False

    def _register_halt_signal(self, sig_name):
        """Register a halt signal"""
        if hasattr(signal, sig_name):
            signal.signal(getattr(signal, sig_name), self._handle_halt)

    def _handle_halt(self, sig_num, frame):
        """Handle a halt signal"""
        self._log.info(f"Signal {sig_num} caught")
        self._halt_flag.set()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical(f"Critical halt")
            raise KeyboardInterrupt

    def _registered_process_names(self) -> list[str]:
        """Return a list of currently registered process names"""
        raise NotImplementedError

    def _register_process(self,
                          process_name: str,
                          process_cls: str,
                          quota: int,
                          config: dict):
        """Register a new process or update an existing one"""
        raise NotImplementedError

    def _deregister_process(self,
                            process_name: str):
        """Unregister an existing process."""
        raise NotImplementedError

    def _populate_config_from_file(self, config: dict, file: t.Union[str, pathlib.Path]):
        with open(file, "r", encoding="utf-8") as h:
            data = yaml.safe_load(h)
            if isinstance(data, dict):
                config.update(data)
            else:
                self._log.error(f"Process configuration file [{file}] does not contain a YAML dictionary")

    def _load_config(self):
        self._log.notice(f"Reading configuration from disk")
        config = {}
        if self._config_dir is not None:
            if not self._config_dir.exists():
                self._log.error(f"Config directory specified but does not exist")
            elif not self._config_dir.is_dir():
                self._log.error(f"Config directory is not a directory")
            else:
                work_dirs = [self._config_dir]
                while work_dirs:
                    for file in os.scandir(work_dirs.pop()):
                        if file.name.lower().endswith(".yaml"):
                            self._populate_config_from_file(config, file.path)
                        elif file.is_dir():
                            work_dirs.append(file.path)
        if self._config_file is not None:
            if not self._config_file.exists():
                self._log.error('Config file specified but does not exist')
            else:
                self._populate_config_from_file(config, self._config_file)
        if not config:
            raise CNODCError("No processes specified", "PROCESSCTRL", 1001)
        return config

    def _reload_config(self):
        """Reload configuration from disk."""
        config = self._load_config()
        # We will remove keys from this list as we see them and deregister all the ones we didn't see
        deregister_list = self._registered_process_names()
        for process_name in config:
            # Remove the process name immediately, so we don't stop them if there is an error processing the definition
            if process_name in deregister_list:
                deregister_list.remove(process_name)
            # Validate class name
            if 'class_name' not in config[process_name]:
                self._log.error(f"Process [{process_name}] is missing a class name")
                continue
            try:
                cls = dynamic_object(config[process_name]['class_name'])
                if not hasattr(cls, 'run'):
                    raise CNODCError('Invalid worker class, no run method', 'PROCESSCTRL', 1000)
            except Exception as ex:
                self._log.exception(f'Process [{process_name}] has an invalid class name')
                continue
            # Validate configuration
            proc_config = {}
            if 'config' in config[process_name] and config[process_name]['config']:
                if isinstance(config[process_name]['config'], dict):
                    proc_config = config[process_name]['config']
                else:
                    self._log.error(f"Process [{process_name}] does not define a dictionary for its configuration")
                    continue
            # Validate count
            count = 1
            if 'count' in config[process_name] and config[process_name]['count']:
                try:
                    count = max(int(config[process_name]['count']), 1)
                except (TypeError, ValueError):
                    self._log.warning(
                        f"Processing [{process_name}] has a non-integer value for the quota [{config[process_name]['count']}], defaulting to 1")
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
        self._log.debug("Registering halt signals")
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        self._log.debug("Loading configuration")
        self.reload_check()
        self._log.debug("Running workers")
        self.run()

    def run(self):
        raise NotImplementedError


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
                except Exception:
                    zrlog.get_logger("cnodc.save_file").exception("Exception while opening save file")

    def save_file(self):
        """Save the data to disk."""
        if self._save_file is not None and self._file_loaded and not self._save_failed:
            try:
                with open(self._save_file, "w") as h:
                    h.write(json.dumps(self._values))
            except Exception:
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
                 _config: dict = None,
                 defaults: dict = None):
        self._halt_flag = _halt_flag
        self._end_flag = _end_flag
        self._process_uuid = _process_uuid
        self._process_name = process_name
        self._process_version = process_version
        self._config = _config or {}
        self._defaults = defaults or {}
        self._log: t.Optional[ImprovedLogger] = zrlog.get_logger(f"cnodc.worker.{process_name.lower()}")
        zrlog.set_extras({
            'process_uuid': self._process_uuid,
            'process_name': self._process_name,
            'process_version': self._process_version,
        })
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        self._save_data = SaveData(self.get_config('save_file'))
        self._save_data.load_file()

    def breakpoint(self):
        """ Check if we need to break. """
        self._halt_flag.breakpoint()

    def responsive_sleep(self, time_seconds: float, max_delay: float = 1.0):
        """Sleep for a given amount of time, with regular wake-ups to check the halt/end flags."""
        if time_seconds < (2 * max_delay):
            time.sleep(time_seconds)
        else:
            st = time.monotonic()
            et = st
            while (et - st) < time_seconds:
                time.sleep(min(max_delay, max(time_seconds - (et - st), 0.01)))
                et = time.monotonic()
                if not self.continue_loop():
                    break

    def continue_loop(self):
        """Check if the halt or end flags are set (True if neither are). """
        return self._halt_flag.check_continue(False) and self._end_flag.check_continue(False)

    def get_config(self, key, default=None):
        """Get a configuration setting by name, or the given default value if it isn't present."""
        if key in self._config and self._config[key] is not None:
            return self._config[key]
        elif key in self._defaults and self._defaults[key] is not None:
            return self._defaults[key]
        return default

    def set_defaults(self, values: dict):
        """Set the default values for the configuration settings."""
        self._defaults.update(values)

    def run(self) -> None:
        """Run the worker process with appropriate error handling."""
        ex = None
        try:
            self._log.debug(f'Starting process {self._process_uuid}')
            self.on_start()
            self._log.debug(f'Process {self._process_uuid} is running')
            self._run()
        except Exception as ex:
            ex = ex
            self._log.error(f"{ex.__class__.__name__}: {str(ex)}")
            self._log.exception(ex)
        finally:
            self._log.debug(f'Cleaning up {self._process_uuid}')
            self.on_complete(ex)
            self._save_data.save_file()
            self._log.debug(f'Process {self._process_uuid} complete')

    def on_start(self):
        """Override this method to provide functionality prior to _run() being called."""
        pass

    def on_complete(self, ex: Exception = None):
        """Override this method for clean-up after _run() is called."""
        pass

    def _run(self):
        """Override this method with a loop to process items."""
        pass

    def before_item(self):
        """Override this method to be called before each item is processed."""
        pass

    def temp_dir(self) -> pathlib.Path:
        """Get a temporary directory that will be cleaned up after the current item."""
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        return pathlib.Path(self._temp_dir.name)

    def after_item(self):
        """Override this method to be called after each item is processed."""
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
