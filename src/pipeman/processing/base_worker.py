import json
import pathlib
import tempfile
import time
import typing as t

import zrlog
from autoinject import injector
from zrlog.logger import ImprovedLogger

from medsutil.cached import CachedObjectMixin, cached_method
from medsutil.dynamic import dynamic_object, DynamicObjectLoadError
from medsutil.halts import HaltFlag, gzip_with_halt, ungzip_with_halt
import medsutil.metrics as mum
from medsutil.metrics import Gauge, Histogram

from medsutil.storage import StorageController, FilePath


class BaseWorker(CachedObjectMixin):
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
        super().__init__()
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
        self._log: ImprovedLogger = zrlog.get_logger(f"cnodc.worker.{process_name.lower()}")
        zrlog.set_extras({
            'process_uuid': self._process_uuid,
            'process_name': self._process_name,
            'process_version': self._process_version,
        })
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        self._save_data: t.Optional[SaveData] = None
        self._hook_cache = {}
        self._events: list[str] = ['on_start', 'before_cycle', 'after_cycle', 'on_exit']
        self._metrics = {}
        self._last_run_gauge = None
        self._run_time_histogram = None

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

    def create_counter(self, name: str, description: str = "", labels: list[str] | tuple | None = None) -> mum.Counter:
        key = f"counter_{name}"
        if key not in self._metrics:
            self._metrics[key] = mum.Counter(name=name, namespace="pipeman", subsystem=self._process_name, documentation=description, labelnames=labels or tuple())
        return self._metrics[key]

    def count(self, name, *args, **kwargs):
        if args or kwargs:
            self.create_counter(name).labels(*args, **kwargs).inc()
        else:
            self.create_counter(name).inc()

    def run_hook(self, hook_name, **kwargs):
        self._log.debug('Hook [%s] fired', hook_name)
        for hook_callable in self._build_hooks(hook_name):
            self._log.trace('Hook [%s] sent to [%s]', hook_name, hook_callable)
            hook_callable(worker=self, **kwargs)

    @cached_method
    def _build_hooks(self, hook_name: str) -> list[t.Callable]:
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
        self._log.trace(f"Merged config setting from cycle [%s=%s]", key, values)
        return values

    def get_config[T, Y](self, key: str, default: Y = None, coerce: t.Optional[t.Callable[[t.Any], T] | type[T]] = None, merge: bool = False) -> T | Y:
        """Get a configuration setting by name, or the given default value if it isn't present."""
        value = ...
        if key in self._cycle_config and self._cycle_config[key] is not None:
            self._log.trace(f"Config setting from cycle [%s=%s]", key, self._cycle_config[key])
            value = self._cycle_config[key]
        elif key in self._config and self._config[key] is not None:
            self._log.trace(f"Config setting from config [%s=%s]", key, self._config[key])
            value = self._config[key]
        elif key in self._defaults and self._defaults[key] is not None:
            self._log.trace(f"Config setting from defaults [%s=%s]", key, self._defaults[key])
            value = self._defaults[key]
        if value is not ...:
            if value is not None and coerce is not None:
                return coerce(value)
            return value
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

    def run_once_after_start(self):
        self._run_once()

    def _run(self):
        while self.continue_loop():
            sleep_time = self._run_once()
            self.responsive_sleep(sleep_time)

    def _run_once(self) -> float:
        """Override this method  to process items."""
        raise NotImplementedError

    def on_start(self):
        """Override this method to provide functionality prior to _run() being called."""
        self._last_run_gauge = Gauge(name="last_run", documentation="When did the last execution finish?", unit="timestamp_seconds", namespace="pipeman", subsystem=self.process_id.replace(":", "_"), multiprocess_mode="livemostrecent")
        self._run_time_histogram = Histogram(name="run_time", documentation="How long did the execution take to finish", unit="seconds", namespace="pipeman", subsystem=self.process_id.replace(":", "_"))
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
        self._cycle_config = {}
        if self._temp_dir is not None:
            self._log.trace('Cleaning up temp directory')
            self._temp_dir.cleanup()
            self._temp_dir = None
        if self._last_run_gauge is not None:
            self._last_run_gauge.set_to_current_time()

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

    @t.overload
    def get_handle(self, file_path: str | pathlib.Path, raise_ex: bool = True) -> FilePath: ...

    @t.overload
    def get_handle(self, file_path: str | pathlib.Path) -> t.Optional[FilePath]: ...

    @injector.inject
    def get_handle(self, file_path, raise_ex: bool = False, storage: StorageController = None) -> t.Optional[FilePath]:
        return storage.get_filepath(
            file_path=file_path,
            raise_ex=raise_ex,
            halt_flag=self._halt_flag
        )

    def gzip_local_file(self, source_file, destination_file):
        gzip_with_halt(source_file, destination_file, halt_flag=self._halt_flag)   # pragma: no coverage

    def ungzip_local_file(self, source_file, destination_file):
        ungzip_with_halt(source_file, destination_file, halt_flag=self._halt_flag)   # pragma: no coverage


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
