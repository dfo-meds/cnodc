"""Provides base tooling for PIPEMAN controllers.

Note that usually we expect these to be multiprocessing controllers, see
:py:class:`pipeman_service.multiprocess.MultiController`. However, we
use a single-threaded controller for testing purposes. This file contains all
the shared source code.
"""
import copy
import os
import queue
import signal
import time
import typing as t
import pathlib
import json
from dataclasses import dataclass

import yaml
import zrlog

import medsutil.types as ct
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import DynamicObjectLoadError
from medsutil.exceptions import CodedError
from pipeman.instrumented import InstrumentedObject

if t.TYPE_CHECKING:
    from medsutil.halts import HaltFlag


class PipemanServiceError(CodedError):
    """ An error raised by the pipeman service. """
    CODE_SPACE = "PIPEMAN-SERVICE"


@dataclass
class WorkerInfo:
    """ Information about a worker as loaded from the configuration file. """


    process_name: str
    """ The name of the process, as provided in configuration. """

    quota: int
    """ The number of instances of the process that should be running. """

    server_name: str
    """ The name of the current server. """

    config_str: str
    """ The worker's configuration, as a JSON-compatible exported string. """

    worker_cls: str
    """ The fully-qualified name of the worker class (e.g. package.module.ClassName). """

    halt_flag: ct.SupportsEvent
    """ The halt flag that indicates when the service is shutting down. """

    signals: str
    """ The list of signals that the subprocess should ignore. """

    no_start: bool
    """ Set to true for testing purposes sometimes - don't worry about it in production. """

    process_index: int | None = None
    """ The index of the current process, or None if it is not yet assigned. """

    process_uuid: str | None = None
    """ A unique identifier for the process, or None if it is not yet assigned. """

    logging_queue: queue.Queue | None = None
    """ A logging queue, used in multiprocessing only. """

    @staticmethod
    def config_to_str(config: t.Mapping[str, t.Any]) -> str:
        """ Converts configuration to a string.

            :param config: The configuration to convert to a string

            :raises PipemanServiceError: If the configuration is not a mapping (2000)
            :raises PipemanServiceError: If there was an error in JSON encoding (2001).
        """
        # configuration type check
        if not isinstance(config, t.Mapping):
            raise PipemanServiceError("Configuration must be a dictionary or other mapping", 2000)

        # convert to JSON string
        try:
            return json.dumps(config)
        except TypeError:
            raise PipemanServiceError("Configuration must be JSON-compatible", 2001)

    @staticmethod
    def str_to_config(config_str: str) -> dict[str, t.Any]:
        """ Converts configuration back from a string to a dictionary.

            :param config_str: The configuration string.

            :raises PipemanServiceError: If the configuration is invalid (2100).
        """
        try:
            return json.loads(config_str)
        except TypeError:
            raise PipemanServiceError("Invalid configuration, must be JSON-compatible", 2100)



class _WorkRunnerProtocol(t.Protocol):
    """ The protocol that process managers must implement. """

    @property
    def halt_flag(self) -> ct.SupportsEvent:
        """ The halt flag that indicates that the entire service is shutting down. """
        raise NotImplementedError

    @property
    def end_flag(self) -> ct.SupportsEvent:
        """ The end flag that indicates that this specific worker is shutting down. """
        raise NotImplementedError

    def start(self):
        """ Starts the process running. """
        raise NotImplementedError

    def run(self):
        """ Handles actually running the worker.

        Called by start() automatically usually.
        """
        raise NotImplementedError

    def shutdown(self):
        """ Tells the process manager to stop the worker gracefully."""
        raise NotImplementedError

    def is_alive(self) -> bool:
        """ Checks if the worker is still actively working. """
        raise NotImplementedError


class _WorkerProtocol(t.Protocol):
    """ The protocol that workers must follow. """

    def run(self):
        """ Runs whatever code is necessary in a loop.

            This function should only return if there is an error and
            the worker needs to be restarted.
        """
        raise NotImplementedError


class _WorkerCreatorProtocol(t.Protocol):
    """ The protocol that a worker creator must follow. """

    def __call__(self,
                 _process_uuid: str,
                 _config: dict[str, ct.SupportsNativeJson],
                 _halt_flag: HaltFlag,
                 _end_flag: HaltFlag):
        """ Creates a new worker.
        :param _process_uuid: A unique identifier for the worker.
        :param _config: The worker's configuration.
        :param _halt_flag: The halt flag.
        :param _end_flag: The end flag.
        :return: _WorkerProtocol
        """
        raise NotImplementedError


class _WorkRunnerCreatorProtocol(t.Protocol):
    """ The protocol that a process creator must follow. """

    def __call__(self,
                 proc_info: WorkerInfo) -> _WorkRunnerProtocol:
        """ Creates a new process manager.
        :param proc_info: The information describing the worker to build.
        :return: A process runner object.
        """
        raise NotImplementedError


class BaseRunner:
    """ Creates and manages a worker object from its configuration.

        These objects are built by the :py:class:`_RunnerSet` and are responsible
        for building and running the worker object directly.

        .. caution:: This class should be sub-classed appropriately to provide concurrency.
    """

    def __init__(self, *args, proc_info: WorkerInfo, end_flag: ct.SupportsEvent, **kwargs):
        """ Builds the process runner object.
            :param args: Arguments to pass to any other super-class.
            :param proc_info: An instance of :py:class:`pipeman_service.controller.ProcessInfo` with details on the worker to run.
            :param end_flag: A suitable instance of :py:class:`medsutil.types.SupportsEvent` that can be used to tell the worker to shutdown gracefully.
            :param kwargs: Keyword arguments to pass to any other super-class.
        """
        self._end_flag = end_flag
        self._proc_info = proc_info
        self._log = None
        super().__init__(*args, **kwargs)

    @property
    def halt_flag(self) -> ct.SupportsEvent:
        """ The halt flag, which tells the worker to exit as soon as possible when set. """
        return self._proc_info.halt_flag

    @property
    def end_flag(self) -> ct.SupportsEvent:
        """ The end flag, which tells the worker to exit after the current item is done being processed. """
        return self._end_flag

    def _noop_signals(self):
        """ Sets the signals to a no-op handler.
            This is only necessary for multiprocessing controllers.
        """
        for sig in self._proc_info.signals.split("\n"):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), self._handle_signal)

    def _handle_signal(self, *args, **kwargs):
        """ Handles signals by doing nothing. """
        ...

    def _build_and_run(self) -> _WorkerProtocol:
        """ Performs the necessary set-up tasks and runs the worker object.

            :raises PipemanServiceError: If the worker class name is invalid (1000).
        """
        from medsutil.halts import HaltFlag
        from medsutil.dynamic import dynamic_object

        self._log.trace('Building and running worker class')
        try:
            worker = dynamic_object(self._proc_info.worker_cls)(
                _process_uuid=self._proc_info.process_uuid,
                _server_name=self._proc_info.server_name,
                _config=WorkerInfo.str_to_config(self._proc_info.config_str),
                _halt_flag=HaltFlag(self.halt_flag),
                _end_flag=HaltFlag(self.end_flag),
            )
            worker.run()
            return worker
        except DynamicObjectLoadError as e:
            raise PipemanServiceError(f"Invalid worker class name [{self._proc_info.worker_cls}]", 1000) from e

    def shutdown(self):
        """ Requests that the process running shutdown as soon as it finishes the current item. """
        if self._log:
            self._log.trace('Shutdown requested')
        self._end_flag.set()

    def run(self):
        """Creates and run the worker. """

        # clear the signals
        self._noop_signals()

        # do any setup tasks for this particular process runner
        self.setup()

        # sets up a log.
        # note, this is done after the setup() function so that
        # we can override the logging class for multiprocessing.
        self._log = zrlog.get_logger(f'pipeman_service.runner.{self._proc_info.process_name}[{self._proc_info.process_index}]')

        try:
            # actually build and run the worker
            self._build_and_run()
        finally:
            # cleanup tasks
            self.teardown()

    def teardown(self):
        """ Runs cleanup tasks after the worker is shutdown. """
        pass

    def setup(self):
        """ Runs setup tasks before the worker is built. """
        pass


class _RunnerSet:
    """ Represents a set of work runners all running the same worker.

        A runner set maintains 1 or more work runners, depending on the quota.
        Each work runner is identical.
    """

    def __init__(self,
                 process_cls: _WorkRunnerCreatorProtocol,
                 process_info: WorkerInfo):
        """ Builds the object.
            :param process_cls: A type or function which builds work runner objects.
            :param process_info: The process information to pass to the work runners.
        """
        self._process_cls = process_cls
        self._info = process_info
        self._active_runners: dict[str, _WorkRunnerProtocol] = {}
        self._spawn_workers = True
        self._log = zrlog.get_logger(f'pipeman_service.runner_set.{process_info.process_name}')
        self._idx = 0

    def _build_runner(self) -> tuple[_WorkRunnerProtocol, str]:
        """ Creates a new process runner object. """

        # Increment the index
        self._idx += 1

        # Get the current time that the object was created at
        n = AwareDateTime.now().strftime("%M%S%f")

        # Build a unique process ID
        proc_id = f"{self._info.server_name}:{self._info.process_name}:{self._idx}:{n}"

        # Copy the server info and update the information from the above.
        x = copy.copy(self._info)
        x.process_index = self._idx
        x.process_uuid = proc_id

        # Build and return the process runner.
        obj = self._process_cls(proc_info=x)
        return obj, proc_id

    def set_quota(self, new_count: int):
        """ Sets the target number of workers to have running. """
        self._info.quota = new_count if new_count > 1 else 1

    def deactivate(self):
        """ Stops all the workers gracefully and stops spawning new workers. """
        self._spawn_workers = False
        self.shutdown_all()

    def activate(self):
        """Starts spawning new workers. """
        self._spawn_workers = True

    def shutdown_all(self):
        """Requests that all process runners stop as soon as possible."""
        self._log.trace('Shutting down all active processes')
        for x in self._active_runners:
            self._active_runners[x].shutdown()

    def is_active(self, _no_reap: bool = False) -> bool:
        """ Checks if any process runners are still active. """
        if not _no_reap:
            self._reap()
        return len(self._active_runners) > 0

    def is_activated(self) -> bool:
        """ Checks if the process set is currently active. """
        return self._spawn_workers

    def set_config(self, config: t.Mapping[str, t.Any]):
        """ Sets the configuration for new workers, and restarts any currently running ones.

            :param config: The mapping of configuration items. Must be JSON-compatible.

            :raises PipemanServiceError: If the configuration is not a mapping (2000)
            :raises PipemanServiceError: If there was an error in JSON encoding (2001).
        """
        # convert to string
        config_str = WorkerInfo.config_to_str(config)

        # check for reset
        reset_all = config_str != self._info.config_str

        # save config
        self._info.config_str = config_str

        # restart workers if necessary
        if reset_all:
            self.shutdown_all()

    def run_one(self):
        """ Runs a single process.

        .. danger:: This is used in testing, not production code. """
        proc, _ = self._build_runner()
        proc.run()
        return proc

    def reap_and_sow(self):
        """ Clears workers that have errored, then starts enough workers to meet the quota."""
        self._reap()
        self._sow()

    def _reap(self):
        """ Clears workers that have errored. """
        for x in list(self._active_runners.keys()):
            if not self._active_runners[x].is_alive():
                self._log.info('Removing dead process %s', x)
                del self._active_runners[x]

    def _sow(self):
        """ Starts new workers as needed."""
        if self._spawn_workers:
            current = len(self._active_runners)
            while current < self._info.quota:
                self._log.info('Starting new process')
                proc, proc_uuid = self._build_runner()
                self._active_runners[proc_uuid] = proc
                proc.start()
                current += 1


def original_signal_handler(*args, **kwargs):
    """ Handles signals like Python does by default. """
    raise KeyboardInterrupt


class BaseController(InstrumentedObject):
    """ Base class for controllers.

        A controller orchestrates workers by creating :py:class:`_RunnerSet` objects which
        orchestrate workers for a single type of worker.
    """

    def __init__[T: _WorkRunnerProtocol](self,
                                         process_creator: _WorkRunnerCreatorProtocol | type[T],
                                         log_name: str,
                                         halt_flag: ct.SupportsEvent,
                                         config_file: t.Optional[pathlib.Path] = None,
                                         config_file_dir: t.Optional[pathlib.Path] = None,
                                         flag_file: t.Optional[pathlib.Path] = None,
                                         server_name: t.Optional[str] = None,
                                         socket_port: int = 9173,
                                         logging_queue: queue.Queue | None = None,
                                         _no_start: bool = False,
                                         _is_top_level: bool = True,
                                         _no_report: bool = True):
        """ Builds the controller object.

            :param process_creator: A callable or type which can be used to build a work runner.
            :param log_name: The name of a logger for this controller.
            :param halt_flag: An object which can be used to indicate that the system is shutting down.
            :param config_file: A YAML configuration file containing one or more worker definitions.
            :param config_file_dir: A directory containing one or more YAML configuration files.
            :param flag_file: A file that, if it exists, will prompt the controller to reload configuration.
            :param server_name: The name of the server, used in reporting metrics.
            :param socket_port: The socket to listen to for commands.
            :param logging_queue: A shared queue for logging (used in multiprocessing).
            :param _no_start: Prevents `start()` from being called on the worker objects. Testing only!
            :param _is_top_level: Indicates this is the top-level process if true - this controls if disk space is reported.
            :param _no_report: If true, disables reporting of metrics to the database.
        """
        super().__init__(
            log_name=log_name,
            subsystem='controller',
            server_name=server_name or '',
            process_id=AwareDateTime.now().strftime("%Y%M%d%H%M%f"),
            process_name=f"controller",
            process_version='1.0',
            no_report=_no_report,
            is_top_level=_is_top_level,
        )
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
        self._process_info: dict[str, _RunnerSet] = {}
        self._no_start = _no_start
        self._logging_queue = logging_queue
        self._sleep_time = 2.5

        from medsutil.servicecmd import ServiceCommandManager
        self._manager = ServiceCommandManager(socket_port, self._handle_command)
        self._base_kwargs = {
            "halt_flag": self._halt_flag,
            "no_start": self._no_start,
            "logging_queue": self._logging_queue,
            "signals": "",
        }

    def _register_worker(self,
                         process_name: str,
                         process_cls_name: str,
                         count: int = 1,
                         config: t.Mapping[str, ct.SupportsExtendedJson] | None = None):
        """ Registers a new worker with the controller.

            .. note: The runner set is also activated again, which will launch
            the workers again if it was previously deactivated.

            :param process_name: The name of the worker (should be unique across all workers).
            :param process_cls_name: The fully-qualified name of the worker class.
            :param count: The number of worker processes to build.
            :param config: The configuration for the worker, if applicable.

            :raises PipemanServiceError: If the configuration is not a mapping (2000)
            :raises PipemanServiceError: If there was an error in JSON encoding (2001).
        """
        if process_name not in self._process_info:
            self._log.debug("Registering process %s", process_name)
            self._process_info[process_name] = _RunnerSet(self._process_runner_cls, WorkerInfo(
                process_name=process_name,
                worker_cls=process_cls_name,
                quota=count,
                config_str=WorkerInfo.config_to_str(config or {}),
                server_name=self.server_name,
                **self._base_kwargs
            ))
        else:
            self._log.debug("Updating process %s", process_name)
            self._process_info[process_name].set_quota(count)
            self._process_info[process_name].set_config(config or {})
            self._process_info[process_name].activate()

    def _deactivate_worker(self, process_name: str):
        """ Deactivates the given runner set and gracefully stops all workers. """
        if process_name in self._process_info:
            self._log.debug("Deactivating process %s", process_name)
            self._process_info[process_name].deactivate()

    def _registered_process_names(self) -> list[str]:
        """ Returns a list of registered process names. """
        return list(self._process_info.keys())

    def _check_reload(self):
        """ Checks if the configuration needs to be reloaded. """

        # always reload if we haven't yet loaded configuration
        if not self._loaded:
            return True

        # check if the flag file has been set
        if self._flag_file is not None and self._flag_file.exists():
            self._log.debug("Flag file detected, reloading configuration")
            self._flag_file.unlink(True)
            self._reload_requested = False
            return True

        # check if the service has received a request for reload
        if self._reload_requested:
            self._reload_requested = False
            return True

        return False

    def _register_halt_signal(self, sig_name):
        """ Registers a halt signal. """
        if hasattr(signal, sig_name):
            self._log.debug("Registering halt signal %s", sig_name)
            signal.signal(getattr(signal, sig_name), self._handle_halt)
            self._signals.add(sig_name)

    def _handle_halt(self, sig_num, frame=None):
        """ Handles a halt signal.

            :raises KeyboardInterrupt: If three halt signals were caught.
        """
        self._log.info("Signal %s caught", sig_num)
        self._halt_flag.set()
        self._break_count += 1
        self._log.debug("Break count [%s], halt flag [%s]", self._break_count, self._halt_flag.is_set())
        if self._break_count >= 3:
            self._log.critical("3 or more halt signals were caught, crashing out")
            raise KeyboardInterrupt

    def cleanup(self):
        """ Cleans up before shutdown. """
        for sig_name in self._signals:
            signal.signal(getattr(signal, sig_name), original_signal_handler)
        self._signals = set()
        self._manager.cleanup()

    def _load_config_from_file(self, file: t.Union[str, pathlib.Path]) -> dict[str, t.Any]:
        """ Loads configuration from a YAML file """
        self._log.debug('Loading configuration from %s', file)
        with open(file, "r", encoding="utf-8") as h:
            data = yaml.safe_load(h)
            if isinstance(data, dict):
                return data
            else:
                self._log.error("Process configuration file [%s] does not contain a YAML dictionary", file)
                return {}

    def _load_config(self) -> dict[str, t.Any]:
        """ Loads configuration from the config directory and file specified.

            :returns: The configuration dictionary.
            :raises PipemanServiceError: If there are no workers defined (3000).
        """

        # holds all the configuration items
        config = {}

        # check the directory
        if self._config_dir is not None:

            if not self._config_dir.exists():
                self._log.error("Config directory [%s] specified but does not exist", self._config_dir)

            elif not self._config_dir.is_dir():
                self._log.error("Config directory [%s] is not a directory", self._config_dir)

            else:
                self._log.debug("Reading configuration from YAML files in %s", self._config_dir)
                work_dirs = [str(self._config_dir)]
                while work_dirs:
                    for file in os.scandir(work_dirs.pop()):
                        if file.name.lower().endswith(".yaml"):
                            config.update(self._load_config_from_file(file.path))
                        elif file.is_dir():
                            work_dirs.append(file.path)

        # check the config file
        if self._config_file is not None:
            if not self._config_file.exists():
                self._log.error('Config file [%s] specified but does not exist', self._config_file)
            else:
                config.update(self._load_config_from_file(self._config_file))

        # Ensure we have at least one worker
        if not config:
            raise PipemanServiceError("No workers defined", 3000)

        return config

    def _process_config_entry(self,
                              process_name: str,
                              config: dict[str, t.Any]) -> tuple[dict[str, ct.SupportsExtendedJson], int]:
        """

        :param process_name: The process name to check
        :param config: The configuration for the process
        :return: A tuple containing the configuration dictionary and number of workers to spawn
        :raises PipemanServiceError: If class_name is missing (4000).
        :raises PipemanServiceError: If the worker class does not exist (4001).
        :raises PipemanServiceError: If the worker class doesn't have a run method (4002).
        :raises PipemanServiceError: If the configuration exists and is not a dictionary (4003).
        :raises PipemanServiceError: If the spawn count is specified and is not a number (4004).
        """
        # ensure there is a class name
        if 'class_name' not in config:
            raise PipemanServiceError(f"Invalid worker definition for [{process_name}], no class_name provided", 4000)

        # check we can load the class and it looks appropriate
        cls_name = config['class_name']
        try:
            from medsutil.dynamic import dynamic_object
            cls = dynamic_object(cls_name)
            if not hasattr(cls, 'run'):
                raise PipemanServiceError(f"Invalid worker definition [{cls_name}] for [{process_name}], object has no run() method", 4002)
        except DynamicObjectLoadError as ex:
            raise PipemanServiceError(f"Invalid worker definition, class_name [{cls_name}] for [{process_name}] does not exist", 4001) from ex

        # Validate configuration
        proc_config = {}
        if 'config' in config and config['config']:
            if isinstance(config['config'], dict):
                proc_config = config['config']
            else:
                raise PipemanServiceError(f"Invalid worker definition, configuration for [{process_name}] must be a dictionary", 4003)

        # Validate count
        count = 1
        if 'count' in config and config['count']:
            try:
                count = max(int(config['count']), 1)
            except (TypeError, ValueError) as ex:
                raise PipemanServiceError(f"Invalid worker definition, expecting numeric value number of processes for [{process_name}]", 4004)

        return proc_config, count

    def _reload_config(self):
        """ Reload configuration from disks.

            :raises PipemanServiceError: If there are errors in the configuration (2000-2001, 4000-4004).
        """
        # reporting
        self._log.trace('Reloading process configuration')
        self.report(activity='reloading configuration')

        # load the configuration from disk
        config = self._load_config()

        # We will remove keys from this list as we see them and deregister all the ones we didn't see
        deregister_list = self._registered_process_names()

        for process_name in config:

            # Remove the process name immediately, so we don't stop them if there is an error processing the definition
            if process_name in deregister_list:
                deregister_list.remove(process_name)

            # Process the configuration
            proc_config, count = self._process_config_entry(process_name, config[process_name])

            # Register the process
            self._register_worker(
                process_name,
                config[process_name]['class_name'],
                count,
                proc_config,
            )

        # Deregister processes not found in current list
        for process_name in deregister_list:
            self._deactivate_worker(process_name)

        # Flag that we have loaded the configuration once at least
        self._loaded = True

    def reload_check(self):
        """ Reloads configuration if it is necessary """
        if self._check_reload():
            self._reload_config()

    def startup(self):
        """ Initializes everything for the control loop. """

        # Register signals (if they're available on this OS)
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        self._register_halt_signal("SIGABRT")

        # Update the signals keyword arguments
        self._base_kwargs['signals'] = "\n".join(self._signals)

        # Load configuration
        self.reload_check()

        # Set-up the port manager
        self._manager.setup()

    def start(self):
        """ Starts the run loop with cleanup. """
        try:
            self.report(status='booting', activity='', _update_resources=True)
            self.startup()
            self.report(status='running', activity='', _update_resources=True)
            self.run()
        finally:
            self.report(status='stopping', activity='cleanup')
            self.cleanup()
            self.report(Status='stopped', activity='', _update_resources=True, exit=True)

    def run(self):
        """ Runs in a loop until a halt signal is sent. """
        self._log.notice('Pipeman service controller online')
        try:
            # Our loop
            while not (self._halt_flag.is_set() or self._shutdown_requested):

                # check for configuration changes
                self.reload_check()

                # launch new workers as necessary
                self.report(activity="reaping and sowing")
                self.reap_and_sow()

                # check for signals from the manager
                self.report(activity="checking for signals", _update_resources=True)
                self._manager.check()

                # handle the kill signal
                if self._kill_requested:
                    raise SystemExit(1)

                # wait on the flag to be set
                if hasattr(self._halt_flag, 'wait'):
                    self._halt_flag.wait(self._sleep_time)

        finally:

            # graceful shutdown only if kill was not requested
            if not self._kill_requested:

                # shutdown and wait for all processes
                self._log.info('Closing processes')
                self.report(activity='shutting down processes')
                self.deactivate_all()
                self.wait_for_all()

                # report
                self._log.info('Complete, exiting')
                self.report(activity='shutdown', _last_report=True)

            else:

                # critical exit
                self._log.critical("Kill requested, some processes may not have gracefully exited")

    def _handle_command(self, message: bytes, address):
        """ Handles a command sent to the pipe. """
        match message:

            # health check, just return 0 indicating success
            case b'health':
                return b'0'

            # interrupt, set the halt flag as if we had received the appropriate signal
            case b'interrupt':
                self._halt_flag.set()
                return b'0'

            # graceful shutdown
            case b'shutdown':
                self._shutdown_requested = True
                return b'0'

            # reload configuration
            case b'reload':
                self._reload_requested = True
                return b'0'

            # immediate exit next time we exit the loop
            case b'kill':
                self._halt_flag.set()
                self._kill_requested = True
                return b'0'

            # invalid command
            case _:
                return b'1'

    def deactivate_all(self):
        """ Requests that every process stop immediately. """
        self._log.trace(f"Requesting all processes to halt")
        for process_name in self._process_info:
            self._process_info[process_name].deactivate()

    def wait_for_all(self, max_time: float = 0) -> bool:
        """ Blocks until all processes have stopped or the given time has passed.
            :param max_time: The time to wait.
            :returns: True if all processes have stopped, False otherwise.
        """
        complete = False
        start_time = time.monotonic()
        while not complete:
            complete = True
            for process_name in self._process_info:
                if self._process_info[process_name].is_active():
                    self._log.debug("Process %s has not yet exited", process_name)
                    complete = False
            elapsed = time.monotonic() - start_time
            if elapsed > max_time:
                return complete
            if not complete:
                time.sleep(0.05)
        return complete

    def reap_and_sow(self):
        """ Runs reap_and_sow() for every runner set. """
        self._log.trace('Requesting all runners to reap and sow')
        for process_name in self._process_info:
            self._process_info[process_name].reap_and_sow()
