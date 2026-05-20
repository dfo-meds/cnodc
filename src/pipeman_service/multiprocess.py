"""Controller for multiple processes based on the multiprocessing library."""
import typing as t
import os
import tempfile

import psutil
from autoinject import injector
import multiprocessing as mp
import zirconium as zr
from prometheus_client.multiprocess import mark_process_dead
from psutil import NoSuchProcess, AccessDenied

from nodb import NODB
from pipeman_service.controller import BaseController, BaseProcess


class ImprovedEvent:

    def __init__(self):
        self.value = mp.Value('b')
        self.value.value = 0

    def is_set(self):
        return self.value.value == 1

    def set(self):
        self.value.value = 1

    def clear(self):
        self.value.value = 0


class _MultiProcessRunner(BaseProcess, mp.Process):
    """Implementation of a process that runs a worker class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, end_flag=ImprovedEvent(), daemon=True)

    def setup(self):
        from pipeman.boot import init_pipeman
        init_pipeman('cli')
        if self._proc_info.logging_queue is not None:
            import medsutil.logging as ml
            ml.init_as_subprocess(self._proc_info.logging_queue)
        super().setup()

    def teardown(self):
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR", default=None):
            mark_process_dead(os.getpid())
        super().teardown()



class MultiProcessController(BaseController):
    """Controller for running multiple workers based on the multiprocessing library.

        Workers are defined in a configuration file that includes which class to
        load, how many workers to run, and their configuration.

        The controller also defines a flag file that, if set, will cause the controller
        to reload all configuration from the original file and update all
        running processes. This is useful to make configuration changes on the fly without
        restarting the entire system.
    """

    config: zr.ApplicationConfig = None
    nodb: NODB = None

    @injector.construct
    def __init__(self, **kwargs):
        import medsutil.logging as ml
        lc = self.config.as_dict("logging", default={})
        lq = mp.Queue()
        self._logging_subprocess = ml.init_subprocess_handler(
            lq,
            logging_config={
                x: lc[x]
                for x in lc.keys()
            }
        )
        ml.init_as_subprocess(lq)
        super().__init__(
            process_creator=_MultiProcessRunner,
            log_name="cnodc.multi_process",
            logging_queue=lq,
            halt_flag=ImprovedEvent(),
            **kwargs
        )
        self._process = psutil.Process()

    def _resource_report(self):
        with self._process.oneshot():
            try:
                with tempfile.TemporaryDirectory() as tdir:
                    du = psutil.disk_usage(tdir)
                    self._status_info['temp_free'] = du.free
                    self._status_info['temp_total'] = du.total

                cpu_times = self._process.cpu_times()
                self._status_info['cpu_percent'] = self._process.cpu_percent()
                self._status_info['cpu_user'] = cpu_times.user
                self._status_info['cpu_system'] = cpu_times.system
                if hasattr(cpu_times, 'iowait'):
                    self._status_info['cpu_iowait'] = cpu_times.iowait

                mem_info = self._process.memory_full_info()
                self._status_info['memory_total'] = mem_info.uss

            except (NoSuchProcess, AccessDenied):
                ...

    def report(self, with_resource: bool = False, _exit: bool = False, **kwargs):
        self._status_info.update(kwargs)
        if with_resource:
            self._resource_report()
        if not self._no_report:
            with self.nodb as db:
                db.upsert_process_info(
                    self._server_name or '',
                    self._controller_proc_name,
                    'controller',
                    '1.0',
                    self._status_info
                )
                if _exit:
                    db.clear_process_info_for_server(t.cast(str, self._server_name))

    def startup(self):
        super().startup()

    def cleanup(self):
        super().cleanup()
        self._logging_subprocess.stop()
