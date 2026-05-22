import contextlib
import logging
import tempfile
import uuid

import psutil
import zrlog
from autoinject import injector

import medsutil.metrics as mum
from medsutil.exceptions import CodedError
import nodb.interface as interface


class InstrumentedObject:

    nodb: interface.NODB = None

    @injector.construct
    def __init__(self,
                 *args,
                 log_name: str,
                 namespace: str = 'pipeman',
                 subsystem: str = '',
                 server_name: str = '',
                 process_id: str = '',
                 process_name: str = '',
                 process_version: str = '',
                 no_report: bool = False,
                 is_master: bool = False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._status_info = {
            'status': 'uninitialized',
            'activity': '',
        }
        self._psutil_process = psutil.Process()
        self._metrics = {}
        self._metrics_metadata = {
            'namespace': namespace,
            'subsystem': subsystem,
        }
        self._pinfo = {
            'server_name': server_name or str(uuid.uuid4()),
            'process_id': process_id,
            'process_name': process_name,
            'process_version': process_version
        }
        self._is_master = is_master
        self._log = zrlog.get_logger(log_name)
        self._no_report: bool = no_report
        zrlog.set_extras(self._pinfo)

    @property
    def process_full_id(self):
        return f"{self.process_name}:{self.process_version}:{self.process_uuid}"

    @property
    def server_name(self):
        return self._pinfo['server_name']

    @property
    def process_name(self):
        return self._pinfo['process_name']

    @property
    def process_uuid(self):
        return self._pinfo['process_id']

    @property
    def process_version(self):
        return self._pinfo['process_version']

    def _disable_reporting(self):
        self._no_report = True

    def report(self,
               _last_report: bool = False,
               _update_resources: bool = False,
               **kwargs):
        if _update_resources:
            self._update_resources_report()
        if not self._no_report:
            self._status_info.update(kwargs)
            try:
                self._make_report(_last_report)
            except CodedError:
                logging.getLogger("instrumentation").exception("Exception while reporting")

    def _update_resources_report(self):
        with self._psutil_process.oneshot():
            try:
                if self._is_master:
                    with tempfile.TemporaryDirectory() as tdir:
                        du = psutil.disk_usage(tdir)
                        self._status_info['temp_free'] = du.free
                        self._status_info['temp_total'] = du.total
                        if self._is_master:
                            self.gauge('temp_free_bytes', 'Free disk space').set(du.free)
                            self.gauge('temp_free_ratio', 'Free disk space (ratio)').set(du.free / du.total)

                cpu_times = self._psutil_process.cpu_times()
                self._status_info['cpu_percent'] = self._psutil_process.cpu_percent()
                self._status_info['cpu_user'] = cpu_times.user
                self._status_info['cpu_system'] = cpu_times.system
                if hasattr(cpu_times, 'iowait'):
                    self._status_info['cpu_iowait'] = cpu_times.iowait

                mem_info = self._psutil_process.memory_full_info()
                self._status_info['memory_total'] = mem_info.uss

                if self._is_master:
                    self.gauge("cpu_usage_ratio", "CPU Usage").set(self._psutil_process.cpu_percent())
                    self.gauge("cpu_user_time_seconds", "CPU Usage (User)").set(cpu_times.user)
                    self.gauge("cpu_system_time_seconds", "CPU Usage (System)").set(cpu_times.system)
                    if hasattr(cpu_times, 'iowait'):
                        self.gauge("cpu_iowait_time_seconds", "CPU Usage (IOWait)").set(cpu_times.iowait)
                    self.gauge("memory_used_bytes", "Memory Used (bytes)").set(mem_info.uss)
                    self.gauge("memory_used_ratio", "Memory Used (ratio)").set(mem_info.uss / psutil.virtual_memory().total)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                ...

    def _make_report(self, _last_report: bool = False):
        with self.nodb as db:
            db.upsert_process_info(
                **self._pinfo,
                info=self._status_info
            )
            if _last_report:
                if self._is_master:
                    db.clear_process_info_for_server(self.server_name)
                else:
                    db.clear_process_info(
                        self.server_name,
                        self.process_uuid
                    )

    def gauge(self, name: str, description: str = "", labels: list[str] | tuple | None = None, multiprocess_mode="livemostrecent", **kwargs) -> mum.Gauge:
        key = f'gauge_{name}'
        if key not in self._metrics:
            kwargs.update(self._metrics_metadata)
            self._metrics[key] = mum.Gauge(name=name, documentation=description, labelnames=labels or tuple(), multiprocess_mode=multiprocess_mode, **kwargs)
        return self._metrics[key]

    def histogram(self, name: str, description: str = "", labels: list[str] | tuple = None, **kwargs) -> mum.Histogram:
        key = f"histogram_{name}"
        if key not in self._metrics:
            kwargs.update(self._metrics_metadata)
            self._metrics[key] = mum.Histogram(name=name, documentation=description, labelnames=labels or tuple(), **kwargs)
        return self._metrics[key]

    def counter(self, name: str, description: str = "", labels: list[str] | tuple | None = None, **kwargs) -> mum.Counter:
        key = f"counter_{name}"
        if key not in self._metrics:
            kwargs.update(self._metrics_metadata)
            self._metrics[key] = mum.Counter(name=name, documentation=description, labelnames=labels or tuple(), **kwargs)
        return self._metrics[key]

    def count(self, name, **kwargs):
        if kwargs:
            self.counter(name, labels=list(kwargs.keys())).labels(**kwargs).inc()
        else:
            self.counter(name).inc()
