import threading
from contextlib import contextmanager

import prometheus_client as pc
import prometheus_client.multiprocess as pcmp
from prometheus_client.metrics import MetricWrapperBase
from prometheus_flask_exporter import PrometheusMetrics
import os
import zrlog
from autoinject import injector


@injector.injectable_global
class PromMetrics:

    def __init__(self):
        self.disable_metrics: bool = False
        self.metric_flask = None
        self._metrics = {}
        self._lock = threading.RLock()
        self.log = zrlog.get_logger("medsutil.metrics")
        self._collector = None
        self._reg = None

    def init_metrics(self, for_mp: bool = True):
        if self._reg is None:
            self._reg = pc.CollectorRegistry()
            if os.environ.get("PROMETHEUS_MULTIPROC_DIR", default=None):
                self._collector = pcmp.MultiProcessCollector(self._reg)
            else:
                if for_mp and not self.disable_metrics:
                    self.log.warning("PROMETHEUS_MULTIPROC_DIR not set, Prometheus metrics may be corrupt if using a multi-process WSGI server")
                self._reg = self._reg

    def init_app(self, app):
        self.init_metrics(True)
        if self.metric_flask is None:
            if "medsutil" not in app.extensions:
                app.extensions["medsutil"] = {}
            self.metric_flask = PrometheusMetrics(app, registry=self._reg)
            app.extensions["medsutil"]["prometheus_registry"] = self._reg
            app.extensions["medsutil"]["prometheus_collector"] = self._collector
            app.extensions["medsutil"]["prometheus_metrics"] = self.metric_flask

    def get_stat(self, name, _metric_cls: type[MetricWrapperBase], documentation='', **kwargs):
        self.init_metrics(True)
        if name not in self._metrics:
            with self._lock:
                if name not in self._metrics:
                    self._metrics[name] = _metric_cls(name, documentation, registry=self._reg if self._collector is None else None, **kwargs)
        return self._metrics[name]

class BaseMetric[X: MetricWrapperBase]:

    metrics: PromMetrics = None

    @injector.construct
    def __init__(self, name, _metric_cls: type[X], **kwargs):
        self._metric: X = self.metrics.get_stat(name, _metric_cls=_metric_cls, **kwargs)

    def __getattr__(self, item):
        if self.metrics.disable_metrics:
            if item.startswith('_disabled_'):
                return lambda *args, **kwargs: ...
            else:
                key = f"_disabled_{item}"
                return getattr(self, key)
        else:
            return getattr(self._metric, item)

    def _disabled_labels(self, *args, **kwargs):
        return self

    @contextmanager
    def _disabled_track_inprogress(self, *args, **kwargs):
        yield self

    @contextmanager
    def _disabled_count_exceptions(self, *args, **kwargs):
        yield self

    @contextmanager
    def _disabled_time(self, *args, **kwargs):
        yield self


class Gauge(BaseMetric[pc.Gauge]):

    def __init__(self, name, **kwargs):
        super().__init__(name,**kwargs, _metric_cls=pc.Gauge)


class Counter(BaseMetric[pc.Counter]):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs, _metric_cls=pc.Counter)


class Summary(BaseMetric[pc.Summary]):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs, _metric_cls=pc.Summary)


class Histogram(BaseMetric[pc.Histogram]):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs, _metric_cls=pc.Histogram)


