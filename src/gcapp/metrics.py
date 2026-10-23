"""
    Prometheus metrics wrapper, which can disable metrics during a test and helps ensuring metrics are appropriately
    handled by classes.

    When adding metrics to your code, you can use the Gauge, Counter, Summary, and Histogram classes available here.
    If metrics are disabled, they are replaced with no-op versions of themselves. When metrics are enabled, the classes
    are actually wrappers around the real Prometheus object which is kept in a global dictionary.

    Multiprocessing support is handled automatically for you too.

    Web applications should ensure init_metrics() is called [or init_app() for Flask-based applications].

"""
from contextlib import contextmanager
import os
import typing as t
import threading

import zrlog
from autoinject import injector, auto

if t.TYPE_CHECKING:
    from prometheus_client.metrics import MetricWrapperBase
    from flask import Flask


# Used to disable metrics in tests only!
DISABLE_METRICS = threading.Event()


@injector.injectable_global
class PromMetrics:
    """Injectable class to manage metrics."""

    def __init__(self):
        self._flask_extension = None
        self._metrics = {}
        self._lock = threading.RLock()
        self._log = zrlog.get_logger("gcapp.metrics")
        self._collector = None
        self._reg = None

    @property
    def is_disabled(self) -> bool:
        """ Returns true if metrics are disabled, otherwise false"""
        return DISABLE_METRICS.is_set()

    def init_metrics(self, for_mp: bool = True):
        """ Initializes the Prometheus registry and multiprocessing support if requested """
        if self._reg is None:

            # Standard Prometheus registry
            import prometheus_client as pc
            self._reg = pc.CollectorRegistry()

            # Multiprocessing support
            if os.environ.get("PROMETHEUS_MULTIPROC_DIR", default=None):
                import prometheus_client.multiprocess as pcmp
                self._collector = pcmp.MultiProcessCollector(self._reg)

            # Warning if multiprocessing directory is not specified
            elif for_mp and not self.is_disabled:
                self._log.warning("PROMETHEUS_MULTIPROC_DIR not set, Prometheus metrics may be corrupted")

    def init_app(self, app: Flask, for_mp: bool = True):
        """ Initializes Prometheus metrics for a Flask application. """
        self.init_metrics(for_mp=for_mp)
        if self._flask_extension is None:

            # Build the Flask extension
            from prometheus_flask_exporter import PrometheusMetrics
            self._flask_extension = PrometheusMetrics(app, registry=self._reg)

            # Add to the extensions list
            if "medsutil" not in app.extensions:
                app.extensions["medsutil"] = {}
            app.extensions["medsutil"]["prometheus_registry"] = self._reg
            app.extensions["medsutil"]["prometheus_collector"] = self._collector
            app.extensions["medsutil"]["prometheus_metrics"] = self._flask_extension

    def get_stat(self, name, _metric_cls: type[MetricWrapperBase], documentation='', **kwargs) -> MetricWrapperBase:
        """ Retrieves a Prometheus metrics class, with caching.

            :param name: The name of the metric - will be cached and shared.
            :param _metric_cls: The metric class to build
            :param documentation: The documentation for the metric
            :param kwargs: Additional keyword arguments passed to _metric_cls

            :returns: The metric object
        """

        # Ensure metrics are enabled
        if self._reg is None:
            self.init_metrics(True)

        # Build new item, if necessary
        if name not in self._metrics:
            with self._lock:
                # we check again in case another object was building it while we were waiting on the lock
                if name not in self._metrics:
                    self._metrics[name] = _metric_cls(
                        name,
                        documentation,
                        registry=self._reg if self._collector is None else None,
                        **kwargs
                    )

        return self._metrics[name]


class BaseMetric[X]:
    """ Base wrapper for Prometheus metrics.

        Mostly this helps handle the disabled state by returning no-op functions instead of the actual ones.
        Otherwise, it's simply a wrapper around the Prometheus object.
    """

    metrics: PromMetrics = auto()

    @injector.construct
    def __init__(self, name: str, _metric_cls: type[X], **kwargs):
        self._metric: X = self.metrics.get_stat(name, _metric_cls=_metric_cls, **kwargs)

    def __getattr__(self, item):
        """ Implements a wrapper around the Prometheus object that can be disabled.

            When the metric is enabled, we find and return the original object's class.

            When it is disabled, we return a no-op function. The default is one that returns None.
            It is sometimes overridden where returning None isn't appropriate. These overrides start with _disabled_
            here.

            To add an override, make a method to replace the one you want but that starts with _disabled_.
        """
        if self.metrics.is_disabled:

            # this is the default no-op function
            if item.startswith('_disabled_'):
                return lambda *args, **kwargs: ...

            # this maps the item we requested to one called _disabled_{item}.
            # we then use getattr() to allow us to override it easily if the default
            # no-op is not useful
            else:
                key = f"_disabled_{item}"
                return getattr(self, key)
        else:

            # when metrics are enabled, we just defer to the Prometheus object
            return getattr(self._metric, item)

    def _disabled_labels(self, *args, **kwargs):
        """ Override for labels() that returns this wrapper object. """
        return self

    @contextmanager
    def _disabled_track_inprogress(self, *args, **kwargs):
        """ Override for track_inprogress() that returns this wrapper object. """
        yield self

    @contextmanager
    def _disabled_count_exceptions(self, *args, **kwargs):
        """ Override for count_exceptions() that returns this wrapper object. """
        yield self

    @contextmanager
    def _disabled_time(self, *args, **kwargs):
        """ Override for time() that returns this wrapper object. """
        yield self


class Gauge(BaseMetric):
    """ Wrapper for prometheus_client.Gauge """

    def __init__(self, name, **kwargs):
        import prometheus_client as pc
        super().__init__(name,**kwargs, _metric_cls=pc.Gauge)


class Counter(BaseMetric):
    """ Wrapper for prometheus_client.Counter """

    def __init__(self, name, **kwargs):
        import prometheus_client as pc
        super().__init__(name, **kwargs, _metric_cls=pc.Counter)


class Summary(BaseMetric):
    """ Wrapper for prometheus_client.Summary """

    def __init__(self, name, **kwargs):
        import prometheus_client as pc
        super().__init__(name, **kwargs, _metric_cls=pc.Summary)


class Histogram(BaseMetric):
    """ Wrapper for prometheus_client.Histogram """

    def __init__(self, name, **kwargs):
        import prometheus_client as pc
        super().__init__(name, **kwargs, _metric_cls=pc.Histogram)
