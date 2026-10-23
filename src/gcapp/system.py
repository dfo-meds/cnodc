"""
    Provides the basic System class that is the core of an application.
"""

import threading
import typing as t
import enum

import zirconium as zr
import zrlog
from autoinject import injector, auto

from gcapp.eventman import EventManager
from gcapp.pluginman import PluginManager


class MetricsType(enum.Enum):
    DISABLED = 0
    THREADED = 1
    MULTIPROCESSING = 2


@injector.injectable_global
class System:
    """Core management of plugins and configuration."""

    config: zr.ApplicationConfig = auto()
    events: EventManager = auto()
    plugins: PluginManager = auto()

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger('gcapp.system')
        self._lock = threading.RLock()
        self._metrics_type: MetricsType = MetricsType.DISABLED
        self._click_commands: list[tuple[str, str, str]] = []

    def enable_metrics(self, is_mp: bool = True):
        """ Enables multiprocessing or threaded metrics. """
        if is_mp:
            self._metrics_type = MetricsType.MULTIPROCESSING
        else:
            self._metrics_type = MetricsType.THREADED

    def disable_metrics(self):
        """ Disables metrics. """
        self._metrics_type = MetricsType.DISABLED

    def init(self, *args, **kwargs):
        """ Initializes the application for the first time. """
        self.events.fire("init.before", self)
        self.plugins.init_plugins(self)
        self.events.fire("init", self)
        self.events.fire("init.after", self)
        self._subclass_init()
        if self._metrics_type is not MetricsType.DISABLED:
            from gcapp.metrics import PromMetrics
            @injector.inject
            def _run_me(prom_metrics: PromMetrics = auto()):
                prom_metrics.init_metrics(self._metrics_type is MetricsType.MULTIPROCESSING)
            _run_me()

    def before_load(self, load_cb: t.Callable[[t.Self], t.Any] | str):
        """ Registers a function to call at the start of init(). """
        self.events.on("init.before", load_cb)

    def on_load(self, load_cb: t.Callable[[t.Self], t.Any] | str):
        """ Registers a function to call during init(). """
        self.events.on("init", load_cb)

    def after_load(self, load_cb: t.Callable[[t.Self], t.Any] | str):
        """ Registers a function to call after init(). """
        self.events.on("init.after", load_cb)

    def _subclass_init(self):
        """ Additional initialization code for subclasses. """
        ...

    def setup(self):
        """ Runs all the setup scripts. """
        self.events.fire("setup.before")
        self.events.fire("setup")
        self.events.fire("setup.data")
        self.events.fire("setup.after")

    def before_setup(self, cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call at the start of setup(). """
        self.events.on("setup.before", cb)

    def on_setup(self, setup_cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call on setup. """
        self.events.on("setup", setup_cb)

    def on_setup_data(self, setup_cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call on setup.data """
        self.events.on("setup.data", setup_cb)

    def after_setup(self, cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call after setup(). """
        self.events.on("setup.after", cb)

    def cleanup(self):
        """ Runs all the cleanup scripts. """
        self.events.fire("cleanup.before")
        self.events.fire("cleanup")
        self.events.fire("cleanup.after")

    def before_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call at the start of cleanup(). """
        self.events.on("cleanup.before", cleanup_cb)

    def on_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call during cleanup(). """
        self.events.on("cleanup", cleanup_cb)

    def after_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        """ Registers a function to call after cleanup(). """
        self.events.on("cleanup.after", cleanup_cb)
