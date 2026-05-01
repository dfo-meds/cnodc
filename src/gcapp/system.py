import importlib
import threading
import typing as t

import flask
import zrlog
from autoinject import injector, NamedContextInformant
import zirconium as zr

from gcapp.eventman import EventManager
from gcapp.pluginman import PluginManager


@injector.injectable_global
class System:
    """Core management of plugins and configuration."""

    config: zr.ApplicationConfig = None
    events: EventManager = None
    plugins: PluginManager = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger('gcflask.system')
        self._lock = threading.RLock()
        self._nci: NamedContextInformant = NamedContextInformant("gcsystem")
        injector.register_informant(self._nci)
        self._click_commands: list[tuple[str, str, str]] = []
        self._nci.switch_context('preinit')

    def init(self, *args, **kwargs):
        """Initialize the application for the first time."""
        self._nci.switch_context("init")
        self._nci.destroy('preinit')
        self.events.fire("init.before", self)
        self.plugins.init_plugins()
        self.events.fire("init", self)
        self.events.fire("init.after", self)
        self._subclass_init()
        self._nci.switch_context("active")
        self._nci.destroy('init')

    def _subclass_init(self): ...

    def setup(self):
        """Run all the setup scripts."""
        self.events.fire("setup.before")
        self.events.fire("setup")
        self.events.fire("setup.data")
        self.events.fire("setup.after")

    def cleanup(self):
        """Run all the cleanup scripts."""
        self.events.fire("cleanup.before")
        self.events.fire("cleanup")
        self.events.fire("cleanup.after")

    def before_load(self, load_cb: t.Callable[[System], t.Any] | str):
        """Register a function to call at the start of init()."""
        self.events.on("init.before", load_cb)

    def on_load(self, load_cb: t.Callable[[System], t.Any] | str):
        """Register a function to call during init()."""
        self.events.on("init", load_cb)

    def after_load(self, load_cb: t.Callable[[System], t.Any] | str):
        self.events.on("init.after", load_cb)

    def before_setup(self, cb: t.Callable[[], t.Any] | str):
        self.events.on("setup.before", cb)

    def on_setup(self, setup_cb: t.Callable[[], t.Any] | str):
        """Register a function to call on setup."""
        self.events.on("setup", setup_cb)

    def on_setup_data(self, setup_cb: t.Callable[[], t.Any] | str):
        self.events.on("setup.data", setup_cb)

    def after_setup(self, cb: t.Callable[[], t.Any] | str):
        self.events.on("setup.after", cb)

    def before_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        """Register a function to call at the start of cleanup()."""
        self.events.on("cleanup.before", cleanup_cb)

    def on_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        """Register a function to call during cleanup()."""
        self.events.on("cleanup", cleanup_cb)

    def after_cleanup(self, cleanup_cb: t.Callable[[], t.Any] | str):
        self.events.on("cleanup.after", cleanup_cb)
