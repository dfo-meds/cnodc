"""
Provides a plugin manager class that can be used to manage custom functionality.

This is useful when you want to allow third-party modules to extend your feature.

It's also used to help organize code here, by using "built-in" plugins which extend the core system.
"""

import importlib
import pkgutil
import typing as t

import zirconium as zr
import zrlog
from autoinject import injector, auto

if t.TYPE_CHECKING:
    from gcapp.system import System


@injector.injectable_global
class PluginManager:
    """ Helps manage plugins.

        Plugins are modules or packages that can be discovered from a few sources:

        1. A specific package name - all modules under the package will be loaded,
           and sub-packages will be recursively checked. These are registered via
           discover_from_package().
        2. A specific entry point group - all modules listed in it will be loaded.
           These are registered via discover_from_entry_point().
        3. Modules added directly via add_plugin()
        4. Modules listed in the configuration setting gcapp.plugins, which should be a dictionary
           of module names and either their priority or "skip" to prevent them from being loaded.

        Plugins are loaded by importing the module. If it contains a function named init_plugin(), it
        is called with the System object.
    """

    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self):
        self._plugin_discovery_entry_points: list[str] = []
        self._plugin_discovery_roots: list[str] = []
        self._plugin_manual_list: list[str] = []
        self._plugins: set[str] = set()
        self._log = zrlog.get_logger('gcapp.plugins')

    def discover_from_entry_point(self, entry_point: str):
        """ Registers an entry point group that will contain modules which are plugins. """
        self._plugin_discovery_entry_points.append(entry_point)

    def discover_from_package(self, package_name: str):
        """ Registers a package that contains plugin modules to load. """
        self._plugin_discovery_roots.append(package_name)

    def add_plugin(self, module_name: str):
        """ Registers a specific module as a plugin. """
        self._plugin_manual_list.append(module_name)

    def init_plugins(self, system: System):
        """ Finds and loads all registered plugins. """

        # Get list of configured plugins
        plugin_priorities: dict[str, int | str] = self.config.get(('gcapp', 'plugins'), default={})

        # All plugins we've discovered
        discovered_plugins = set(plugin_priorities.keys())

        # Discover additional plugins via module search
        if self._plugin_discovery_roots:
            for discovery_root in self._plugin_discovery_roots:
                self._log.debug("Discovering plugins from package [%s]", discovery_root)
                discovery_module = importlib.import_module(discovery_root)
                discovered_plugins.update(
                    name for _, name, _ in pkgutil.iter_modules(discovery_module.__path__, discovery_root + ".")
                )

        # Discover even more plugins via entry point search
        if self._plugin_discovery_entry_points:
            from importlib.metadata import entry_points
            for entry_point in self._plugin_discovery_entry_points:
                self._log.debug("Discovering plugins from entry point [%s]", entry_point)
                discovered_plugins.update(
                    ep.name for ep in entry_points(group=entry_point)
                )

        # Manually listed ones
        if self._plugin_manual_list:
            discovered_plugins.update(self._plugin_manual_list)

        # Order them properly
        plugin_list = []
        for plugin_name in discovered_plugins:
            # priorities are from configuration, otherwise they're 0
            priority = plugin_priorities.get(plugin_name, 0)
            if priority == 'skip':
                continue
            plugin_list.append((plugin_name, priority))
        plugin_list.sort(key=lambda x: x[1])

        # Actually load them
        for plugin_name, _ in plugin_list:
            self._load_plugin(plugin_name, system)

    def _load_plugin(self, name: str, system: System):
        """Load a plugin from its fully qualified name."""
        if name not in self._plugins:

            # Import it
            try:
                self._log.debug("Loading plugin [%s]", name)
                mod = importlib.import_module(name)

                # Call init_plugin() if it exists
                if hasattr(mod, "init_plugin"):
                    getattr(mod, "init_plugin")(system)

                # Store it in the list
                self._plugins.add(name)
                self._log.info("Plugin loaded [%s]", name)
            except Exception as e:
                e.add_note(f"Error while loading plugin [{name}]")
                raise

    @property
    def loaded_plugins(self) -> set[str]:
        """ Retrieves all loaded plugins. """
        # returns a copy so it can't be modified
        return {x for x in self._plugins}
