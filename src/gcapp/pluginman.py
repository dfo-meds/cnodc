import importlib
import pkgutil

import zirconium as zr
import zrlog
from autoinject import injector


@injector.injectable_global
class PluginManager:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._plugin_discovery_entry_point: str = ''
        self._plugin_discovery_roots = []
        self._plugin_manual_list = []
        self._plugins: set[str] = set()
        self._log = zrlog.get_logger('gcapp.plugins')

    def discover_from_module(self, module_name: str):
        self._plugin_discovery_roots.append(module_name)

    def add_plugin(self, module_name: str):
        self._plugin_manual_list.append(module_name)

    def init_plugins(self, system):
        """Find and initialize all of the plugins."""
        # Get list of configured plugins
        plugin_priorities: dict[str, int | str] = self.config.get(('gcapp', 'plugins'), default={})
        discovered_plugins = set(plugin_priorities.keys())

        # Discover additional plugins via module search
        if self._plugin_discovery_roots:
            for discovery_root in self._plugin_discovery_roots:
                discovery_module = importlib.import_module(discovery_root)
                discovered_plugins.update(
                    name for _, name, _ in pkgutil.iter_modules(discovery_module.__path__, discovery_root + ".")
                )

        # Discover even more plugins via entrypoint
        if self._plugin_discovery_entry_point:
            from importlib.metadata import entry_points
            discovered_plugins.update(
                ep.name for ep in entry_points(group=self._plugin_discovery_entry_point)
            )

        # Manually listed ones
        if self._plugin_manual_list:
            discovered_plugins.update(self._plugin_manual_list)

        # Order them properly
        plugin_list = []
        for plugin_name in discovered_plugins:
            priority = plugin_priorities.get(plugin_name, 0)
            if priority == 'skip':
                continue
            plugin_list.append((plugin_name, priority))
        plugin_list.sort(key=lambda x: x[1])

        # Actually load them
        for plugin_name, _ in plugin_list:
            self._load_plugin(plugin_name, system)

    def _load_plugin(self, name: str, system):
        """Load a plugin from its fully qualified name."""
        if name not in self._plugins:
            self._log.debug(f"Loading plugin {name}...")
            # Import it
            mod = importlib.import_module(name)
            # Call init_plugin() if it exists
            if hasattr(mod, "init_plugin"):
                getattr(mod, "init_plugin")(system)
            # Store it in the list
            self._plugins.add(name)
            self._log.info(f"Plugin {name} loaded")
