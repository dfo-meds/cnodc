import pathlib

from autoinject import injector, auto

from threading import RLock
import yaml
import typing as t
import time
import zrlog

from medsutil.awaretime import AwareDateTime


@injector.injectable
class SetupChecker:

    def last_setup_run(self) -> AwareDateTime | None:
        return None


@injector.injectable
class GlobalRegistry:

    REFRESH_FREQUENCY = 15  # seconds

    setup_checker: SetupChecker = auto()

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("dmd.global_registry")
        self._registry: list[BaseRegistry] = []
        self._last_setup_run = None
        self._last_setup_check = None

    def register(self, obj: BaseRegistry):
        self._registry.append(obj)

    def unregister(self, obj: BaseRegistry):
        if obj in self._registry:
            self._registry.remove(obj)

    def check_all(self):
        # Don't check everytime to prevent a lot of overhead
        if self._last_setup_check and (time.monotonic() - self._last_setup_check) < self.REFRESH_FREQUENCY:
            return
        self._last_setup_check = time.monotonic()
        self._check_all()

    def _check_all(self):
        self._log.debug("Checking for registry updates")
        # Check if setup has actually run recently
        setup_last_run = self.setup_checker.last_setup_run()
        if self._last_setup_run == setup_last_run:
            return
        self._log.info(f"Updating [{len(self._registry)}] registry files")
        # Do the reload
        for obj in self._registry:
            obj.reload_types()
        self._last_setup_run = setup_last_run


@injector.injectable
class RegistryStorage:

    def load_registry_map(self, registry_name: str) -> t.Iterable[tuple[str, dict]]:
        raise NotImplementedError

    def delete_registry_map(self, registry_name: str):
        raise NotImplementedError

    def upsert_registry_entry(self,
                              registry_name: str,
                              entry_name: str,
                              entry_value: dict):
        raise NotImplementedError

    def bulk_update_registry_map(self, registry_name: str, entry_map: dict[str, dict]):
        raise NotImplementedError


class BaseRegistry:

    gor: GlobalRegistry = auto()
    storage: RegistryStorage = auto()

    @injector.construct
    def __init__(self, registry_name: str, ensure_fields=None):
        self._registry_map = {}
        self._lock = RLock()
        self._registry_name = registry_name
        self._ensure_fields = ensure_fields
        self.gor.register(self)
        self._log = zrlog.get_logger(f"dmd.{registry_name}_registry")

    def __cleanup__(self):
        self.gor.unregister(self)

    def __del__(self):
        self.gor.unregister(self)

    def __iter__(self):
        return iter(self._registry_map)

    def __contains__(self, key):
        return key in self._registry_map

    def __getitem__(self, key):
        return self._registry_map[key]

    def keys(self):
        return self._registry_map.keys()

    def sorted_keys(self):
        keys = list(self._registry_map.keys())
        keys.sort()
        return keys

    def reload_types(self):
        with self._lock:
            found = []
            for obj_name, config in self.storage.load_registry_map(self._registry_name):
                found.append(obj_name)
                self._registry_map[obj_name] = config
            for obj_name in list(self._registry_map.keys()):
                if obj_name not in found:
                    del self._registry_map[obj_name]

    def save_changes(self):
        self.storage.bulk_update_registry_map(self._registry_name, self._registry_map)

    def register(self, obj_name, _update_db: bool = True, **config):
        if self._ensure_fields:
            for f in self._ensure_fields:
                if f not in config:
                    config[f] = None
        if _update_db:
            self.storage.upsert_registry_entry(self._registry_name, obj_name, config)
        if obj_name in self._registry_map:
            self._deep_update(self._registry_map[obj_name], config)
        else:
            self._registry_map[obj_name] = config

    @staticmethod
    def _deep_update(d1, d2):
        for key in d2:
            if key in d1 and isinstance(d1[key], dict) and isinstance(d2[key], dict):
                BaseRegistry._deep_update(d1[key], d2[key])
            else:
                d1[key] = d2[key]

    def register_from_dict(self, cfg_dict: dict[str, dict]):
        cfg_dict = cfg_dict or {}
        self._log.debug(f"Importing {len(cfg_dict)} registry entries for [{self._registry_name}]")
        for key in cfg_dict:
            self.register(key, **cfg_dict[key], _update_db=False)
        self.save_changes()

    def register_from_yaml(self, file_path: pathlib.Path):
        self._log.notice(f"Importing registry entries from [{file_path}] for {self._registry_name}")
        with open(file_path, "r", encoding="utf-8") as h:
            self.register_from_dict(yaml.safe_load(h))

    def remove_all(self):
        self._log.notice(f"Removing all object definitions of type [{self._registry_name}]")
        self.storage.delete_registry_map(self._registry_name)
