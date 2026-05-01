import zirconium as zr
import pathlib
from autoinject import injector
import typing as t
import os
import logging

def _config_paths():
    yield pathlib.Path(".").absolute().resolve()
    yield pathlib.Path("~").expanduser().absolute().resolve()
    custom_config_path = os.environ.get("CONFIG_DIRECTORIES", "./config")
    if custom_config_path:
        paths = custom_config_path.split(";")
        for path in paths:
            if path:
                p = pathlib.Path(path).absolute().resolve()
                if p.exists():
                    yield p


def boot(
        app_name: str,
        other_names: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None
):

    @zr.configure
    def configure_extra_files(config: zr.ApplicationConfig):
        config_paths = [x for x in _config_paths()]
        logging.getLogger("gcapp.boot").info(f"Config Search Paths: {';'.join(str(x) for x in config_paths)}")
        for path in config_paths:
            config.register_default_file(path / f".{app_name}.defaults.toml")
            config.register_file(path / f".{app_name}.toml")
            if other_names:
                for name in other_names:
                    config.register_default_file(path / f".{app_name}.{name}.defaults.toml")
                    config.register_file(path / f".{app_name}.{name}.toml")

    from gcapp.util import init_system_logging, init_overrides
    init_system_logging()
    init_overrides(manual_overrides)


def boot_system(
        app_name: str,
        other_names: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        init_hooks: t.Sequence[str | t.Callable] | None = None,
        system_cls: type = None,
):

    boot(app_name, other_names, manual_overrides)


    from gcapp.system import System
    if system_cls is not None:
        injector.override(System, system_cls)

    @injector.inject
    def _boot_system(system: System = None):
        if init_hooks:
            for hook in init_hooks:
                system.before_load(hook)
        system.init()
        return system

    return _boot_system()