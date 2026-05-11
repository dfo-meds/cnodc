import pathlib
import typing as t
import os
import logging


def _config_paths(extra_paths: t.Sequence[str | pathlib.Path] | None = None) -> t.Generator[pathlib.Path, None, None]:
    yield pathlib.Path(".").absolute().resolve()
    yield pathlib.Path("~").expanduser().absolute().resolve()
    custom_config_path = os.environ.get("GCAPP_CONFIG_DIRECTORIES", "./config")
    if custom_config_path:
        paths = custom_config_path.split(";")
        for path in paths:
            if path:
                p = pathlib.Path(path).absolute().resolve()
                if p.exists():
                    yield p
    if extra_paths:
        for path in extra_paths:
            if isinstance(path, str):
                yield pathlib.Path(path).absolute().resolve()
            else:
                yield path.absolute().resolve()


def _env_variables(variables: dict[str, str]) -> dict[str, str]:
    env_var_map = {}
    existing_env_vars = list(os.environ.keys())
    for key in variables:
        if '$1' in key:
            pos = key.find('$1')
            prefix = key[:pos]
            suffix = key[pos+2:]
            for env_var in existing_env_vars:
                if env_var.startswith(prefix) and env_var.endswith(suffix):
                    dollar1 = env_var[len(prefix):-len(suffix)]
                    env_var_map[env_var] = variables[key].replace("$1", dollar1)
        else:
            env_var_map[key] = variables[key]
    return env_var_map


def boot(
        app_name: str,
        app_components: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        individual_log_levels: dict[str, int] | None = None,
        extra_config_paths: list[str | pathlib.Path] | None = None,
        version_no: str | None = None,
        env_map_files: list[pathlib.Path] | None = None
):

    delayed_log_messages: list[tuple[str, int]] = []
    # Set up configuration files
    import zirconium as zr
    @zr.configure
    def configure_extra_files(config: zr.ApplicationConfig):
        config_paths = [x for x in _config_paths(extra_config_paths)]
        logging.getLogger("gcapp.boot").info(f"Config Search Paths: {';'.join(str(x) for x in config_paths)}")
        for path in config_paths:
            config.register_default_file(path / f".{app_name}.defaults.toml")
            config.register_file(path / f".{app_name}.toml")
            if app_components:
                for name in app_components:
                    config.register_default_file(path / f".{app_name}.{name}.defaults.toml")
                    config.register_file(path / f".{app_name}.{name}.toml")

        import yaml
        if env_map_files:
            for file in env_map_files:
                if file.exists():
                    with open(file, 'r', encoding='utf-8') as h:
                        d = yaml.safe_load(h)
                        if not isinstance(d, dict):
                            continue
                        config.register_environ_map(_env_variables(d))

    # Initialize system logging and autoinject overrides
    from gcapp.boot_util import init_system_logging, init_overrides
    init_overrides(manual_overrides)
    init_system_logging(version_no)

    # We delay the messages to here to ensure everything is configured correctly.
    boot_logger = logging.getLogger('boot')
    for log_msg, log_lvl in delayed_log_messages:
        boot_logger.log(log_lvl, log_msg)

    # Configure custom logging levels
    import importlib
    if individual_log_levels:
        for log_obj, log_level in individual_log_levels.items():
            module_name, obj_name = log_obj.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            try:
                logger = getattr(mod, obj_name)
                logger.setLevel(log_level)
            except AttributeError:
                boot_logger.exception("Could not find logger for %s or it is not a logger", log_obj)

def boot_system(
        app_name: str,
        other_names: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        init_hooks: t.Sequence[str | t.Callable] | None = None,
        system_cls: type = None,
):

    boot(app_name, other_names, manual_overrides)

    from autoinject import injector
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