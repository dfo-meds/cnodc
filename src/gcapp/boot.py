import pathlib
import typing as t
import os
import logging
import shutil


ROOT_DIR = pathlib.Path(__file__).absolute().resolve()
while ROOT_DIR.name in ('src', 'gcapp', 'boot.py'):
    ROOT_DIR = ROOT_DIR.parent


def _fix_multiprocessing_directory(create_local_default: bool = False):
    # Ensure we have a multiprocessing directory
    prom_dir = os.environ.get('PROMETHEUS_MULTIPROC_DIR', '')
    if not prom_dir:
        if not create_local_default:
            return False
        prom_dir = str(ROOT_DIR / ".temp_prometheus")
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = prom_dir
    prom_dir = pathlib.Path(prom_dir)
    if prom_dir.exists():
        shutil.rmtree(prom_dir)
    prom_dir.mkdir()
    return True


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


def boot(
        app_name: str,
        app_components: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        create_local_prom_mp_dir: bool = False,
        is_multiprocessing: bool = False,
        individual_log_levels: dict[str, int] | None = None,
        extra_config_paths: list[str | pathlib.Path] | None = None,
        version_no: str | None = None
):

    delayed_log_messages: list[tuple[str, int]] = []
    # Ensure Prometheus metrics directory is correctly set up
    if is_multiprocessing:
        if not _fix_multiprocessing_directory(create_local_prom_mp_dir):
            delayed_log_messages.append(('Prometheus directory not configured for a multiprocessing system; this may cause errors in your metrics!!', logging.WARNING))

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

    # Initialize system logging and autoinject overrides
    from gcapp.boot_util import init_system_logging, init_overrides
    init_overrides(manual_overrides)
    init_system_logging(version_no)

    # Configure custom logging levels
    if individual_log_levels:
        for log_name, log_level in individual_log_levels.items():
            logging.getLogger(log_name).setLevel(log_level)

    # We delay the messages to here to ensure everything is configured correctly.
    boot_logger = logging.getLogger('boot')
    for log_msg, log_lvl in delayed_log_messages:
        boot_logger.log(log_lvl, log_msg)


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