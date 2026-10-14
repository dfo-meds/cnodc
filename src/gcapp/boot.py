""" Provides a framework for booting (with configuration loading and logging setup) """
import pathlib
import typing as t
import os
import logging
import sys


def _config_paths(extra_paths: t.Sequence[str] | None = None) -> t.Generator[pathlib.Path, None, None]:
    """ Retrieves configuration directories.

    These include:
    - The current working directory
    - The user's home directory
    - Any directories in the semicolon delimited environment variable GCAPP_CONFIG_DIRECTORIES
    - Any additional paths specified in extra_paths

    :param extra_paths: A sequence of additional paths to search for configuration files.

    :returns: A generator of pathlib.Path objects to search for configuration directories
    """
    paths = [".", "~"]

    custom_config_path = os.environ.get("GCAPP_CONFIG_DIRECTORIES", "./config")
    if custom_config_path:
        paths.extend(custom_config_path.split(";"))

    if extra_paths:
        paths.extend(extra_paths)

    for path in paths:
        if path:
            p = pathlib.Path(path).expanduser().absolute().resolve()
            if p.exists():
                yield p


def _expand_environment_placeholders(variables: dict[str, str | list[str] | tuple[str]]) -> dict[str, str | list[str] | tuple[str]]:
    """ Expands a map of environment variables to configuration keys.

    The keys may contain the text "$1", and the values should as well. Where the key contains $1, the following happens:
    - Any environment variable that starts with the text preceding and ends with the text following "$1" is found
    - Each environment variable found is mapped to the configuration key with $1 replaced by the actual text found in the environment variable.

    Why did I build this? I don't remember.

    :param variables: A dictionary mapping environment variables to configuration keys.
    :returns: A dictionary mapping environment variables to configuration keys with $1 expanded.
    """
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
                    if isinstance(variables[key], str):
                        env_var_map[env_var] = variables[key].replace("$1", dollar1)
                    else:
                        env_var_map[env_var] = [x.replace("$1", dollar1) for x in variables[key]]
        else:
            env_var_map[key] = variables[key]
    return env_var_map


def boot(
        app_name: str,
        app_components: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        individual_log_levels: dict[str, int] | None = None,
        extra_config_paths: list[str] | None = None,
        version_no: str | None = None,
        env_map_files: list[pathlib.Path] | None = None,
        boot_log_level: int = logging.WARNING,
        default_config: dict[str, t.Any] | None = None
):
    """ Boots an application from a configuration file.

    Note that this is a very low-level boot.  It just performs these tasks:

    1. Sets up a temporary logging handler for boot
    2. Loads all configuration files
    3. Initializes the dependency injector system
    4. Initializes the full logging system as configured

    Configuration files are named either `.[APP_NAME].[EXT]` or `.[APP_NAME].[COMPONENT].[EXT]` where
    - `[APP_NAME] is the name of the application
    - `[COMPONENT]` is one of the components being loaded (e.g. `cli` or `web`)
    - `[EXT]` is an extension that zirconium recognizes (e.g. `toml` or `yaml`)

    :param app_name: The name of the app, used in configuration file names.
    :param app_components: A sequence of additional strings (if applicable) to build configuration names.
    :param manual_overrides: A map of fully-qualified class names to other fully-qualified class names to pass to the dependency injection system.
    :param individual_log_levels: A map of log names to log levels - these will be set manually after the logging is setup.
    :param extra_config_paths: Additional paths to search for configuration files.
    :param version_no: The version of the application.
    :param env_map_files: A list of YAML files containing a map of environment variables to configuration keys.
    :param boot_log_level: The log level to use on boot before logging is configured.
    :param default_config: Additional default configuration to send to zirconium
    """

    # Temporary logging settings during boot process
    temp_handle = logging.StreamHandler(sys.stdout)
    temp_handle.setLevel(boot_log_level)
    logging.getLogger().addHandler(temp_handle)
    logging.getLogger().setLevel(boot_log_level)

    # Set up configuration files
    import zirconium as zr
    @zr.configure
    def configure_extra_files(config: zr.ApplicationConfig):
        if default_config is not None:
            config.set_defaults(default_config)
        config_paths = [x for x in _config_paths(extra_config_paths)]
        logging.getLogger("gcapp.boot").info(f"Config Search Paths: {';'.join(str(x) for x in config_paths)}")
        for path in config_paths:
            config.register_default_file(path / f".{app_name}.defaults.toml")
            config.register_file(path / f".{app_name}.toml")
            if app_components:
                for name in app_components:
                    config.register_file(path / f".{app_name}.{name}.toml")
                    config.register_default_file(path / f".{app_name}.{name}.defaults.toml")

        import yaml
        if env_map_files:
            for file in env_map_files:
                if file.exists():
                    with open(file, 'r', encoding='utf-8') as h:
                        d = yaml.safe_load(h)
                        if not isinstance(d, dict):
                            continue
                        config.register_environ_map(_expand_environment_placeholders(d))

    # Initialize system logging and autoinject overrides
    from gcapp.boot_util import init_system_logging, init_overrides
    init_overrides(manual_overrides)
    init_system_logging(version_no)

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
                logging.getLogger().exception("Could not find logger for %s or it is not a logger", log_obj)

def boot_system(
        app_name: str,
        app_components: t.Sequence[str] | None = None,
        manual_overrides: dict[str | type, str | type | t.Callable] | None = None,
        init_hooks: t.Sequence[str | t.Callable] | None = None,
        system_cls: type | str | None = None,
        enable_metrics: bool = False,
        is_multiprocessing: bool = False,
        **kwargs
):
    """ Boots the system.

    A system-level boot is more work than the lower level boot(). This makes an actual system object, has a plugin
    framework, setup functions, etc. It's far more extensive but far more powerful.

    1. All the normal boot() stuff happens first
    2. The System class (or a specified sub-class) is instantiated.
    3. Metrics are enabled or disabled
    4. Initialization hooks are called
    5. The System.init() method is called (note that this is different from __init__()).


    :param app_name: The name of the application (passed to boot())
    :param app_components: The component(s) of the application (passed to boot())
    :param manual_overrides: The manual overrides of the application (passed to boot())
    :param init_hooks: Hooks to call right after the System object is instantiated - these must be callables that take the System object.
    :param system_cls: The system class (if none specified, gcapp.system.System is used)
    :param enable_metrics: Whether to enable metrics
    :param is_multiprocessing: Whether this is a multiprocessing environment
    :param kwargs: Additional keyword arguments for boot().

    :returns: The System object that was instantiated.
    """
    if system_cls is not None:
        if not manual_overrides:
            manual_overrides = {}
        manual_overrides["gcapp.system.System"] = system_cls

    boot(
        app_name=app_name,
        app_components=app_components,
        manual_overrides=manual_overrides,
        **kwargs
    )

    from autoinject import injector, auto
    from gcapp.system import System

    @injector.inject
    def _boot_system(system: System = auto()):
        if enable_metrics:
            system.enable_metrics(is_multiprocessing)
        else:
            system.disable_metrics()
        if init_hooks:
            for hook in init_hooks:
                system.before_load(hook)
        system.init()
        return system

    return _boot_system()