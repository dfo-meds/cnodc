import pathlib
import shutil
import os
import logging

__VERSION__ = '0.1.0'
ROOT_DIR = pathlib.Path(__file__).absolute().resolve().parent.parent.parent


def _build_default_multiproc_dir():
    # Ensure we have a multiprocessing directory
    prom_dir = os.environ.get('PROMETHEUS_MULTIPROC_DIR', '')
    if not prom_dir:
        prom_dir = str(ROOT_DIR / ".temp_prometheus")
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = prom_dir

    prom_dir = pathlib.Path(prom_dir)
    if prom_dir.exists():
        shutil.rmtree(prom_dir)
    prom_dir.mkdir()


def _config_paths():
    yield pathlib.Path("").absolute()
    yield pathlib.Path("~").expanduser().absolute()
    custom_config_path = os.environ.get("CNODC_CONFIG_SEARCH_PATHS", "./config")
    if custom_config_path:
        paths = custom_config_path.split(";")
        for path in paths:
            if path:
                p = pathlib.Path(path).absolute()
                if p.exists():
                    yield p


def init_cnodc(app_type: str, with_mp_prometheus_default: bool = False):

    # Build us a multiprocessing metrics directory to share (in production this should be configured in os.environ
    if with_mp_prometheus_default:
        _build_default_multiproc_dir()

    # Avoid spam from pybufrkit when DEBUG mode is enabled
    from pybufrkit.coder import log as pybufrkit_logger
    pybufrkit_logger.setLevel(logging.WARNING)

    # Configure our configuration paths
    import zirconium as zr
    @zr.configure
    def set_config(app_config: zr.ApplicationConfig):
        config_paths = [x for x in _config_paths()]
        logging.getLogger("cnodc.boot").info(f"Config Search Paths: {';'.join(str(x) for x in config_paths)}")
        for path in config_paths:
            app_config.register_default_file(path / ".cnodc.defaults.toml")
            app_config.register_default_file(path / f".cnodc.{app_type}.defaults.toml")
            app_config.register_file(path / ".cnodc.toml")
            app_config.register_file(path / f".cnodc.{app_type}.toml")
        if app_type == 'tests':
            app_config.register_file(pathlib.Path("./tests/.cnodc.tests.toml").absolute())
            app_config.register_file(pathlib.Path("./tests/.cnodc.private.toml").absolute())

    # Configure our logging
    import zrlog
    zrlog.set_default_extra("process_uuid", "")
    zrlog.set_default_extra("process_name", "")
    zrlog.set_default_extra("sys_username", "")
    zrlog.set_default_extra("sys_emulated", "")
    zrlog.set_default_extra("sys_logon", "")
    zrlog.set_default_extra("sys_remote", "")
    zrlog.set_default_extra("username", "")
    zrlog.set_default_extra("remote_ip", "")
    zrlog.set_default_extra("proxy_ip", "")
    zrlog.set_default_extra("correlation_id", "")
    zrlog.set_default_extra("client_id", "")
    zrlog.set_default_extra("request_url", "")
    zrlog.set_default_extra("user_agent", "")
    zrlog.set_default_extra("referrer", "")
    zrlog.set_default_extra("request_method", "")
    zrlog.set_default_extra("version", __VERSION__)
    zrlog.init_logging()

    # Configure our email system
    from autoinject import injector
    from medsutil.email import DelayedEmailController
    from pipeman.delayed_emails import DelayedEmailsQueuer
    injector.override(DelayedEmailController, DelayedEmailsQueuer)

    from nodb.interface import NODB
    from nodb.controller import NODBPostgresController
    injector.override(NODB, NODBPostgresController)


def init_for_tests(skip_long_tests: bool = True,
                   disable_metrics: bool = True,
                   fast_password_hashing: bool = True,
                   with_mp_prometheus_default: bool = True):

    # Setup config and logging
    init_cnodc('tests', with_mp_prometheus_default if not disable_metrics else False)
    logging.disable(logging.NOTSET)

    if disable_metrics:
        # Prevent metrics from being loaded
        from autoinject import injector
        from medsutil.metrics import PromMetrics
        @injector.inject
        def _disable_metrics(pm: PromMetrics = None):
            pm.disable_metrics = True
        _disable_metrics()

    if fast_password_hashing:
        # speed up password hashing for tests only!
        import medsutil.secure as s
        s.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        s.MINIMUM_ITERATIONS = 2

    # skip long tests unless requested to run (there's a lot of them
    if skip_long_tests:
        import tests.helpers.base_test_case as btc
        btc.SKIP_FLAG.set()
