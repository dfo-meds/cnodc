import zirconium as zr
import pathlib
import os
import logging
import zrlog


__VERSION__ = '0.1.0'


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


def init_cnodc(app_type: str):
    # Avoid spam from pybufrkit when DEBUG mode is enabled
    from pybufrkit.coder import log as pybufrkit_logger
    pybufrkit_logger.setLevel(logging.WARNING)

    @zr.configure
    def set_config(app_config: zr.ApplicationConfig):
        config_paths = [x for x in _config_paths()]
        logging.getLogger("cnodc.boot").info(f"Config Search Paths: {';'.join(str(x) for x in config_paths)}")
        for path in config_paths:
            app_config.register_default_file(path / ".cnodc.defaults.toml")
            app_config.register_default_file(path / f".cnodc.{app_type}.defaults.toml")
            app_config.register_file(path / ".cnodc.toml")
            app_config.register_file(path / f".cnodc.{app_type}.toml")


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


