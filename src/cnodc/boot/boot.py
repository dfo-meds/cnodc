import flask
import zirconium as zr
import pathlib
import os
import logging
import zrlog
from autoinject import injector
import prometheus_client as pc
import prometheus_flask_exporter as pfe
import prometheus_client.multiprocess as pcmp
from cnodc.util import CNODCError

__VERSION__ = "0.0.1"

from cnodc.util.flask import RequestInfo


def _config_paths():
    yield pathlib.Path(".").absolute()
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
    pybufrkit_logger.setLevel(logging.INFO)

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


@injector.inject
def init_flask(app: flask.Flask, config: zr.ApplicationConfig):
    info = RequestInfo()
    zrlog.set_default_extra("sys_username", info.sys_username())
    zrlog.set_default_extra("sys_emulated", info.sys_emulated_username())
    zrlog.set_default_extra("sys_logon", info.sys_logon_time())
    zrlog.set_default_extra("sys_remote", info.sys_remote_addr())
    zrlog.set_default_extra("instance_name", config.as_str(("flask", "INSTANCE_NAME"), default="UNKNOWN"))
    log = zrlog.get_logger("cnodc.boot")

    # Load config
    if "flask" in config:
        app.config.update(config["flask"] or {})

    # Check secret key is defined
    if not app.config.get("SECRET_KEY"):
        raise CNODCError("Missing secret_key", "BOOT", 1000)
    elif isinstance(app.config.get("SECRET_KEY"), str):
        app.config["SECRET_KEY"] = app.config.get("SECRET_KEY")._encode("utf-8")

    # Manage autoinject settings
    import flask_autoinject
    flask_autoinject.init_app(app)

    # Register prometheus metrics
    app.extensions["cnodc"] = {
        "registry": pc.CollectorRegistry()
    }
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR", default=None):
        app.extensions["cnodc"]["collector"] = pcmp.MultiProcessCollector(app.extensions["cnodc"]["registry"])
    app.extensions["cnodc"]["prometheus"] = pfe.PrometheusMetrics(app, registry=app.extensions["cnodc"]["registry"])

    # Configure proxy settings
    if config.as_bool(("cnodc", "proxy_fix", "enabled"), default=False):
        from cnodc.util.flask import TrustedProxyFix
        log.info("Proxy fix: enabled")
        app.wsgi_app = TrustedProxyFix(
            app.wsgi_app,
            trust_from_ips=config.get(("cnodc", "proxy_fix", "trusted_upstreams"), default="*"),
            x_for=config.get(("cnodc", "proxy_fix", "x_for"), default=1),
            x_proto=config.get(("cnodc", "proxy_fix", "x_proto"), default=1),
            x_host=config.get(("cnodc", "proxy_fix", "x_host"), default=1),
            x_port=config.get(("cnodc", "proxy_fix", "x_port"), default=1),
            x_prefix=config.get(("cnodc", "proxy_fix", "x_prefix"), default=1)
        )
    else:
        log.info("Proxy fix: disabled")

    # Add logging output variables at start of the request
    @app.before_request
    def add_logging_extras():
        req_info = RequestInfo()
        zrlog.set_extras({
            "username": req_info.username(),
            "remote_ip": req_info.remote_ip(),
            "proxy_ip": req_info.proxy_ip(),
            "correlation_id": req_info.correlation_id(),
            "client_id": req_info.client_id(),
            "request_url": req_info.request_url(),
            "user_agent": req_info.user_agent(),
            "referrer": req_info.referrer(),
            "request_method": req_info.request_method()
        })

    # Load routes
    from cnodc.api.routes import cnodc
    app.register_blueprint(cnodc)

    from cnodc.api.admin_routes import admin
    app.register_blueprint(admin)




