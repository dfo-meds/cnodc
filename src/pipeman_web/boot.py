
def build_cnodc_webapp(name: str, with_default_prometheus_dir: bool = False):
    import flask
    app = flask.Flask(name)
    from pipeman.boot import init_cnodc
    init_cnodc('web', with_default_prometheus_dir)
    _init_flask()(app)
    return app

def _init_flask():
    import zirconium as zr
    import zrlog
    import flask
    import flask_autoinject
    from autoinject import injector

    from medsutil.metrics import PromMetrics
    from pipeman.exceptions import CNODCError
    from medsutil.flask.requestinfo import RequestInfo

    @injector.inject
    def init_flask(app: flask.Flask, config: zr.ApplicationConfig=None, prom_metrics: PromMetrics = None):
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
            app.config["SECRET_KEY"] = app.config.get("SECRET_KEY").encode("utf-8")

        # Manage autoinject settings
        flask_autoinject.init_app(app)

        # Register prometheus metrics
        prom_metrics.init_app(app)

        # Configure proxy settings
        if config.as_bool(("cnodc", "proxy_fix", "enabled"), default=False):
            from medsutil.flask.trustedproxy import TrustedProxyFix
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
        from pipeman_web.routes import cnodc
        app.register_blueprint(cnodc)

        from pipeman_web.admin_routes import admin
        app.register_blueprint(admin)

    return init_flask
