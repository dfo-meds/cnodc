import logging
import typing as t

import flask
import flask_autoinject
import markupsafe
import zrlog
from autoinject import injector
from flask_wtf import CSRFProtect

from gcapp.requestinfo import RequestInfo
from gcapp.system import System
from gcflask.bilingual_rule import BilingualRule
from gcflask.csp import CSPRegistry, csp_nonce, csp_allow
from gcflask.nav import NavMenu, NavItem
from gcflask.trustedproxy import TrustedProxyFix
from medsutil.dynamic import dynamic_object
from medsutil.exceptions import CodedError

class GCFlaskError(CodedError): CODE_SPACE='GCFLASK'


class FlaskSystemMixin(System):

    def __init__(self):
        super().__init__()
        self._flask_app: t.Optional[flask.Flask] = None
        self._flask_blueprints: list[tuple[str, str, str]] = []
        self._menus: dict[str, NavMenu] = {}

    @property
    def flask_app(self) -> flask.Flask:
        if self._flask_app is None:
            raise RuntimeError('Flask app not initialized yet')
        return self._flask_app

    def register_menu_item(self, menu_name: str, menu_hierarchy: str, item: NavItem):
        if menu_name not in self._menus:
            self._menus[menu_name] = NavMenu()
        self._menus[menu_name].append_at(menu_hierarchy.split("."), item)

    def before_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask.before', cb)

    def on_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask', cb)

    def after_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask.after', cb)

    def init(self, *args, app: flask.Flask, **kwargs):
        self._flask_app = app
        super().init(*args, app=app, **kwargs)

    def _subclass_init(self):
        super()._subclass_init()
        self._load_config()
        self._verify_secret_key()
        self.events.fire("init.flask.before", self.flask_app)
        self._configure_autoinject()
        self._configure_bilingual_routes()
        self._configure_proxy_fix()
        self._register_hooks()
        self._configure_csrf()
        self.events.fire("init.flask", self.flask_app)
        self._load_blueprints()
        self.events.fire("init.flask.after", self.flask_app)

    def _load_config(self):
        self.flask_app.config.update(self.config.get("flask", default={}))
        if 'PERMANENT_SESSION_LIFETIME' not in self.flask_app.config:
            self.flask_app.config['PERMANENT_SESSION_LIFETIME'] = 44640
        self._session_timeout = int(self.flask_app.config['PERMANENT_SESSION_LIFETIME']) - 1

    def _verify_secret_key(self):
        secret_key = self.flask_app.config.get('SECRET_KEY', None)
        if not secret_key:
            raise GCFlaskError('Secret key is missing or empty', 1000)
        if isinstance(secret_key, str):
            self.flask_app.config['SECRET_KEY'] = secret_key.encode('utf-8')

    def _configure_autoinject(self):
        flask_autoinject.init_app(self.flask_app)

    def _configure_bilingual_routes(self):
        self.flask_app.url_rule_class = BilingualRule

    def _configure_proxy_fix(self):
        if self.config.as_bool(('gcflask', 'proxy_fix', 'enabled'), default=False):
            self._log.info('Enabling proxy fix')
            self.flask_app.wsgi_app = TrustedProxyFix(
                self.flask_app.wsgi_app,
                trust_from_ips=self.config.get(("gcflask", "proxy_fix", "trusted_upstreams"), default=""),
                x_for=self.config.as_int(("gcflask", "proxy_fix", "x_for"), default=1),
                x_proto=self.config.as_int(("gcflask", "proxy_fix", "x_proto"), default=1),
                x_host=self.config.as_int(("gcflask", "proxy_fix", "x_host"), default=1),
                x_port=self.config.as_int(("gcflask", "proxy_fix", "x_port"), default=1),
                x_prefix=self.config.as_int(("gcflask", "proxy_fix", "x_prefix"), default=1),
            )
        else:
            self._log.info('Disabling proxy fix')

    def _register_hooks(self):
        self.flask_app.before_request(self._before_request)
        self.flask_app.after_request(self._after_request)
        self.flask_app.context_processor(self._context_processor)

    @injector.inject
    def _before_request(self, rinfo: RequestInfo = None, cspr: CSPRegistry = None):
        # Note that this is a static page request
        if flask.request.endpoint == 'static':
            cspr.set_static()
        rinfo.set_logging_extras_web()

    @injector.inject
    def _after_request(self, response: flask.Response, cspr: CSPRegistry = None) -> flask.Response:
        zrlog.get_logger('gcflask.access').log(
            logging.DEBUG if flask.request.endpoint == 'static' else logging.INFO,
            "%s \"%s\" %s",
            flask.request.method,
            flask.request.url,
            response.status_code
        )
        cspr.add_headers(response)
        return response

    def _context_processor(self):
        processors: dict[str, t.Callable[..., str | markupsafe.Markup]] = {
            'csp_nonce': csp_nonce,
            'csp_allow': csp_allow,
        }
        for menu_name in self._menus:
            processors[f"menu_{menu_name}"] = self._menus[menu_name]
        return processors

    def _configure_csrf(self):
        self.flask_app.extensions['csrf'] = CSRFProtect(self.flask_app)

    def _load_blueprints(self):
        universal_prefix = self.config.get(('gcflask', 'path_prefix'), default='').rstrip('/')
        if universal_prefix:
            universal_prefix = f"/{universal_prefix.lstrip('/')}"
        for module_name, object_name, path_prefix in self._flask_blueprints:
            self.flask_app.register_blueprint(dynamic_object(f"{module_name}.{object_name}"), url_prefix=f"{universal_prefix}/{path_prefix}")
