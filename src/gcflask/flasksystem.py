import functools
import logging
import pathlib
import typing as t

import flask
import flask_autoinject
import jinja2
import zrlog
from autoinject import injector
from flask_wtf import CSRFProtect
from jinja2 import pass_context

import gcflask.i18n
from gcflask.util import caps_to_snake
from gcapp.requestinfo import RequestInfo
from gcapp.system import System
from gcflask.i18n_url import BilingualRule
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
        self._flask_globals: dict[str, t.Any] = {
            'csp_nonce': csp_nonce,
            'csp_allow': csp_allow,
            'login_url': '',
            'logout_url': '',
            'refresh_session_url': ''
        }
        self._flask_filters: dict[str, t.Callable[..., str]] = {
            'tr': gcflask.i18n.tr,
            'format_date': gcflask.i18n.format_date,
            'caps_to_snake': caps_to_snake,
        }
        gcflask_root = pathlib.Path(__file__).absolute().resolve().parent
        self._template_directories: list[pathlib.Path] = [
            gcflask_root / 'templates',
        ]
        self._resource_directories: list[tuple[pathlib.Path, int]] = [
            (gcflask_root / 'resources', 0),
        ]

    @property
    def flask_app(self) -> flask.Flask:
        if self._flask_app is None:
            raise RuntimeError('Flask app not initialized yet')
        return self._flask_app

    def register_template_global(self, name: str, value: t.Any):
        self._flask_globals[name] = value

    def register_template_filter(self, name: str, filter_: t.Callable[..., str]):
        self._flask_filters[name] = filter_

    def register_template_directory(self, path: pathlib.Path):
        self._template_directories.append(path)

    def register_resource_directory(self, path: pathlib.Path, weight: int = 0):
        self._resource_directories.append((path, weight))

    def register_menu_item(self, menu_name: str, menu_hierarchy: str, item: NavItem):
        if menu_name not in self._menus:
            self._menus[menu_name] = NavMenu()
        self._menus[menu_name].append_at(menu_hierarchy.split("."), item)

    def set_login_url(self, url: str):
        self._flask_globals['login_url'] = url

    def set_logout_url(self, url: str):
        self._flask_globals['logout_url'] = url

    def set_refresh_session_url(self, url: str):
        self._flask_globals['refresh_session_url'] = url

    def before_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask.before', cb)

    def on_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask', cb)

    def after_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        self.events.on('init.flask.after', cb)

    def init_app(self, app: flask.Flask):
        self._flask_app = app
        self._init_app()

    def _init_app(self):
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
        self._configure_jinja()
        self._configure_health_endpoint()
        self._configure_resource_endpoint()
        self.events.fire("init.flask.after", self.flask_app)

    def _load_config(self):
        self.flask_app.config.update(self.config.get("flask", default={}))
        if 'PERMANENT_SESSION_LIFETIME' not in self.flask_app.config:
            self.flask_app.config['PERMANENT_SESSION_LIFETIME'] = 44640
        self._session_timeout = self.flask_app.config['PERMANENT_SESSION_LIFETIME'].total_seconds() - 1

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

    def _configure_jinja(self):
        for global_key, value in self._flask_globals.items():
            self.flask_app.jinja_env.globals[global_key] = value
        for filter_key, filter_ in self._flask_filters.items():
            self.flask_app.jinja_env.filters[filter_key] = self._wrap_for_jinja(filter_)
        self.flask_app.jinja_loader = jinja2.ChoiceLoader([
            jinja2.FileSystemLoader(x)
            for x in self._template_directories
        ])
        self.flask_app.jinja_env.globals['session_timeout'] = self._session_timeout

    @staticmethod
    def _wrap_for_jinja(func: t.Callable[..., str]) -> t.Callable[..., str]:
        @pass_context
        def _wrapper(ctx, *args, **kwargs):
            return func(*args, **kwargs)
        return _wrapper

    def _configure_proxy_fix(self):
        if self.config.as_bool(('gcflask', 'proxy_fix', 'enabled'), default=False):
            self._log.info('Enabling proxy fix')
            trust_from = self.config.get(("gcflask", "proxy_fix", "trusted_upstreams"), default="")
            if ';' in trust_from:
                trust_from = trust_from.split(';')
            self.flask_app.wsgi_app = TrustedProxyFix(
                self.flask_app.wsgi_app,
                trust_from_ips=trust_from,
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

    @injector.inject
    def _context_processor(self, ld: gcflask.i18n.LanguageDetector = None, tm: gcflask.i18n.TranslationManager = None):
        language = ld.detect_language(tm.supported_languages())
        contextual_vars: dict[str, t.Any] = {
            'language': language,
            'i18n_sort': functools.partial(gcflask.i18n.i18n_sort, language_order=[language, 'und']),
        }
        for menu_name in self._menus:
            contextual_vars[f"menu_{menu_name}"] = self._menus[menu_name]
        if flask.has_request_context():
            contextual_vars["default_title"] = gcflask.i18n.tr(f"{flask.request.endpoint}.title", default=t.cast(str, flask.request.endpoint))
        else:
            contextual_vars["default_title"] = ''
        return contextual_vars

    def _configure_csrf(self):
        self.flask_app.extensions['csrf'] = CSRFProtect(self.flask_app)

    def _configure_resource_endpoint(self):
        self.flask_app.add_url_rule(
            f"/resources/<path:filename>",
            endpoint="resources",
            host=None,
            view_func=self._deliver_resource_file,
        )
        self._resource_directories.sort(key=lambda x: x[1], reverse=True)

    def _configure_health_endpoint(self):
        self.flask_app.add_url_rule(
            f"/-/health",
            endpoint="health",
            host=None,
            view_func=self._health_check
        )
        self._resource_directories.sort(key=lambda x: x[1], reverse=True)

    def _health_check(self):
        return 'healthy', 200

    def _deliver_resource_file(self, filename):
        for path, _ in self._resource_directories:
            test_path = (path / filename).absolute().resolve()
            if test_path.exists():
                return flask.send_from_directory(path, filename)
        return flask.abort(404)

    def register_blueprint(self, module_name, object_name, path_prefix = ""):
        self._flask_blueprints.append((module_name, object_name, path_prefix))

    def _load_blueprints(self):
        universal_prefix = self.config.get(('gcflask', 'path_prefix'), default='').rstrip('/')
        if universal_prefix:
            universal_prefix = f"/{universal_prefix.lstrip('/')}"
        for module_name, object_name, path_prefix in self._flask_blueprints:
            if universal_prefix or path_prefix:
                prefix = '/' + ('/'.join((universal_prefix.strip('/'), path_prefix.strip('/'))))
                self.flask_app.register_blueprint(dynamic_object(f"{module_name}.{object_name}"), url_prefix=prefix)
            else:
                self.flask_app.register_blueprint(dynamic_object(f"{module_name}.{object_name}"))
