""" Implementation of :py:class:`gcapp.system.System` that supports Flask. """

import functools
import logging
import pathlib
import typing as t

import flask
import flask_autoinject
import jinja2
import zrlog
from autoinject import injector, auto
from flask_wtf import CSRFProtect
from jinja2 import pass_context

from medsutil.dynamic import dynamic_object
from medsutil.exceptions import CodedError
import gcapp.i18n as i18n
from gcapp.requestinfo import RequestInfo
from gcapp.system import System, MetricsType
from gcflask.user import PermissionType, current_user
from gcflask.util import caps_to_snake
from gcflask.i18n_url import BilingualRule
from gcflask.csp import CSPRegistry, csp_nonce, csp_allow
from gcflask.nav import NavMenu, NavItem
from gcflask.trustedproxy import TrustedProxyFix


class GCFlaskError(CodedError): CODE_SPACE='GCFLASK'


class APIOperation(t.TypedDict):
    """ Represents an API operation (pre-resolution). """

    endpoint: str
    """ The Flask endpoint """

    url_kwargs: dict[str, t.Any]
    """ Keyword arguments to be passed to :py:func:`flask.url_for`. """

    access: PermissionType
    """ Who has access to the item. """

    request_kwargs: dict[str, t.Any]
    """ Arguments to be passed in the JSON body. """

    metadata: dict[str, t.Any]
    """ Additional metadata about the API operation. """


class APIResolvedOperation(t.TypedDict):
    """ Represents a resolved API operation. """

    endpoint: str
    """ The full external-facing URL for the API. """

    kwargs: dict[str, t.Any]
    """ JSON keyword arguments to submit. """

    metadata: dict[str, t.Any]
    """ Additional metadata about the API operation. """


class FlaskSystemMixin(System):
    """ Mix-in with all the support for a Flask server. """

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
            'tr': i18n.tr,
            'format_date': i18n.format_date,
            'caps_to_snake': caps_to_snake,
        }
        gcflask_root = pathlib.Path(__file__).absolute().resolve().parent
        self._template_directories: list[pathlib.Path] = [
            gcflask_root / 'templates',
        ]
        self._resource_directories: list[tuple[pathlib.Path, int]] = [
            (gcflask_root / 'resources', 0),
        ]
        self._api_operations: dict[str, APIOperation] = {}
        self._dynamic_api_operations: dict[str, t.Callable[[], dict[str, APIOperation]]] = {}

    @property
    def flask_app(self) -> flask.Flask:
        """ Retrieves the flask application. """
        if self._flask_app is None:
            raise RuntimeError('Flask app not initialized yet')
        return self._flask_app

    def get_api_operations(self) -> dict[str, APIResolvedOperation]:
        """ Retrieves all registered API operations, after resolving them. """
        operations: dict[str, APIResolvedOperation] = {}
        for key, operation in self._api_operations.items():
            resolved = self._resolve_operation(operation)
            if resolved is not None:
                operations[key] = resolved
        for key, builder in self._dynamic_api_operations.items():
            for subkey, operation in builder().items():
                resolved = self._resolve_operation(operation)
                if resolved is not None:
                    operations[f"{key}.{subkey}"] = resolved
        return operations

    def _resolve_operation(self, api_operation: APIOperation) -> APIResolvedOperation | None:
        """ Resolves an operation.

            This ensures the user has access to it and then combines the endpoint and url_kwargs into
            a full URL.

            :returns: The resolved operation information or None if the user doesn't have access.
        """
        if flask.has_request_context():
            c_user = current_user()
            if not c_user.require_all(api_operation["access"]):
                return None
        resolved: APIResolvedOperation = {
            "endpoint": flask.url_for(api_operation["endpoint"], _external=True, **api_operation["url_kwargs"]),
            "kwargs": api_operation["request_kwargs"],
            "metadata": api_operation["metadata"],
        }
        return resolved

    def register_dynamic_api_operation_builder(self,
                                               build_identifier: str,
                                               builder: t.Callable[[], dict[str, APIOperation]]):
        """ Registers a function that defines APIOperations. """
        self._dynamic_api_operations[build_identifier] = builder

    def register_api_operation(self,
                               operation_identifier: str,
                               flask_endpoint: str,
                               required_permissions: PermissionType = None,
                               request_kwargs: dict[str, t.Any] | None = None,
                               url_kwargs: dict[str, t.Any] | None = None,
                               metadata: dict[str, t.Any] | None = None):
        """ Registers a static API operation. """
        self._api_operations[operation_identifier] = {
            'endpoint': flask_endpoint,
            'access': required_permissions,
            'request_kwargs': request_kwargs or {},
            'url_kwargs': url_kwargs or {},
            "metadata": metadata or {}
        }

    def register_template_global(self, name: str, value: t.Any):
        """ Registers a global value for Flask templates. """
        self._flask_globals[name] = value

    def register_template_filter(self, name: str, filter_: t.Callable[..., str]):
        """ Registers a filter for Flask templates. """
        self._flask_filters[name] = filter_

    def register_template_directory(self, path: pathlib.Path):
        """ Registers a template directory for Flask.  """
        self._template_directories.append(path)

    def register_resource_directory(self, path: pathlib.Path, weight: int = 0):
        """ Registers a resource directory for Flask (containing static files). """
        self._resource_directories.append((path, weight))

    def register_menu_item(self, menu_name: str, menu_hierarchy: str, item: NavItem):
        """ Registers a menu item.

            :param menu_name: A unique identifier for the menu.
            :param menu_hierarchy: A period-delimited position of the item within the menu.
            :param item: The navigation item to append.
        """
        if menu_name not in self._menus:
            self._menus[menu_name] = NavMenu()
        self._menus[menu_name].append_at(menu_hierarchy.split("."), item)

    def set_login_url(self, url: str):
        """ Sets the login URL for the site. """
        self._flask_globals['login_url'] = url

    def set_logout_url(self, url: str):
        """ Sets the logout URL for the site. """
        self._flask_globals['logout_url'] = url

    def set_refresh_session_url(self, url: str):
        """ Sets the URL for refreshing the current session. """
        self._flask_globals['refresh_session_url'] = url

    def before_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        """ Registers a callback to run before Flask-related stuff is initialized."""
        self.events.on('init.flask.before', cb)

    def on_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        """ Registers a callback to run while Flask-related stuff is initialized."""
        self.events.on('init.flask', cb)

    def after_flask_init(self, cb: t.Callable[[flask.Flask], t.Any] | str):
        """ Registers a callback to run after Flask-related stuff is initialized. """
        self.events.on('init.flask.after', cb)

    def init_app(self, app: flask.Flask):
        """ Initializes the flask Application. """
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
        if self._metrics_type is not MetricsType.DISABLED:
            from gcapp.metrics import PromMetrics
            @injector.inject
            def _run_me(prom_metrics: PromMetrics = auto()):
                prom_metrics.init_app(self._metrics_type is MetricsType.MULTIPROCESSING)
            _run_me()

    def _load_config(self):
        """ Load configuration for flask """
        self.flask_app.config.update(self.config.get("flask", default={}))
        if 'PERMANENT_SESSION_LIFETIME' not in self.flask_app.config:
            self.flask_app.config['PERMANENT_SESSION_LIFETIME'] = 44640
        self._session_timeout = self.flask_app.config['PERMANENT_SESSION_LIFETIME'].total_seconds() - 1

    def _verify_secret_key(self):
        """ Ensure there is a secret key present. """
        secret_key = self.flask_app.config.get('SECRET_KEY', None)
        if not secret_key:
            raise GCFlaskError('Secret key is missing or empty', 1000)
        if isinstance(secret_key, str):
            self.flask_app.config['SECRET_KEY'] = secret_key.encode('utf-8')

    def _configure_autoinject(self):
        """ Ensure injection is done properly for Flask requests"""
        flask_autoinject.init_app(self.flask_app)

    def _configure_bilingual_routes(self):
        """ Ensure urls have bilingual routes """
        self.flask_app.url_rule_class = BilingualRule

    def _configure_jinja(self):
        """ Configures template stuff. """
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
        """ Handles context for functions that don't take it. """
        @pass_context
        def _wrapper(ctx, *args, **kwargs):
            return func(*args, **kwargs)
        return _wrapper

    def _configure_proxy_fix(self):
        """ Sets up proxy manager. """
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
        """ Adds the before and after request hooks, and the context processor. """
        self.flask_app.before_request(self._before_request)
        self.flask_app.after_request(self._after_request)
        self.flask_app.context_processor(self._context_processor)

    @injector.inject
    def _before_request(self, rinfo: RequestInfo = auto()):
        """ Add the web-based extra logging information"""
        rinfo.set_logging_extras_web()

    @injector.inject
    def _after_request(self, response: flask.Response, wsr: CSPRegistry = auto()) -> flask.Response:
        """ Log the response and add the CSP/caching headers"""
        zrlog.get_logger('gcflask.access').log(
            # static requests are spammy, so we set them to debug instead of info
            logging.DEBUG if flask.request.endpoint == 'static' else logging.INFO,
            "%s \"%s\" %s",
            flask.request.method,
            flask.request.url,
            response.status_code
        )
        response.headers.update(wsr.get_headers())

        # TODO: revisit caching
        response.cache_control.max_age = 0
        response.cache_control.no_store = True
        response.cache_control.no_cache = True
        response.cache_control.private = True
        response.cache_control.must_revalidate = True

        return response

    @injector.inject
    def _context_processor(self, ld: i18n.LanguageDetector = auto(), tm: i18n.TranslationManager =  auto()):
        """ Adds extra context to the template rendering. """

        # page language and i18n sorting
        language = ld.detect_language(tm.supported_languages())
        contextual_vars: dict[str, t.Any] = {
            'language': language,
            'i18n_sort': functools.partial(i18n.i18n_sort, language_order=[language, 'und']),
        }

        # menus
        for menu_name in self._menus:
            contextual_vars[f"menu_{menu_name}"] = self._menus[menu_name]

        # title
        if flask.has_request_context():
            contextual_vars["default_title"] = i18n.tr(f"{flask.request.endpoint}.title", default=t.cast(str, flask.request.endpoint))
        else:
            contextual_vars["default_title"] = ''

        return contextual_vars

    def _configure_csrf(self):
        """ Enables CSRF protection """
        self.flask_app.extensions['csrf'] = CSRFProtect(self.flask_app)

    def _configure_resource_endpoint(self):
        """ Adds a resources endpoint that delivers files from the resource directories registered. """
        self.flask_app.add_url_rule(
            f"/resources/<path:filename>",
            endpoint="resources",
            host=None,
            view_func=self._deliver_resource_file,
        )
        self._resource_directories.sort(key=lambda x: x[1], reverse=True)

    def _configure_health_endpoint(self):
        """ Adds a health endpoint. """
        self.flask_app.add_url_rule(
            f"/-/health",
            endpoint="health",
            host=None,
            view_func=self._health_check
        )

    def _health_check(self) -> tuple[str, int]:
        """ Health check endpoint."""
        return 'healthy', 200

    def _deliver_resource_file(self, filename: str) -> flask.Response:
        """ Resource endpoint. """
        for path, _ in self._resource_directories:
            test_path = (path / filename).absolute().resolve()
            if test_path.exists():
                # NB: do not deliver content directly, rely on flask to do it right
                return flask.send_from_directory(path, filename)
        return flask.abort(404)

    def register_blueprint(self, module_name: str, object_name: str, path_prefix: str = ""):
        """ Registers a blueprint to include. """
        self._flask_blueprints.append((module_name, object_name, path_prefix))

    def _load_blueprints(self):
        universal_prefix = self.config.get(('gcflask', 'path_prefix'), default='').rstrip('/')
        if universal_prefix:
            universal_prefix = f"/{universal_prefix.lstrip('/')}"
        for module_name, object_name, path_prefix in self._flask_blueprints:
            try:
                if universal_prefix or path_prefix:
                    prefix = '/' + ('/'.join((universal_prefix.strip('/'), path_prefix.strip('/'))))
                    self.flask_app.register_blueprint(dynamic_object(f"{module_name}.{object_name}"), url_prefix=prefix)
                else:
                    self.flask_app.register_blueprint(dynamic_object(f"{module_name}.{object_name}"))
            except Exception as ex:
                ex.add_note(f"Error while registering {module_name}.{object_name}")
                raise
