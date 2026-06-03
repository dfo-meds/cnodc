import base64
import binascii
import enum
import typing as t
import uuid

import flask_login
import itsdangerous
import zirconium as zr
from autoinject import injector
import zrlog
import flask
import flask_login as fl

from gcflask.i18n import LanguageDetector, TranslationManager
from gcflask.user import AuthenticatedUser, AnonymousUser
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_object
from medsutil.secure import SecureOperations
from medsutil.exceptions import CodedError


class AuthError(CodedError):
    CODE_SPACE='AUTH'

    def __init__(self, msg, code_number, *, is_lockable: bool = False, create_record: bool = True, username: str | None = None):
        super().__init__(msg, code_number)
        self.is_lockable = is_lockable
        self.create_record = create_record
        self.username = username


class AuthenticationHandler:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, handler_name: str, supports_interactive: bool = True, *, authentication_manager: AuthenticationManager):
        self._auth_manager = authentication_manager
        self._handler_name = handler_name
        self.supports_interactive = supports_interactive
        self._log = zrlog.get_logger(f"login_handler.{handler_name}")

    def display_name(self):
        return self._handler_name

    def load_user(self, user_id):
        return None

    def login_page(self):
        raise NotImplementedError

    def logout(self) -> t.Any: ...

    def update_user(self, user: AuthenticatedUser):
        raise NotImplementedError

    def _log_login_success(self, user: AuthenticatedUser, from_api: bool = False):
        self._create_login_record(
            success=True,
            username=user.get_username(),
            from_api=from_api,
        )
        self._log.notice("Successful login for %s", user.get_username())

    def _log_login_error(self, error: AuthError, from_api: bool = False):
        if error.create_record:
            self._create_login_record(
                success=False,
                from_api=from_api,
                error_message=str(error),
                lockable=error.is_lockable,
                username=error.username
            )
        self._log.error(str(error), exc_info=(error.__class__, error, error.__traceback__))


    def _create_login_record(self,
                             success: bool,
                             username: str | None = None,
                             from_api: bool = False,
                             error_message: str | None = None,
                             lockable: bool = False): ...


    def attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        try:
            user = self._attempt_login_from_password(username, password)
            if user is not None:
                self._log_login_success(user)
            return user
        except AuthError as ex:
            ex.username = username
            self._log_login_error(ex)

    def attempt_login_from_redirect(self, redirect_info: dict):
        try:
            user = self._attempt_login_from_redirect(redirect_info)
            if user is not None:
                self._log_login_success(user)
            return user
        except AuthError as ex:
            self._log_login_error(ex)

    def attempt_login_from_request(self, request: flask.Request) -> AuthenticatedUser | None:
        try:
            auth_header = request.headers.get('Authorization', default=None)
            if auth_header:
                user = self._attempt_login_from_auth_header(auth_header)
            else:
                raise AuthError("No request login method found", 2000, create_record=False)
            if user is not None:
                self._log_login_success(user, True)
            return user
        except AuthError as ex:
            self._log_login_error(ex, True)

    def _attempt_login_from_auth_header(self, auth_header: str) -> AuthenticatedUser | None:
        if ' ' not in auth_header:
            raise AuthError("Invalid authorization header format", 1000, create_record=False)
        scheme, token = auth_header.split(' ', maxsplit=1)
        if scheme.lower() == 'basic':
            return self._attempt_login_from_basic_auth(token)
        elif scheme.lower() == 'bearer':
            return self._attempt_login_from_token(token)
        raise AuthError(f"Invalid authorization scheme: {scheme}", 1001, create_record=False)

    def _attempt_login_from_basic_auth(self, auth_header: str) -> AuthenticatedUser | None:
        try:
            auth_decoded = base64.b64decode(auth_header).decode("utf-8")
            if ":" not in auth_decoded:
                raise AuthError('Invalid format, missing semi-colon', 1101, create_record=False)
            pieces = auth_decoded.split(":", maxsplit=1)
            if not len(pieces) == 2:
                raise AuthError('Invalid format, not enough pieces', 1102, create_record=False)
            return self._attempt_login_from_password(pieces[0], pieces[1])
        except binascii.Error:
            raise AuthError("Invalid base64 encoding for basic auth", 1103, create_record=False)
        except UnicodeError:
            raise AuthError("Invalid utf-8 encoding for basic auth", 1104, create_record=False)

    def _attempt_login_from_token(self, token: str) -> AuthenticatedUser | None:
        raise AuthError("Tokens not supported", 9000, create_record=False)

    def _attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        raise AuthError("Passwords not supported", 9001, create_record=False, username=username)

    def _attempt_login_from_redirect(self, redirect_info: dict):
        raise AuthError("Redirect not supported", 9002)


class AuthResult(enum.Enum):
    DENY = "deny"
    ALLOW = "allow"
    SPLASH = "splash"


@injector.injectable
class SessionStore:

    def __init__(self):
        self._session_lifetime: int = 3600 * 24 * 30

    def verify_session(self):
        if flask.has_request_context():
            sess_id = flask.session.get('_session_uuid', None)
            if sess_id is not None:
                is_valid, expiry_time = self._get_session_info(sess_id)
                if (not is_valid) or expiry_time < AwareDateTime.utcnow():
                    self._invalidate_session(sess_id)
                    self.create()
            else:
                self.create()

    def create(self):
        if flask.has_request_context():
            flask.session.clear()
            flask.session['_session_uuid'] = str(uuid.uuid4())
            self._persist_session(flask.session['_session_uuid'], AwareDateTime.utcnow() + flask.current_app.permanent_session_lifetime)
            flask.session.permanent = True
            flask.session.modified = True

    def invalidate(self):
        if flask.has_request_context():
            sess_id = flask.session.get('_session_uuid', None)
            if sess_id is not None:
                self._invalidate_session(sess_id)
        self.create()

    def refresh(self):
        if flask.has_request_context():
            sess_id = flask.session.get('_session_uuid', None)
            self.verify_session()
            if sess_id is not None:
                self._refresh_session(sess_id, AwareDateTime.utcnow() + flask.current_app.permanent_session_lifetime)
                flask.session.permanent = True
                flask.session.modified = True

    def _get_session_info(self, session_id: str) -> tuple[bool, AwareDateTime]: ...
    def _refresh_session(self, session_id: str, valid_until: AwareDateTime): ...
    def _persist_session(self, session_id: str, valid_until: AwareDateTime): ...
    def _invalidate_session(self, session_id: str): ...


@injector.injectable_global
class AuthenticationManager:

    config: zr.ApplicationConfig = None
    secure_ops: SecureOperations = None
    ld: LanguageDetector = None
    tm: TranslationManager = None
    sessions: SessionStore = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("gcflask.auth")
        self._login_managers: dict[str, AuthenticationHandler] = {}
        others = self.config.as_dict(("gcflask", "authentication", "handlers"), default={})
        for hname in others:
            self._login_managers[hname] = dynamic_object(others[hname])(hname, authentication_manager=self)
        self._login_required_redirect: str = self.config.as_str(("gcflask", "authentication", "login_required"), default="auth.login")
        self._login_redirect: str = self.config.as_str(("gcflask", "authentication", "login_success"), default="base.home")
        self._logout_redirect: str = self.config.as_str(("gcflask", "authentication", "logout_success"), default="base.home")
        self._forbidden_redirect: str = self.config.as_str(("gcflask", "authentication", "unauthorized"), default="base.home")
        self._csrf_redirect: str = self.config.as_str(("gcflask", "authentication", "referrer_failed"), default="base.splash")
        self._login_selected_redirect: str = self.config.as_str(("gcflask", "authentication", "login_for_handler"), default="auth.login_for_handler")
        self._interactive_managers: list[str] = [x for x, y in self._login_managers.items() if y.supports_interactive]

    @property
    def login_view(self):
        return self._login_required_redirect

    def anonymous_user(self):
        return AnonymousUser()

    def unauthorized_handler(self, result=AuthResult.DENY, is_api_call: bool = False):
        """Handle unauthorized requests."""
        if is_api_call:
            return flask.abort(403)
        if not fl.current_user.is_authenticated:
            return flask.redirect(flask.url_for(
                self._login_required_redirect,
                next_url=self.secure_ops.timed_serializer.dumps(flask.request.url)
            ))
        if result is AuthResult.SPLASH:
            return flask.redirect(flask.url_for(
                self._csrf_redirect,
                next_url=self.secure_ops.timed_serializer.dumps(flask.request.url)
            ))
        return flask.redirect(flask.url_for(
            self._forbidden_redirect
        ))

    def endpoint_login(self, redirect_to: str = None):
        if fl.current_user.is_authenticated:
            return self.login_success(redirect_to)
        elif len(self._interactive_managers) > 1:
            return flask.render_template('auth_handler_select.html', options={
                x: flask.url_for(self._login_selected_redirect, handler=x)
                for x in self._interactive_managers
            })
        elif not self._interactive_managers:
            raise flask.abort(404)
        else:
            return self.endpoint_login_for_handler(self._interactive_managers[0])

    def endpoint_login_for_handler(self, handler_name: str):
        if fl.current_user.is_authenticated:
            return self.login_success()
        elif handler_name not in self._interactive_managers:
            return flask.abort(404)
        else:
            return self._login_managers[handler_name].login_page()

    def login_user(self, user: AuthenticatedUser, auth_handler_name: str, redirect_to: str = None, no_redirect: bool = False):
        self.sessions.invalidate()
        fl.login_user(user)
        flask.session['auth_handler'] = auth_handler_name
        if no_redirect:
            return None
        return self.login_success(redirect_to)

    def login_success(self, redirect_to: str = None):
        next_url: str | None = None
        if 'next_url' in flask.request.args:
            try:
                next_url = self.secure_ops.timed_serializer.loads(flask.request.args["next_url"], 3600)
            except itsdangerous.BadData:
                self._log.exception("Error while unserializing next_url from args")
        lang = None
        if 'lang' in flask.request.args:
            lang = flask.request.args['lang']
        elif 'language' in flask.session:
            lang = flask.session['language']
        if lang is None or lang not in self.tm.supported_languages():
            lang = ""
        if next_url is not None:
            return flask.redirect(next_url)
        return flask.redirect(flask.url_for(self._login_redirect, lang=lang))

    def endpoint_logout(self):
        auth_handler: str | None = flask.session.get('auth_handler')
        response = None
        if auth_handler and auth_handler in self._login_managers:
            response = self._login_managers[auth_handler].logout()
        fl.logout_user()
        flask.session.modified = True
        return response or self.logout_success()

    def logout_success_url(self):
        flask.url_for(self._logout_redirect, _external=True)

    def user_loader(self, user_id) -> t.Optional[AuthenticatedUser]:
        auth_handler: str | None = flask.session.get('auth_handler')
        if auth_handler and auth_handler in self._login_managers:
            result = self._login_managers[auth_handler].load_user(user_id)
            return result
        return None

    def logout_success(self):
        return flask.redirect(flask.url_for(self._logout_redirect))

    def request_loader(self, request: flask.Request) -> t.Optional[AuthenticatedUser]:
        for h in self._login_managers:
            user = self._login_managers[h].attempt_login_from_request(request)
            if user is not None:
                self.login_user(user, h, no_redirect=True)
                return user
        return None

    def endpoint_login_from_redirect(self, handler_name: str, redirect_info: dict):
        if fl.current_user.is_authenticated:
            return self.login_success()
        if handler_name not in self._login_managers:
            return flask.abort(404)
        return self._login_managers[handler_name].attempt_login_from_redirect(redirect_info)


@injector.inject
def init_app(app: flask.Flask, am: AuthenticationManager = None):
    lm = flask_login.LoginManager()
    lm.session_protection = 'strong'
    lm.init_app(app)
    lm.user_loader(am.user_loader)
    lm.request_loader(am.request_loader)
    lm.unauthorized_handler(am.unauthorized_handler)
    lm.anonymous_user = am.anonymous_user
    lm.login_view = am.login_view
    app.extensions['login_manager'] = lm
