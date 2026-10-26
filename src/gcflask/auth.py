"""
Secure authentication and authorization system for Flask.
"""
import base64
import binascii
import enum
import typing as t
import uuid

import itsdangerous
import zirconium as zr
from autoinject import injector, auto
import zrlog
import flask
import flask_login as fl

from gcapp.i18n.base import TranslationManager, LanguageDetector
from gcflask.user import AuthenticatedUser, AnonymousUser
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_object
from medsutil.secure import SecureOperations
from medsutil.exceptions import CodedError


class AuthError(CodedError):
    """ Errors that occurred during authentication or authorization. """
    CODE_SPACE='AUTH'

    def __init__(self, msg, code_number, *, is_lockable: bool = False, create_record: bool = True, username: str | None = None, as_info: bool = False):
        """

        :param msg: A message about what happened
        :param code_number: A unique code number within the code space
        :param is_lockable: Set to true if the action could lock the user's account
        :param create_record: Set to false to not create a login record
        :param username: The username that was trying to login
        :param as_info: Set to true to log this as informational, not an error (prevents spam)
        """
        super().__init__(msg, code_number)
        self.is_lockable = is_lockable
        self.create_record = create_record
        self.username = username
        self.as_info = as_info


class BaseAuthenticationHandler:
    """ Base class for those that handle authentication of the user. """

    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self, handler_name: str, supports_interactive: bool = True, *, authentication_manager: AuthenticationManager):
        """
        :param handler_name: A unique identifier for this handler.
        :param supports_interactive: Set to true if the handler supports redirecting the user to a webpage
        :param authentication_manager: The authentication manager that is processing the request
        """
        self._auth_manager = authentication_manager
        self._handler_name = handler_name
        self.supports_interactive = supports_interactive
        self._log = zrlog.get_logger(f"gcflask.auth.{handler_name}")

    def display_name(self):
        """ The name to show the user. """
        return self._handler_name

    def load_user(self, user_id: str | int) -> AuthenticatedUser | None:
        """ Finds and returns the user with the given user ID, or None if one can't be found. """
        return None

    def login_page(self) -> flask.Response | str:
        """ Returns the login page or a redirect to it. """
        raise NotImplementedError

    def logout(self) -> flask.Response | str | None:
        """ Returns a logout page, redirect, or None to use the defualt one. """
        return None

    def _log_login_success(self, user: AuthenticatedUser, from_api: bool = False):
        """ Logins a successful login for a given user. """
        self._create_login_record(
            success=True,
            username=user.get_username(),
            from_api=from_api,
        )
        self._log.notice("Successful login for %s", user.get_username())

    def _log_login_error(self, error: AuthError, from_api: bool = False):
        """ Logs an error that occurred during authentication. """
        if error.create_record:
            self._create_login_record(
                success=False,
                from_api=from_api,
                error_message=str(error),
                lockable=error.is_lockable,
                username=error.username
            )
        if error.as_info:
            self._log.info(str(error), exc_info=(error.__class__, error, error.__traceback__))
        else:
            self._log.error(str(error), exc_info=(error.__class__, error, error.__traceback__))

    def _create_login_record(self,
                             success: bool,
                             username: str | None = None,
                             from_api: bool = False,
                             error_message: str | None = None,
                             lockable: bool = False):
        """ Creates a persistent login record. """
        ...

    def attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        """ Attempts to authenticate a user given their username and password. """
        try:
            user = self._attempt_login_from_password(username, password)
            if user is not None:
                self._log_login_success(user)
            return user
        except AuthError as ex:
            ex.username = username
            self._log_login_error(ex)

    def _attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        """ Actual authentication for password-based logins (default is it's not supported).

            :raises AuthError: When password-based logins are not supported (9001).
        """
        raise AuthError("Passwords not supported", 9001, create_record=False, username=username)

    def attempt_login_from_redirect(self, redirect_info: dict):
        """ Attempts to authenticate a user from a redirect (i.e. from an OAuth response). """
        try:
            user = self._attempt_login_from_redirect(redirect_info)
            if user is not None:
                self._log_login_success(user)
            return user
        except AuthError as ex:
            self._log_login_error(ex)

    def _attempt_login_from_redirect(self, redirect_info: dict):
        """ Actual authentication for redirect-based logins (default is it's not supported).

            :raises AuthError: When redirect-based logins are not supported (9002).
        """
        raise AuthError("Redirect not supported", 9002)

    def attempt_login_from_request(self, request: flask.Request) -> AuthenticatedUser | None:
        """ Attempts to authenticate a user using the Authorization header. """
        try:
            auth_header = request.headers.get('Authorization', default=None)
            if auth_header:
                user = self._attempt_login_from_auth_header(auth_header)
                if user is not None:
                    self._log_login_success(user, True)
                return user
        except AuthError as ex:
            self._log_login_error(ex, True)
        return None

    def _attempt_login_from_auth_header(self, auth_header: str) -> AuthenticatedUser | None:
        """ Processes an HTTP Authorization header and attempts a login from it.

            :raises AuthError: If there is no space in the Authorization header (1000)
            :raises AuthError: If the scheme isn't recognized (1001)
        """

        # Auth headers are always SCHEME TOKEN, so if there's no space it's invalid
        if ' ' not in auth_header:
            raise AuthError("Invalid authorization header format", 1000, create_record=False)

        scheme, token = auth_header.split(' ', maxsplit=1)

        # Basic auth
        if scheme.lower() == 'basic':
            return self._attempt_login_from_basic_auth(token)

        # Bearer auth
        elif scheme.lower() == 'bearer':
            return self._attempt_login_from_token(token)

        # Some other method we don't support
        raise AuthError(f"Invalid authorization scheme: {scheme}", 1001, create_record=False)

    def _attempt_login_from_basic_auth(self, auth_header: str) -> AuthenticatedUser | None:
        """ Processes an HTTP Basic Authorization header and attempts a login from it.

            :raises AuthError: If there is no colon in the header (1101)
            :raises AuthError: If the header is not base64 encoded (1103)
            :raises AuthError: If the header is not UTF-8 encoded (1104)
        """
        try:
            # Auth header token is base64 encoded, so we decode it
            auth_decoded = base64.b64decode(auth_header).decode("utf-8")

            # Format is expected to be username:password
            if ":" not in auth_decoded:
                raise AuthError('Invalid format, missing colon', 1101, create_record=False)

            # Defer to password-based authentication
            username, password = auth_decoded.split(":", maxsplit=1)
            return self._attempt_login_from_password(username, password)

        except binascii.Error:
            raise AuthError("Invalid base64 encoding for basic auth", 1103, create_record=False)

        except UnicodeError:
            raise AuthError("Invalid utf-8 encoding for basic auth", 1104, create_record=False)

    def _attempt_login_from_token(self, token: str) -> AuthenticatedUser | None:
        """ Actual token-based authentication from Authorization header (not supported by default).

            :raises AuthError: If token-based logins are not supported (9000)
        """
        raise AuthError("Tokens not supported", 9000, create_record=False)


class AuthResult(enum.Enum):
    """ Authentication and authorization results in one of these states. """

    DENY = "deny"
    """ The user is not allowed here """

    ALLOW = "allow"
    """ The user is allowed here """

    SPLASH = "splash"
    """ The user should be redirected to the landing page """


@injector.injectable
class SessionStore:
    """ Manager for sessions to find and load them."""

    def __init__(self):
        self._session_lifetime: int = 3600 * 24 * 30

    def verify_session(self):
        """ Ensures the session is valid and active - otherwise it's created."""
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
        """ Creates a new session, clearing any existing one for the user. """
        if flask.has_request_context():
            flask.session.clear()
            flask.session['_session_uuid'] = str(uuid.uuid4())
            self._persist_session(flask.session['_session_uuid'], AwareDateTime.utcnow() + flask.current_app.permanent_session_lifetime)
            flask.session.permanent = True
            flask.session.modified = True

    def invalidate(self):
        """ Invalidates the current session and creates a new one. """
        if flask.has_request_context():
            sess_id = flask.session.get('_session_uuid', None)
            if sess_id is not None:
                self._invalidate_session(sess_id)
        self.create()

    def refresh(self):
        """ Refreshes the session, extending its lifetime. """
        if flask.has_request_context():
            sess_id = flask.session.get('_session_uuid', None)
            self.verify_session()
            if sess_id is not None:
                self._refresh_session(sess_id, AwareDateTime.utcnow() + flask.current_app.permanent_session_lifetime)
                flask.session.permanent = True
                flask.session.modified = True

    def _get_session_info(self, session_id: str) -> tuple[bool, AwareDateTime]:
        """ Retrieves if the session is valid and its expiry time. """
        ...

    def _refresh_session(self, session_id: str, valid_until: AwareDateTime):
        """ Updates the expiry time of the session. """
        ...

    def _persist_session(self, session_id: str, valid_until: AwareDateTime):
        """ Persists the session to the database."""
        ...

    def _invalidate_session(self, session_id: str):
        """ Invalidates a session identifier. """
        ...


@injector.injectable_global
class AuthenticationManager:
    """ Global singleton class for managing authentication."""

    config: zr.ApplicationConfig = auto()
    secure_ops: SecureOperations = auto()
    ld: LanguageDetector = auto()
    tm: TranslationManager = auto()
    sessions: SessionStore = auto()

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("gcflask.auth")
        self._login_managers: dict[str, BaseAuthenticationHandler] = {}
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
    def login_view(self) -> str:
        return self._login_required_redirect

    def anonymous_user(self) -> fl.AnonymousUserMixin:
        return AnonymousUser()

    def unauthorized_handler(self,
                             result: AuthResult = AuthResult.DENY,
                             is_api_call: bool = False) -> flask.Response :
        """ Handles unauthorized requests.

            :param result: The outcome of the authorization process.
            :param is_api_call: Whether the current call is to an API or not.

            :raises werkzeug.exceptions.HTTPException: When is_api_call is True
        """

        # API calls result in a 403 error
        if is_api_call:
            return flask.abort(403)

        # if not logged in, return the login required page
        if not fl.current_user.is_authenticated:
            return flask.redirect(flask.url_for(
                self._login_required_redirect,
                next_url=self.secure_ops.timed_serializer.dumps(flask.request.url)
            ))

        # if the result indicated we should go to the splash page, we do that
        if result is AuthResult.SPLASH:
            return flask.redirect(flask.url_for(
                self._csrf_redirect,
                next_url=self.secure_ops.timed_serializer.dumps(flask.request.url)
            ))

        # otherwise, go to the forbidden page
        return flask.redirect(flask.url_for(
            self._forbidden_redirect
        ))

    def endpoint_login(self, redirect_to: str = None) -> flask.Response | str:
        """ Handles a request to login via one of the available handlers.

            If the user is already logged in, they'll be immediately redirected.

            If there is exactly one login handler, we will immediately redirect to it.
            Otherwise, a selection page will be displayed.

            :param redirect_to: When login is complete, where to send the user.

            :raises werkzeug.exceptions.HTTPException: When there are no registered login handlers.
        """
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

    def endpoint_login_for_handler(self, handler_name: str) -> flask.Response | str:
        """ Handles a request to login via a specific handler.

            :raises werkzeug.exceptions.HTTPException: If the requested handler does not exist.
        """
        if fl.current_user.is_authenticated:
            return self.login_success()
        elif handler_name not in self._interactive_managers:
            return flask.abort(404)
        else:
            return self._login_managers[handler_name].login_page()

    def login_user(self,
                   user: AuthenticatedUser,
                   auth_handler_name: str,
                   redirect_to: str | None = None,
                   no_redirect: bool = False) -> flask.Response | None:
        """ Logins in a user and redirects to the next page.

            This invalidates the current session, logs in the user, and stores enough details to get the user later.

            :param user: The user to login.
            :param auth_handler_name: The name of the authentication handler that authenticated the user.
            :param redirect_to: When login is complete, where to send the user.
            :param no_redirect: If true, redirect is skipped and None is returned..
        """
        self.sessions.invalidate()
        fl.login_user(user)
        flask.session['auth_handler'] = auth_handler_name
        if no_redirect:
            return None
        return self.login_success(redirect_to)

    def login_success(self, redirect_to: str = None) -> flask.Response:
        """ Handles what happens next once login is successful."""
        next_url: str | None = redirect_to
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

    def endpoint_logout(self) -> flask.Response | str:
        """ Logouts out the current user. """
        auth_handler: str | None = flask.session.get('auth_handler')
        response = None
        if auth_handler and auth_handler in self._login_managers:
            response = self._login_managers[auth_handler].logout()
        fl.logout_user()
        flask.session.modified = True
        return response or self.logout_success()

    def logout_success_url(self) -> str:
        """ Returns the URL of the logout redirect page. """
        return flask.url_for(self._logout_redirect, _external=True)

    def user_loader(self, user_id: str | int) -> t.Optional[AuthenticatedUser]:
        """ Loads the current user and returns them, or None if no user is logged in. """
        auth_handler: str | None = flask.session.get('auth_handler')
        if auth_handler and auth_handler in self._login_managers:
            result = self._login_managers[auth_handler].load_user(user_id)
            return result
        return None

    def logout_success(self) -> flask.Response:
        """ Returns a redirect to the logout page. """
        return flask.redirect(flask.url_for(self._logout_redirect))

    def request_loader(self, request: flask.Request) -> t.Optional[AuthenticatedUser]:
        """ Attempts a login from a request header. """
        for h in self._login_managers:
            user = self._login_managers[h].attempt_login_from_request(request)
            if user is not None:
                self.login_user(user, h, no_redirect=True)
                return user
        return None

    def endpoint_login_from_redirect(self, handler_name: str, redirect_info: dict):
        """ deprecated? """
        if fl.current_user.is_authenticated:
            return self.login_success()
        if handler_name not in self._login_managers:
            return flask.abort(404)
        return self._login_managers[handler_name].attempt_login_from_redirect(redirect_info)


@injector.inject
def init_app(app: flask.Flask, am: AuthenticationManager = auto()):
    """ Initializes a Flask application to use gcflask's authentication component """

    # Use flask_login with strong enough configuration for GC security requirements
    lm = fl.LoginManager()
    lm.session_protection = 'strong'
    lm.init_app(app)

    # Configure the login manager to use the authentication manager (which delegates to handlers)
    lm.user_loader(am.user_loader)
    lm.request_loader(am.request_loader)
    lm.unauthorized_handler(am.unauthorized_handler)
    lm.anonymous_user = am.anonymous_user
    lm.login_view = am.login_view

    # Add to the extensions list
    app.extensions['login_manager'] = lm
