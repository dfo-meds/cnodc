import functools

import flask_login
import flask_login as fl
import flask
import zirconium as zr
from autoinject import injector, auto
import typing as t
import zrlog
from urllib.parse import urlparse

from gcflask.auth import AuthResult, AuthenticationManager
from gcflask.user import AuthenticatedUser, ANONYMOUS_PRIVILEGE, ADMIN_PRIVILEGE, ANYONE_PRIVILEGE, \
    AUTHENTICATED_PRIVILEGE


@injector.injectable_global
class RequestSecurity:
    """Global security handler for requests"""

    cfg: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._allowed_hosts = self.cfg.get(("gcflask", "security", "allowed_hosts"), default=[])
        self._require_https = self.cfg.as_bool(("gcflask", "security", "require_https"), default=False)
        self._verify_referrer_for_get = self.cfg.as_bool(("gcflask", "security", "verify_referrer_for_get"), default=False)
        self._log = zrlog.get_logger("gcflask.security")

    def is_authenticated(self) -> bool:
        """Check if the user is authenticated."""
        if flask.has_request_context():
            return fl.current_user.is_authenticated
        return False

    def require_all(self, perm_names: t.Sequence[str] | str) -> bool:
        """Check if the user has at least one of the given permissions."""
        if flask.has_request_context():
            cu: AuthenticatedUser = fl.current_user
            if isinstance(perm_names, str):
                return cu.require_all([perm_names])
            else:
                return cu.require_all(perm_names)
        return False

    def check_referrer(self) -> bool:
        """Check that the referer is good."""
        if not self._allowed_hosts:
            self._log.info("Request allowed because no allowed hosts are provided")
            return True
        if not flask.has_request_context():
            self._log.info("Request denied because no request context is available to check the method and referrer")
            return False
        method = flask.request.method
        if method in ("HEAD", "OPTIONS"):
            self._log.info("Request allowed because method type is %s", method)
            return True
        if method in ("GET", "LIST") and not self._verify_referrer_for_get:
            self._log.info("Request allowed because method type is %s and verification is disabled for GET requests", method)
            return True
        ref = flask.request.headers.get("Referer")
        org = flask.request.headers.get("Origin")
        if org is None:
            org = ref
        if org is None:
            self._log.warning(f"Request denied because referrer not provided!")
            return False
        pieces = urlparse(org)
        if pieces.netloc not in self._allowed_hosts:
            self._log.warning(f"Request denied because of bad referrer [%s]", pieces.netloc)
            return False
        return True

    def check_for_https(self) -> bool:
        """Check for HTTPS."""
        if not self._require_https:
            self._log.info("Request allowed because HTTPS is not required")
            return True
        if not flask.has_request_context():
            self._log.info("Request denied because no request context is available")
            return False
        pieces = urlparse(flask.request.url)
        if not pieces.scheme == "https":
            self._log.warning(f"Request denied because it is not an HTTPS request")
            return False
        return True

    def check_access(self,
                     perm_names: t.Sequence[str] | str | None,
                     check_referrer: bool | None = None,
                     check_https: bool | None = None) -> AuthResult:
        """Check all configured requirements to access the page."""
        if perm_names and not self.require_all(perm_names):
            self._log.warning(f"Request denied because of missing privileges: [%s]", perm_names)
            return AuthResult.DENY
        if check_referrer is not False and not self.check_referrer():
            return AuthResult.SPLASH
        if check_https is not False and not self.check_for_https():
            return AuthResult.SPLASH
        return AuthResult.ALLOW


class RequirePermission:
    _auth_man: AuthenticationManager = None
    _rs: RequestSecurity = None

    @property
    def auth_man(self) -> AuthenticationManager:
        if self._auth_man is None:
            self._auth_man = injector.get(AuthenticationManager)
        return self._auth_man

    @property
    def rs(self) -> RequestSecurity:
        if self._rs is None:
            self._rs = injector.get(RequestSecurity)
        return self._rs

    def __call__[**P,Q](self,
        required_permissions: str | t.Sequence[str] | None = None,
        *,
        authenticated_only: bool = False,
        anonymous_only: bool = False,
        anyone: bool = False,
        check_referrer: bool = None,
        check_https: bool = None,
        is_api: bool = False,
        allow_auth_header_access: bool = False) -> t.Callable[[t.Callable[P, Q]], t.Callable[P,Q]]:
        """Ensure the current user is logged in and has one of the given permissions before allowing the request."""

        if (authenticated_only or anonymous_only or anyone):
            if required_permissions is None:
                required_permissions = []
            elif isinstance(required_permissions, str):
                required_permissions = [required_permissions]
            if anonymous_only:
                required_permissions.append(ANONYMOUS_PRIVILEGE)
            if authenticated_only:
                required_permissions.append(AUTHENTICATED_PRIVILEGE)
            if anyone:
                required_permissions.append(ANYONE_PRIVILEGE)

        if is_api:
            allow_auth_header_access = True

        def _decorator(func: t.Callable[P, Q]) -> t.Callable[P,Q]:

            @functools.wraps(func)
            def _decorated(*args, **kwargs):
                if allow_auth_header_access:
                    if flask_login.current_user.is_anonymous:
                        self.auth_man.request_loader(flask.request)
                result = self.rs.check_access(
                    required_permissions,
                    check_referrer,
                    check_https
                )
                if result == AuthResult.ALLOW:
                    return flask.current_app.ensure_sync(func)(*args, **kwargs)
                else:
                    return self.auth_man.unauthorized_handler(result=result, is_api_call=is_api)
            return _decorated
        return _decorator

require_permission = RequirePermission()
