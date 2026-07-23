import typing as t
import flask_login as fl

from medsutil.awaretime import AwareDateTime

ADMIN_PRIVILEGE = '__admin__'
ANONYMOUS_PRIVILEGE = '__anonymous__'
ANYONE_PRIVILEGE = '__anyone__'
AUTHENTICATED_PRIVILEGE = '__authenticated__'


class AnyPermission:

    def __init__(self, permissions: t.Sequence[str | AnyPermission]):
        self.permissions: t.Sequence[PermissionType] = permissions

PermissionType = AnyPermission | str | t.Sequence[str | AnyPermission] | None


class BaseUserMixin:

    def __init__(self, display_name: str | None = None, email: str | None = None, permissions: t.Iterable[str] | None = None, **extras):
        super().__init__()
        self._permissions: set[str] = set(permissions or [])
        self._permissions.add(ANYONE_PRIVILEGE)
        self._email = email or None
        self._display = display_name or None
        self._extras = extras

    @property
    def email(self) -> str:
        return self._email or ''

    @property
    def display_name(self) -> str:
        return self._display or ''

    @property
    def is_admin(self) -> bool:
        return ADMIN_PRIVILEGE in self._permissions

    @property
    def permissions(self) -> set[str]:
        return self._permissions

    def require_all(self, permissions: PermissionType) -> bool:
        """Check if the user has the given permission."""
        if (not permissions) or self.is_admin:
            return True
        perms = [permissions] if isinstance(permissions, (str, AnyPermission)) else permissions
        for x in perms:
            if isinstance(x, str):
                if x not in self._permissions:
                    return False
            elif isinstance(x, AnyPermission):
                if not self.require_any(x.permissions):
                    return False
        return True

    def require_any(self, permissions: PermissionType) -> bool:
        if (not permissions) or self.is_admin:
            return True
        perms = [permissions] if isinstance(permissions, (str, AnyPermission)) else permissions
        for x in perms:
            if isinstance(x, str):
                if x in self._permissions:
                    return True
            elif isinstance(x, AnyPermission):
                if self.require_any(x.permissions):
                    return True
        return False

    def last_login_success_time(self) -> AwareDateTime | None:
        dt = self.extra('last_success', None)
        if dt is not None:
            return AwareDateTime.fromisoformat(dt)
        return None

    def last_login_error_time(self) -> AwareDateTime | None:
        dt = self.extra('last_error', None)
        if dt is not None:
            return AwareDateTime.fromisoformat(dt)
        return None

    def last_login_success_ip(self) -> str:
        return self.extra('last_success_ip', '') or ''

    def last_login_error_ip(self) -> str:
        return self.extra('last_error_ip', '') or ''

    def total_errors_since_last_login(self) -> int | None:
        return int(self.extra('total_errors', 0))

    def extra(self, name: str, default = None) -> t.Any:
        """Retrieve the value of an extra user property as set by the authentication system."""
        try:
            return self._extras[name]
        except KeyError:
            return default



class AuthenticatedUser(BaseUserMixin, fl.UserMixin):
    """Represents an authenticated user."""

    def __init__(self,
                 unique_id: int,
                 username: str,
                 display_name: str,
                 email: str = None,
                 permissions: t.Iterable[str] | None = None,
                 **extras):
        super().__init__(display_name, email, permissions, **extras)
        self._unique_id = unique_id
        self._username = username
        self._permissions.add(AUTHENTICATED_PRIVILEGE)

    def get_username(self) -> str:
        return self._username

    def get_id(self) -> int:
        return self._unique_id


class AnonymousUser(fl.AnonymousUserMixin, BaseUserMixin):
    """Anonymous implementation of the AuthenticatedUser."""

    def __init__(self):
        super().__init__(
            permissions=[ANONYMOUS_PRIVILEGE]
        )

def current_user() -> AuthenticatedUser | AnonymousUser:
    import flask, flask_login
    if flask.has_request_context():
        return flask_login.current_user
    return AnonymousUser()