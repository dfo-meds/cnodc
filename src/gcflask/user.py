import typing as t
import flask_login as fl


class AuthenticatedUser(fl.UserMixin):
    """Represents an authenticated user."""

    def __init__(self,
                 unique_id: str,
                 display_name: str,
                 permissions: list[str],
                 **extras):
        self._unique_id = unique_id
        self._permissions = permissions
        self._display = display_name
        self._extras = extras

    def get(self, name: str, default = None) -> t.Any:
        """Retrieve the value of an extra user property as set by the authentication system."""
        try:
            return self._extras[name]
        except KeyError:
            return default

    def get_id(self):
        return self._unique_id

    def is_admin(self):
        return '__admin__' in self._permissions

    def require_all(self, permission_names: t.Sequence[str]):
        """Check if the user has the given permission."""
        return all(x in self._permissions for x in permission_names)


class AnonymousUser(fl.AnonymousUserMixin):
    """Anonymous implementation of the AuthenticatedUser."""

    def __init__(self):
        self.display = "N/A"
        self.user_id = None
        self.organizations = []
        self.datasets = []
        self.permissions = []

    def has_permission(self, permission_name):
        return permission_name == "_is_anonymous" or permission_name == "_is_anyone"

    def is_admin(self):
        return False

    def get(self, name: str, default = None) -> t.Any:
        return default
