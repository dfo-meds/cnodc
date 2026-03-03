from __future__ import annotations
import datetime
import hashlib
import typing as t
import secrets
import enum

import zrlog
from cnodc.util import CNODCError
import cnodc.nodb.base as s

if t.TYPE_CHECKING:  # pragma: no coverage
    from cnodc.nodb import NODBControllerInstance

class UserStatus(enum.Enum):
    """Status of a user in the database."""

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class NODBUser(s.NODBBaseObject):

    DEFAULT_PASSWORD_HASH_ITERATIONS = 752123

    TABLE_NAME = "nodb_users"
    PRIMARY_KEYS: tuple[str] = ("username",)

    username: str = s.StringColumn("username")
    phash: bytes = s.ByteColumn("phash")
    salt: bytes = s.ByteColumn("salt")
    old_phash: t.Optional[bytes] = s.ByteColumn("old_phash")
    old_salt: t.Optional[bytes] = s.ByteColumn("old_salt")
    old_expiry: t.Optional[datetime] = s.DateTimeColumn("old_expiry")
    status: UserStatus = s.EnumColumn("status", UserStatus)
    roles: list = s.JsonColumn("roles")

    def assign_role(self, role_name):
        """Assign a role to the user."""
        if self.roles is None:
            self.roles = [role_name]
            self.modified_values.add('roles')
            self.clear_cache('permissions')
        elif role_name not in self.roles:
            self.roles.append(role_name)
            self.modified_values.add('roles')
            self.clear_cache('permissions')

    def unassign_role(self, role_name):
        """Unassign a role from the user."""
        if self.roles is not None and role_name in self.roles:
            self.roles.remove(role_name)
            self.modified_values.add('roles')
            self.clear_cache('permissions')

    def set_password(self, new_password, salt_length: int = 16, old_expiry_seconds: int = 0):
        """Set the users password."""
        if not isinstance(new_password, str):
            raise CNODCError("Invalid type for new password", "USERCHECK", 1002)
        if len(new_password) > 1024:
            raise CNODCError("Password is too long", "USERCHECK", 1001)
        if new_password == '':
            raise CNODCError('No password provided', 'USERCHECK', 1003)
        if old_expiry_seconds > 0:
            self.old_salt = self.salt
            self.old_phash = self.phash
            self.old_expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=old_expiry_seconds)
        self.salt = secrets.token_bytes(salt_length)
        self.phash = NODBUser.hash_password(new_password, self.salt)

    def check_password(self, password):
        """Check a password to see if it is the correct one."""
        check_hash = NODBUser.hash_password(password, self.salt)
        if secrets.compare_digest(check_hash, self.phash):
            return True
        if self.old_phash and self.old_salt and self.old_expiry:
            if self.old_expiry > datetime.datetime.now(datetime.timezone.utc):
                old_check_hash = NODBUser.hash_password(password, self.old_salt)
                if secrets.compare_digest(old_check_hash, self.old_phash):
                    zrlog.get_logger("cnodc").notice(f"Old password used for login by {self.username}")
                    return True
        return False

    def cleanup(self):
        """Cleanup a user's password entry."""
        if self.old_expiry is not None and self.old_expiry <= datetime.datetime.now(datetime.timezone.utc):
            self.old_phash = None
            self.old_salt = None
            self.old_expiry = None

    def permissions(self, db: NODBControllerInstance) -> set:
        """Retrieve a user's permissions."""
        return self._with_cache('permissions', self._permissions, db)

    def _permissions(self, db: NODBControllerInstance):
        return db.load_permissions(self.roles)

    @staticmethod
    def hash_password(password: str, salt: bytes, iterations=None) -> bytes:
        """Hash a password."""
        if not isinstance(password, str):
            raise CNODCError("Invalid password", "USERCHECK", 1000)
        iterations = iterations or NODBUser.DEFAULT_PASSWORD_HASH_ITERATIONS
        return hashlib.pbkdf2_hmac('sha512', password.encode('utf-8', errors="replace"), salt, iterations)

    @classmethod
    def find_by_username(cls, db, username: str, **kwargs):
        """Locate a user by their username."""
        return db.load_object(cls, {"username": username}, **kwargs)


class NODBSession(s.NODBBaseObject):

    TABLE_NAME: str = "nodb_sessions"
    PRIMARY_KEYS: tuple[str] = ("session_id",)

    session_id: str = s.StringColumn("session_id")
    start_time: datetime = s.DateTimeColumn("start_time")
    expiry_time: datetime = s.DateTimeColumn("expiry_time")
    username: str = s.StringColumn("username")
    session_data: dict = s.JsonColumn("session_data")

    def set_session_value(self, key, value):
        """Set a session value"""
        if self.session_data is None:
            self.session_data = {}
        self.session_data[key] = value

    def get_session_value(self, key, default=None):
        """Get a session value"""
        return self.session_data[key] if self.session_data and key in self.session_data else default

    def is_expired(self) -> bool:
        """Check if the session is expired."""
        return self.expiry_time < datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def find_by_session_id(cls, db, session_id: str, **kwargs):
        """Locate a session by its ID."""
        return db.load_object(cls, {"session_id": session_id}, **kwargs)
