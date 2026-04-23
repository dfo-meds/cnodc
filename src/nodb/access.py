import datetime
import typing as t
import enum

import medsutil.secure as secure
import medsutil.types as ct
from medsutil.awaretime import AwareDateTime

import nodb.interface as interface
import nodb.base as s


class UserStatus(enum.Enum):
    """Status of a user in the database."""

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class NODBUser(s.NODBBaseObject):

    TABLE_NAME: str = "nodb_users"
    PRIMARY_KEYS: tuple[str] = ("username",)

    username: str = s.StringColumn()
    phash: bytes = s.ByteColumn()
    salt: bytes = s.ByteColumn()
    old_phash: bytes | None = s.ByteColumn()
    old_salt: bytes | None = s.ByteColumn()
    old_expiry: AwareDateTime | None = s.DateTimeColumn()
    status: UserStatus = s.EnumColumn(UserStatus)
    roles: set = s.JsonSetColumn()
    display: str = s.StringColumn()
    email: str = s.StringColumn()
    language_pref: str = s.StringColumn()
    locked_until: AwareDateTime = s.DateTimeColumn()

    def assign_role(self, role_name: str):
        """Assign a role to the user."""
        if role_name not in self.roles:
            self.roles.add(role_name)
            self.mark_modified('roles')
            self.clear_cache('permissions')

    def unassign_role(self, role_name: str):
        """Unassign a role from the user."""
        if role_name in self.roles:
            self.roles.remove(role_name)
            self._modified_values.add('roles')
            self.clear_cache('permissions')

    def set_password(self, new_password: str, salt_length: int = 16, old_expiry_seconds: int = 0):
        """Set the user's password."""
        secure.validate_password(new_password)
        if old_expiry_seconds > 0:
            self.old_salt = self.salt
            self.old_phash = self.phash
            self.old_expiry = AwareDateTime.now() + datetime.timedelta(seconds=old_expiry_seconds)
        self.salt = secure.generate_salt(salt_length)
        self.phash = secure.hash_password(new_password, self.salt)

    def check_password(self, password: str) -> bool:
        """Check a password to see if it is the correct one."""
        if secure.check_password(password, self.salt, self.phash):
            return True
        if self.old_phash and self.old_salt and self.old_expiry:
            return secure.check_expired_password(password, self.old_salt, self.old_phash, self.old_expiry, username=self.username)
        return False

    def cleanup(self):
        """Cleanup a user's password entry."""
        if self.old_expiry is not None and self.old_expiry <= AwareDateTime.now():
            self.old_phash = None
            self.old_salt = None
            self.old_expiry = None

    def permissions(self, db: interface.NODBInstance) -> set:
        """Retrieve a user's permissions."""
        return self._with_cache('permissions', self._permissions, db)

    def _permissions(self, db: interface.NODBInstance) -> set[str]:
        return db.load_permissions(self.roles)

    @classmethod
    def find_by_username(cls, db: interface.NODBInstance, username: str, **kwargs) -> NODBUser | None:
        """Locate a user by their username."""
        return db.load_object(cls, {"username": username}, **kwargs)


class NODBSession(s.NODBBaseObject):

    TABLE_NAME: str = "nodb_sessions"
    PRIMARY_KEYS: tuple[str] = ("session_id",)

    session_id: str = s.StringColumn()
    start_time: AwareDateTime = s.DateTimeColumn()
    expiry_time: AwareDateTime = s.DateTimeColumn()
    username: str = s.StringColumn()
    session_data: dict = s.JsonDictColumn()

    def set_session_value(self, key: str, value: ct.SupportsExtendedJson):
        """Set a session value"""
        self.session_data[key] = value
        self.mark_modified('session_data')

    def get_session_value(self, key: str, default: t.Any = None) -> t.Any:
        """Get a session value"""
        return self.session_data[key] if key in self.session_data else default

    def is_expired(self) -> bool:
        """Check if the session is expired."""
        return self.expiry_time < AwareDateTime.now()

    @classmethod
    def find_by_session_id(cls, db: interface.NODBInstance, session_id: str, **kwargs) -> NODBSession | None:
        """Locate a session by its ID."""
        return db.load_object(cls, {"session_id": session_id}, **kwargs)
