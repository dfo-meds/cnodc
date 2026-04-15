import datetime
import secrets
import threading
import typing as t

import itsdangerous
import zirconium
import zrlog
from autoinject import injector

from medsutil import awaretime as awaretime, secure
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from nodb import NODBSession, NODBUser, LockType, UserStatus
from nodb.interface import NODB, NODBInstance


class UserError(CodedError): CODE_SPACE = 'USERCTRL'

class UserController:

    nodb: NODB = None
    config: zirconium.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._serializer = None
        self._serializer_lock = threading.Lock()
        self._logger = zrlog.get_logger('cnodc.user_controller')

    def login(self, username: str, password: str, session_length: int, remote_addr: str | None, instance_name: str) -> NODBSession:
        with self.nodb as db:
            user = self._load_user(db, username)
            if not user.check_password(password):
                self._logger.error(f"Login failed for user [{username}], invalid password")
                raise UserError(f"Invalid username or password", 2300)
            user.cleanup()
            session = NODBSession()
            session.username = user.username
            session.session_id = secrets.token_hex(32)
            session.start_time = awaretime.utc_now()
            session.expiry_time = session.start_time + datetime.timedelta(seconds=session_length)
            db.upsert_object(user)
            db.upsert_object(session)
            db.record_login(username, remote_addr, instance_name)
            db.commit()
            self._logger.notice(f"User [{username}] logged in")
            return session

    def get_serializer(self) -> itsdangerous.Serializer:
        secret_key = self.config.as_str(('flask', 'SECRET_KEY'), '')
        secure.validate_secret_key(secret_key)
        if self._serializer is None:
            with self._serializer_lock:
                if self._serializer is None:
                    self._serializer = itsdangerous.Serializer(t.cast(str, secret_key))
        return self._serializer

    def get_session_token(self, session: NODBSession) -> str:
        serializer = self.get_serializer()
        return serializer.dumps(session.session_id)

    def update_session_expiry(self, session: NODBSession, new_expiry: AwareDateTime) -> NODBSession:
        with self.nodb as db:
            existing = self._load_session(db, session.session_id)
            existing.expiry = new_expiry
            db.update_object(existing)
            db.commit()
            return existing

    def verify_auth_header(self, auth_info) -> str | None:
        if auth_info is None:
            self._logger.error("Token verification failed, no authorization header present")
            return None
        if ' ' not in auth_info:
            self._logger.error("Token verification failed, invalid format for authorization header")
            return None
        auth_type, user_token = auth_info.split(' ', maxsplit=1)
        if auth_type.lower() != 'bearer':
            self._logger.error("Token verification failed, invalid authorization type")
            return None
        serializer = self.get_serializer()
        try:
            return serializer.loads(user_token)
        except itsdangerous.BadSignature:
            self._logger.error("Token verification failed, bad token signature")

    def _load_user(self, db: NODBInstance, username: str) -> NODBUser:
        existing: NODBUser | None = NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
        if not existing:
            raise UserError("No such user", 2000)
        return existing

    def _load_session(self, db: NODBInstance, session_id: str) -> NODBSession:
        existing: NODBSession | None = NODBSession.find_by_session_id(db, session_id, lock_type=LockType.FOR_NO_KEY_UPDATE)
        if not existing:
            raise UserError("No such user", 2100)
        return existing

    def destroy_session(self, session: NODBSession):
        with self.nodb as db:
            db.delete_session(session.session_id)
            db.commit()
            self._logger.info('User session terminated')

    def load_session_info(self, session_id: str | None) -> tuple[NODBSession | None, NODBUser | None, set[str] | None]:
        if session_id is None:
            return None, None, None
        with self.nodb as db:
            session = NODBSession.find_by_session_id(db, session_id, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if session is None:
                return None, None, None
            if session.is_expired():
                db.delete_session(session_id)
                db.commit()
                return None, None, None
            user = NODBUser.find_by_username(db, session.username)
            if user is None:
                db.delete_session(session_id)
                db.commit()
                return None, None, None
            permissions = user.permissions(db) or set()
            return session, user, permissions

    def create_user(self, username: str, password: str):
        with self.nodb as db:
            existing: NODBUser | None = NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if existing is not None:
                raise UserError('User already exists', 2200)
            new_user = NODBUser()
            new_user.username = username
            new_user.set_password(password)
            new_user.status = UserStatus.ACTIVE
            db.upsert_object(new_user)
            db.commit()

    def update_user(self, username: str, password: str = None, old_expiry_seconds: int = 0, is_active: bool | None = None):
        if is_active is None and password is None:
            return
        with self.nodb as db:
            existing = self._load_user(db, username)
            if password is not None:
                existing.set_password(password, old_expiry_seconds=old_expiry_seconds)
            if is_active is not None:
                existing.status = UserStatus.ACTIVE if is_active else UserStatus.INACTIVE
            db.upsert_object(existing)
            db.commit()

    def assign_role(self, username: str, role_name: str):
        with self.nodb as db:
            existing = self._load_user(db, username)
            existing.assign_role(role_name)
            db.upsert_object(existing)
            db.commit()

    def unassign_role(self, username: str, role_name: str):
        with self.nodb as db:
            existing = self._load_user(db, username)
            existing.unassign_role(role_name)
            db.upsert_object(existing)
            db.commit()

    def grant_permission(self, role_name: str, permission_name: str):
        with self.nodb as db:
            db.grant_permission(role_name, permission_name)
            db.commit()

    def remove_permission(self, role_name: str, permission_name: str):
        with self.nodb as db:
            db.remove_permission(role_name, permission_name)
            db.commit()
