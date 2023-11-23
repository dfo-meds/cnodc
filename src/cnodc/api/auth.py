import typing as t
import flask
import secrets
import datetime
import threading
import zrlog

import itsdangerous
import zirconium as zr
from cnodc.nodb import NODBController, LockType
from autoinject import injector
from cnodc.util import CNODCError
import cnodc.nodb.structures as structures


@injector.injectable_global
class LoginController:

    nodb: NODBController = None

    @injector.construct
    def __init__(self):
        self._serializer = None
        self._serializer_lock = threading.Lock()
        self._logger = zrlog.get_logger("cnodc.loginctrl")

    def do_login(self, username: str, password: str) -> structures.NODBSession:
        if not flask.has_request_context():
            self._logger.error(f"Login failed for user [{username}], no request context")
            raise CNODCError("Login only available in request context", "LOGINCTRL", 1000)
        flask_config = flask.current_app.config
        if 'INSTANCE_NAME' not in flask_config:
            self._logger.error(f"Login failed for user [{username}], no instance name")
            raise CNODCError("Missing instance name", "LOGINCTRL", 1001)
        if self.current_user() is not None:
            self._logger.error(f"Login failed for user [{username}], another user is already logged in")
            raise CNODCError("Already logged in", "LOGINCTRL", 1002)
        session_time = self._get_session_time()
        with self.nodb as db:
            user: structures.NODBUser = structures.NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not user:
                self._logger.error(f"Login failed for user [{username}], invalid username")
                raise CNODCError(f"Invalid username or password", "LOGINCTRL", 1003)
            if not user.check_password(password):
                self._logger.error(f"Login failed for user [{username}], invalid password")
                raise CNODCError(f"Invalid username or password", "LOGINCTRL", 1003)
            user.cleanup()
            session = structures.NODBSession()
            session.username = user.username
            session.session_id = secrets.token_hex(32)
            session.start_time = datetime.datetime.now(datetime.timezone.utc)
            session.expiry_time = session.start_time + datetime.timedelta(seconds=session_time)
            db.upsert_object(user)
            db.upsert_object(session)
            db.record_login(
                username,
                flask.request.remote_addr,
                flask_config.get('INSTANCE_NAME')
            )
            db.commit()
            self._logger.notice(f"User [{username}] logged in")
            return session

    def renew_session(self) -> structures.NODBSession:
        if not flask.has_request_context():
            self._logger.error(f"Renewal for current user failed, no request context")
            raise CNODCError("Session renewal only available in request context", "LOGINCTRL", 1006)
        session_time = self._get_session_time()
        with self.nodb as db:
            session = self.current_session()
            if session is None:
                raise CNODCError("No session available", "LOGINCTRL", 1004)
            session.expiry_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=session_time)
            db.save_session(session)
            db.commit()
            self._logger.notice(f"User session renewed")
            return session

    def _get_session_time(self) -> int:
        session_time = flask.current_app.config.get('PERMANENT_SESSION_LIFETIME')
        if session_time < 1:
            self._logger.warning(f"Session time is configured to be less than 1")
            session_time = 86400
        return int(session_time)

    def generate_token(self, session: structures.NODBSession):
        serializer = self._get_serializer()
        return serializer.dumps(session.session_id)

    def _get_serializer(self) -> itsdangerous.Serializer:
        if not flask.current_app.config.get('SECRET_KEY'):
            self._logger.error("Secret key is not defined properly")
            raise CNODCError('Missing secret key', 'LOGINCTRL', 1005)
        if self._serializer is None:
            with self._serializer_lock:
                if self._serializer is None:
                    self._serializer = itsdangerous.Serializer(flask.current_app.config['SECRET_KEY'])
        return self._serializer

    def verify_token(self) -> bool:
        if not flask.has_request_context():
            return False
        if 'token_checked' not in flask.g:
            flask.g.session = None
            flask.g.user = None
            flask.g.permissions = set()
            flask.g.token_checked = True
            auth_info = flask.request.headers.get('Authorization', None)
            if auth_info is None:
                self._logger.error("Token verification failed, no authorization header present")
                return False
            if ' ' not in auth_info:
                self._logger.error("Token verification failed, invalid format for authorization header")
                return False
            auth_type, user_token = auth_info.split(' ', maxsplit=1)
            if auth_type.lower() != 'bearer':
                self._logger.error("Token verification failed, invalid authorization type")
                return False
            serializer = self._get_serializer()
            try:
                session_id = serializer.loads(user_token)
                return self._load_session(session_id)
            except itsdangerous.BadSignature:
                self._logger.error("Token verification failed, bad token signature")
                return False
        else:
            return 'user' in flask.g and flask.g.user is not None

    def _load_session(self, session_id: str):
        with self.nodb as db:
            flask.g.session = structures.NODBSession.find_by_session_id(
                db,
                session_id,
                lock_type=LockType.FOR_NO_KEY_UPDATE
            )
            if flask.g.session is None:
                return False
            if flask.g.session.is_expired():
                flask.g.session = None
                db.delete_session(session_id)
                db.commit()
                return False
            flask.g.user = structures.NODBUser.find_by_username(
                db,
                flask.g.session.username
            )
            if flask.g.user is None:
                flask.g.session = None
                db.delete_session(session_id)
                db.commit()
                return False
            flask.g.permissions = db.load_permissions(flask.g.user.roles)
            return True

    def current_session(self) -> t.Optional[structures.NODBSession]:
        if flask.has_request_context() and self.verify_token():
            return flask.g.session
        return None

    def current_user(self) -> t.Optional[structures.NODBUser]:
        if flask.has_request_context() and self.verify_token():
            return flask.g.user
        return None

    def current_permissions(self) -> set:
        if flask.has_request_context() and self.verify_token():
            return flask.g.permissions
        return set()


class UserController:

    nodb: NODBController = None
    login: LoginController = None

    @injector.construct
    def __init__(self):
        pass

    def change_password(self, password: str):
        user = self.login.current_user()
        if user is None:
            raise CNODCError(
                "Cannot change password, no logged in user",
                "USERCTRL",
                1000
            )
        with self.nodb as db:
            # Regain lock for password update
            user = structures.NODBUser.find_by_username(db, user.username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            user.set_password(password)
            db.upsert_object(user)
            db.commit()

    def create_user(self, username: str, password: str):
        with self.nodb as db:
            existing = structures.NODBUser.find_by_username(db, username)
            if existing:
                raise CNODCError(
                    "Cannot create user, username already exists",
                    "USERCTRL",
                    1001
                )
            new_user = structures.NODBUser()
            new_user.username = username
            new_user.set_password(password)
            new_user.status = structures.UserStatus.ACTIVE
            db.upsert_object(new_user)
            db.commit()

    def update_user(self, username: str, password: str = None, old_expiry_seconds: int = 0, is_active: bool = None):
        if is_active is None and password is None:
            raise CNODCError(
                "No user properties to update",
                "USERNAME",
                1002
            )
        with self.nodb as db:
            existing: structures.NODBUser = structures.NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not existing:
                raise CNODCError(
                    "Cannot update user, no such user",
                    "USERCTRL",
                    1003
                )
            if password is not None:
                existing.set_password(password, old_expiry_seconds=old_expiry_seconds)
            if is_active is True:
                existing.status = structures.UserStatus.ACTIVE
            elif is_active is False:
                existing.status = structures.UserStatus.INACTIVE
            db.upsert_object(existing)
            db.commit()

    def assign_role(self, username: str, role_name: str):
        with self.nodb as db:
            existing: structures.NODBUser = structures.NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not existing:
                raise CNODCError(
                    "Cannot assign user to role, no such user",
                    "USERCTRL",
                    1004
                )
            existing.assign_role(role_name)
            db.upsert_object(existing)
            db.commit()

    def unassign_role(self, username: str, role_name: str):
        with self.nodb as db:
            existing: structures.NODBUser = structures.NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not existing:
                raise CNODCError(
                    "Cannot remove user from role, no such user",
                    "USERCTRL",
                    1005
                )
            existing.unassign_role(role_name)
            db.upsert_object(existing)
            db.commit()







