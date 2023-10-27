import typing as t
import secrets
import hashlib
import datetime
import flask
import itsdangerous

from autoinject import injector
from cnodc.nodb import NODBController
from cnodc.nodb.postgres import LockType
from cnodc.nodb.structures import NODBSession, NODBUser, UserStatus


@injector.inject
def attempt_login(username: str,
                  password: str,
                  ip_address: str,
                  shared: t.Optional[str],
                  expiry_seconds: int = 86400,
                  nodb: NODBController = None) -> dict:
    if 'SECRET_KEY' not in flask.current_app.config:
        return {'error': 'missing secret key'}
    if 'INSTANCE_NAME' not in flask.current_app.config:
        return {'error': 'missing instance name'}
    if current_user() is not None:
        return {'error': 'already logged in'}
    with nodb as db:
        user = db.load_user(username)
        if not user:
            return {'error': 'invalid username or password'}
        if not user.check_password(password):
            return {'error': 'invalid username or password'}
        session = NODBSession()
        session.session_id = secrets.token_hex(32)
        session.start_time = datetime.datetime.utcnow()
        session.expiry_time = session.start_time + datetime.timedelta(seconds=expiry_seconds)
        db.save_session(session)
        db.record_login(username, ip_address, flask.current_app.config['INSTANCE_NAME'])
        db.commit()
        serializer = itsdangerous.serializer.Serializer(flask.current_app.config['SECRET_KEY'])
        return {
            'token': serializer.dumps(session.session_id),
            'expiry': session.expiry_time.isoformat(),
            'shared': shared
        }


@injector.inject
def renew_login(shared: t.Optional[str],
                expiry_seconds: int = 86400,
                nodb: NODBController = None) -> dict:
    if 'SECRET_KEY' not in flask.current_app.config:
        return {'error': 'missing secret key'}
    if 'INSTANCE_NAME' not in flask.current_app.config:
        return {'error': 'missing instance name'}
    with nodb as db:
        session = current_session()
        session.expiry_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=expiry_seconds)
        db.save_session(session)
        db.commit()
        serializer = itsdangerous.serializer.Serializer(flask.current_app.config['SECRET_KEY'])
        return {
            'token': serializer.dumps(session.session_id),
            'expiry': session.expiry_time.isoformat(),
            'shared': shared
        }


@injector.inject
def verify_user_token(user_token, nodb: NODBController = None) -> bool:
    if 'token_checked' not in flask.g:
        with nodb as db:
            flask.g.token_checked = True
            serializer = itsdangerous.serializer.Serializer(flask.current_app.config['SECRET_KEY'])
            try:
                session_id = serializer.loads(user_token)
                flask.g.session = db.load_session(session_id, LockType.FOR_NO_KEY_UPDATE)
                if not flask.g.session:
                    return False
                if flask.g.session.expiry_time < datetime.datetime.utcnow():
                    db.delete_session(session_id)
                    db.commit()
                    return False
                flask.g.user = db.load_user(flask.g.session.username)
                if not flask.g.user:
                    return False
                flask.g.permissions = db.load_permissions(flask.g.user.roles)
            except itsdangerous.BadSignature:
                return False
    return 'user' in flask.g and flask.g.user is not None


@injector.inject
def change_password(new_password: str, nodb: NODBController = None) -> dict:
    user = current_user()
    if not user:
        return {'error': 'no user object found'}
    with nodb as db:
        user.set_password(new_password)
        db.save_user(user)
        db.commit()
    return {'success': True}


@injector.inject
def create_user(username: str, password: str, nodb: NODBController = None) -> dict:
    with nodb as db:
        existing = db.load_user(username)
        if existing:
            return {'error': 'username already exists'}
        new_user = NODBUser()
        new_user.username = username
        new_user.set_password(password)
        new_user.status = UserStatus.ACTIVE
        db.save_user(new_user)
        db.commit()
        return {'success': True}


@injector.inject
def update_user(username: str, password: str = None, is_active: bool = None, nodb: NODBController = None) -> dict:
    if is_active is None and password is None:
        return {'error': 'no values to update'}
    with nodb as db:
        existing = db.load_user(username, LockType.FOR_NO_KEY_UPDATE)
        if not existing:
            return {'error': 'no such user'}
        if password is not None:
            existing.set_password(password)
        if is_active is True:
            existing.status = UserStatus.ACTIVE
        elif is_active is False:
            existing.status = UserStatus.INACTIVE
        db.save_user(existing)
        db.commit()
        return {'success': True}


@injector.inject
def assign_role(username: str, role_name: str, nodb: NODBController = None):
    with nodb as db:
        existing = db.load_user(username, LockType.FOR_NO_KEY_UPDATE)
        if not existing:
            return {'error': 'no such user'}
        existing.assign_role(role_name)
        db.save_user(existing)
        db.commit()


@injector.inject
def unassign_role(username: str, role_name: str, nodb: NODBController = None):
    with nodb as db:
        existing = db.load_user(username, LockType.FOR_NO_KEY_UPDATE)
        if not existing:
            return {'error': 'no such user'}
        existing.unassign_role(role_name)
        db.save_user(existing)
        db.commit()


def current_user() -> t.Optional[NODBUser]:
    if verify_user_token():
        return flask.g.user
    return None


def current_permissions() -> set:
    if verify_user_token() and 'permissions' in flask.g:
        return flask.g.permissions
    return set()


def current_session() -> t.Optional[NODBSession]:
    if verify_user_token():
        return flask.g.session
    return None
