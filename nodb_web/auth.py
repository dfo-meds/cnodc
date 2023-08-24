import typing as t
import secrets
import hashlib
import datetime

import flask
import itsdangerous
from autoinject import injector
from cnodc.nodb import NODBDatabaseProtocol
from cnodc.nodb.proto import LockMode
from cnodc.nodb.structures import NODBSession, NODBUser


@injector.inject
def attempt_login(username: str,
                  password: str,
                  ip_address: str,
                  shared: t.Optional[str],
                  expiry_seconds: int = 86400,
                  nodb: NODBDatabaseProtocol = None) -> dict:
    user = nodb.load_user(username, tx=flask.g.tx)
    if not user:
        return {
            'error': 'no such user or bad password'
        }
    phash = _hash_password(password, user.salt)
    if not secrets.compare_digest(phash, user.phash):
        return {
            'error': 'no such user or bad password'
        }
    session = NODBSession()
    session.session_id = secrets.token_hex(32)
    session.start_time = datetime.datetime.utcnow()
    session.expiry_time = session.start_time + datetime.timedelta(seconds=expiry_seconds)
    nodb.save_session(session, tx=flask.g.tx)
    nodb.record_login(username, ip_address, tx=flask.g.tx)
    flask.g.tx.commit()
    serializer = itsdangerous.serializer.Serializer(flask.current_app.config['SECRET_KEY'])
    return {
        'token': serializer.dumps(session.session_id),
        'expiry': session.expiry_time.isoformat(),
        'shared': shared
    }


@injector.inject
def verify_user_token(user_token, nodb: NODBDatabaseProtocol = None) -> bool:
    if 'token_checked' not in flask.g:
        flask.g.token_checked = True
        serializer = itsdangerous.serializer.Serializer(flask.current_app.config['SECRET_KEY'])
        try:
            session_id = serializer.loads(user_token)
            session = nodb.load_session(session_id, with_lock=LockMode.FOR_NO_KEY_UPDATE, tx=flask.g.tx)
            if not session:
                return False
            user = nodb.load_user(session.username, with_lock=LockMode.FOR_NO_KEY_UPDATE, tx=flask.g.tx)
            if not user:
                return False
            flask.g.user = user
            flask.g.session = session
        except itsdangerous.BadSignature:
            return False
    return 'user' in flask.g


@injector.inject
def change_password(new_password: str, nodb: NODBDatabaseProtocol = None) -> dict:
    user = current_user()
    if not user:
        return {'error': 'no user object found'}
    user.salt = secrets.token_bytes(16)
    user.phash = _hash_password(new_password, user.salt)
    nodb.save_user(user, tx=flask.g.tx)
    flask.g.tx.commit()
    return {'success': True}


def current_user() -> t.Optional[NODBUser]:
    if 'user' in flask.g:
        return flask.g.user
    return None


def _hash_password(password: str, salt: bytes, iterations=752123) -> bytes:
    return hashlib.pbkdf2_hmac('sha512', password, salt, iterations)


