import math
import secrets
import hashlib
import typing

import itsdangerous
import zrlog

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError

DEFAULT_PASSWORD_HASH_ITERATIONS: int = 752123
MINIMUM_ITERATIONS: int = 100000
ALGORITHM: str = 'sha512'

class SecureError(CodedError): CODE_SPACE = 'SECURE'

def validate_password(password: str | bytes | None) -> typing.TypeGuard[str]:
    if not isinstance(password, (str, bytes)):
        raise SecureError("Password must be a string or bytes", 1000)
    if not password:
        raise SecureError('No password provided', 1001)
    if len(password) > 1024:
        raise SecureError("Password is too long", 1002)
    return True

def check_expired_password(password: str | bytes, salt: bytes, password_hash: bytes, expired_date: AwareDateTime, username: str = None) -> bool:
    if expired_date > AwareDateTime.now() and check_password(password, salt, password_hash):
        zrlog.get_logger('medsutil.secure').notice('User [%s] has logged in with an expired password', username)
        return True
    return False

def check_password(password: str | bytes, salt: bytes, password_hash: bytes) -> bool:
    return secrets.compare_digest(password_hash, hash_password(password, salt))

def hash_password(password: str | bytes, salt: bytes, iterations=None) -> bytes:
    if iterations is None or iterations < MINIMUM_ITERATIONS:
        iterations = DEFAULT_PASSWORD_HASH_ITERATIONS
    return hashlib.pbkdf2_hmac(
        ALGORITHM,
        str(password or '').encode('utf-8', errors='replace') if isinstance(password, str) else password,
        salt,
        iterations
    )

PASSWORD_CHARACTERS = 'ABCDEFGHIJKLMNOPQRSTVWXYZabcdefghijklmnopqrstvwxyz2345679@#$%&'  # nosec B105 # not a hard coded password
ENTROPY = 120
PASSWORD_LENGTH = math.ceil(ENTROPY / math.log2(len(PASSWORD_CHARACTERS)))

def generate_secure_random_password() -> str:
    return ''.join(secrets.choice(PASSWORD_CHARACTERS) for _ in range(0, PASSWORD_LENGTH))

def generate_secure_key(length: int = 16) -> bytes:
    if length < 16:
        zrlog.get_logger('medsutil.secure').notice(f"Salt length is too short, minimum 128 bit required, got [%s]", length * 8)
    return secrets.token_bytes(length)

def generate_salt(length: int = 8) -> bytes:
    if length < 8:
        zrlog.get_logger('medsutil.secure').notice(f"Salt length is too short, minimum 64 bit required, got [%s]", length * 8)
    return secrets.token_bytes(length)

def validate_secret_key(key: str | bytes | None) -> typing.TypeGuard[str]:
    if key is None or key == '' or key == b'':
        raise SecureError("No secret key provided", 2000)
    return True

def generate_csp_nonce() -> str:
    return secrets.token_urlsafe(32)


from autoinject import injector
import zirconium as zr

@injector.injectable_global
class SecureOperations:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._secret_key: str = self.config.get('flask', 'SECRET_KEY', default=None)
        if not self._secret_key:
            raise SecureError("Invalid secret key provided", 9000)
        self._serializer = None
        self._timed_serializer = None

    @property
    def serializer(self):
        if self._serializer is None:
            self._serializer = itsdangerous.URLSafeSerializer(self._secret_key, salt="medsutil")
        return self._serializer

    @property
    def timed_serializer(self) -> itsdangerous.TimedSerializer:
        if self._timed_serializer is None:
            self._timed_serializer = itsdangerous.TimedSerializer(self._secret_key, salt="medsutil")
        return self._timed_serializer



