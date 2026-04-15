import math
import secrets
import hashlib
import typing

import zrlog

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError

DEFAULT_PASSWORD_HASH_ITERATIONS: int = 752123

logger = zrlog.get_logger('medsutil.secure')

class SecureError(CodedError): CODE_SPACE = 'SECURE'

def validate_password(password: str | None) -> typing.TypeGuard[str]:
    if not isinstance(password, str):
        raise SecureError("Password must be a string", 1000)
    if password == '':
        raise SecureError('No password provided', 1001)
    if len(password) > 1024:
        raise SecureError("Password is too long", 1002)
    return True

def check_expired_password(password: str, salt: bytes, password_hash: bytes, expired_date: AwareDateTime, username: str = None) -> bool:
    if expired_date > AwareDateTime.now() and check_password(password, salt, password_hash):
        logger.notice('User [%s] has logged in with an expired password', username)
        return True
    return False

def check_password(password: str, salt: bytes, password_hash: bytes) -> bool:
    return secrets.compare_digest(password_hash, hash_password(password, salt))

def hash_password(password: str, salt: bytes, iterations=None) -> bytes:
    return hashlib.pbkdf2_hmac(
        'sha512',
        password.encode('utf-8', errors='replace'),
        salt,
        iterations if iterations > 100000 else DEFAULT_PASSWORD_HASH_ITERATIONS
    )

PASSWORD_CHARACTERS = 'ABCDEFGHIJKLMNOPQRSTVWXYZabcdefghijklmnopqrstvwxyz2345679@#$%&'
ENTROPY = 80
PASSWORD_LENGTH = math.ceil(ENTROPY / math.log2(len(PASSWORD_CHARACTERS)))

def generate_secure_random_password() -> str:
    return ''.join(secrets.choice(PASSWORD_CHARACTERS) for _ in range(0, PASSWORD_LENGTH))

def generate_salt(length: int = 8) -> bytes:
    if length < 8:
        logger.notice(f"Salt length is too short, minimum 64 bit required, got [%s]", length * 8)
    return secrets.token_bytes(length)

def validate_secret_key(key: str | None) -> typing.TypeGuard[str]:
    if key is None or key == '':
        raise SecureError("No secret key provided", 2000)
    return True
