""" Provides secure functions using the secrets library. """
import math
import secrets
import hashlib
import typing

import itsdangerous
import zrlog
from autoinject import injector, auto
import zirconium as zr

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError


DEFAULT_PASSWORD_HASH_ITERATIONS: int = 752123
MINIMUM_ITERATIONS: int = 100000
ALGORITHM: str = 'sha512'
PASSWORD_CHARACTERS = 'ABCDEFGHIJKLMNOPQRSTVWXYZabcdefghijklmnopqrstvwxyz2345679@#$%&'  # nosec B105 # not a hard coded password
ENTROPY = 120
PASSWORD_LENGTH = math.ceil(ENTROPY / math.log2(len(PASSWORD_CHARACTERS)))


class SecureError(CodedError): CODE_SPACE = 'SECURE'


def validate_password(password: str | bytes | None) -> typing.TypeGuard[str | bytes]:
    """ Validates that the password is appropriate as a password.

        :raises SecureError: If the password is not an appropriate type (1000)
        :raises SecureError: If the password is empty (1001)
        :raises SecureError: If the password is too long (1002)
    """
    if not isinstance(password, (str, bytes)):
        raise SecureError("Password must be a string or bytes", 1000)
    if not password:
        raise SecureError('No password provided', 1001)
    if len(password) > 1024:
        raise SecureError("Password is too long", 1002)
    return True


def check_expired_password(password: str | bytes,
                           salt: bytes,
                           password_hash: bytes,
                           expired_date: AwareDateTime,
                           username: str = None) -> bool:
    """
        Checks an expired password if it is still valid.

        :param password: The password to check
        :param salt: The salt used for the password
        :param password_hash: The stored hash of the password
        :param expired_date: The date the password expired
        :param username: The user's username (for logging)
        :returns: If the password is valid and not expired.
    """
    if expired_date > AwareDateTime.now() and check_password(password, salt, password_hash):
        zrlog.get_logger('medsutil.secure').notice('User [%s] has logged in with an expired password', username)
        return True
    return False


def check_password(password: str | bytes, salt: bytes, password_hash: bytes) -> bool:
    """ Checks if a password matches the given hash. """
    try:
        return secrets.compare_digest(password_hash, hash_password(password, salt))
    except SecureError:
        zrlog.get_logger("medsutil.secure").exception("Error while hashing password")
        return False


def hash_password(password: str | bytes, salt: bytes, iterations: int | None = None) -> bytes:
    """ Securely hashes the password.
        :raises SecureError: If there is an error while hashing the password (3000)
    """
    try:
        iteration_count = DEFAULT_PASSWORD_HASH_ITERATIONS if iterations is None or iterations < MINIMUM_ITERATIONS else iterations
        return hashlib.pbkdf2_hmac(
            ALGORITHM,
            password.encode('utf-8', errors='replace') if isinstance(password, str) else password,
            salt,
            iteration_count
        )
    except Exception as ex:
        raise SecureError("Could not hash password", 3000) from ex


def generate_secure_random_password() -> str:
    """ Generates a secure random password. """
    return ''.join(secrets.choice(PASSWORD_CHARACTERS) for _ in range(0, PASSWORD_LENGTH))


def generate_secure_key(length: int = 16) -> bytes:
    """ Generates a secure key. """
    if length < 16:
        zrlog.get_logger('medsutil.secure').notice(f"Salt length is too short, minimum 128 bit required, got [%s]", length * 8)
    return secrets.token_bytes(length)


def generate_salt(length: int = 8) -> bytes:
    """ Generates a secure random salt. """
    if length < 8:
        zrlog.get_logger('medsutil.secure').notice(f"Salt length is too short, minimum 64 bit required, got [%s]", length * 8)
    return secrets.token_bytes(length)


def validate_secret_key(key: str | bytes | None) -> typing.TypeGuard[str | bytes]:
    """ Validates that a secret key is appropriate.
        :raises SecureError: If no secret key is provided (2000)
    """
    if key is None or key == '' or key == b'':
        raise SecureError("No secret key provided", 2000)
    return True


def generate_csp_nonce() -> str:
    """ Generates a secure random nonce for a Content-Security-Policy. """
    return secrets.token_urlsafe(32)


def generate_random_token_name(length: int = 8) -> str:
    """ Generates a random token name. """
    return secrets.token_urlsafe(length)


@injector.injectable_global
class SecureOperations:
    """ Provides secure operations (mostly a serializer). """

    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self):
        """
        :raises SecureError: If no secret key is provided (9000)
        """
        self._secret_key: str = self.config.get('flask', 'SECRET_KEY', default=None)
        if not self._secret_key:
            raise SecureError("Invalid secret key provided", 9000)
        self._serializer = None
        self._timed_serializer = None

    @property
    def serializer(self) -> itsdangerous.URLSafeSerializer:
        if self._serializer is None:
            self._serializer = itsdangerous.URLSafeSerializer(self._secret_key, salt="medsutil")
        return self._serializer

    @property
    def timed_serializer(self) -> itsdangerous.TimedSerializer:
        if self._timed_serializer is None:
            self._timed_serializer = itsdangerous.TimedSerializer(self._secret_key, salt="medsutil")
        return self._timed_serializer
