import base64
import typing as t
from types import EllipsisType

from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.email import EmailController
from nodb.access import NODBUser, UserStatus, NODBAccessToken
from nodb.interface import NODB, LockType
from medsutil.exceptions import CodedError
from medsutil import secure
from urllib.parse import quote


class AccessManagementError(CodedError): CODE_SPACE = "ACCESS-MANAGEMENT"


@injector.construct
class AccessController:
    nodb: NODB
    smtp: EmailController

    def create_user(self,
                    username: str,
                    password: str | None = None,
                    email: str | None = None,
                    display_name: str | None = None,
                    language_pref: str = 'und'):
        if not email:
            email = f"{username}@"
        with self.nodb as db:
            user = NODBUser.find_by_username(db, username)
            if user:
                raise AccessManagementError("User already exists", 1000)
            user = NODBUser.find_by_email(db, email)
            if user:
                raise AccessManagementError("Email already exists", 1001)
            new_user = NODBUser(
                username=username,
                display=display_name or username,
                email=email,
                language_pref=language_pref
            )
            self._set_password(new_user, password)
            db.insert(new_user)
            db.commit()

    def _set_password(self, user: NODBUser, password: str | None, by_user: bool = False):
        if password is None:
            password = secure.generate_secure_random_password()
        else:
            secure.validate_password(password)
        user.set_password(t.cast(str, password))
        if (not by_user) and user.email:
            self.smtp.send_template(
                template_name="password_update" if not user.is_new else "new_account",
                template_lang=user.language_pref or "en",
                to_emails=[user.email],
            )

    def update_user(self,
                    username: str,
                    password: str | None | EllipsisType = ...,
                    locked_time: AwareDateTime | None | EllipsisType = ...,
                    api_access: bool | EllipsisType = ...,
                    enabled: bool | EllipsisType = ...,
                    change_by_user: bool = False):
        with self.nodb as db:
            user = NODBUser.find_by_username(db, username, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not user:
                raise AccessManagementError("User does not exist", 1100)
            if password is not ...:
                self._set_password(user, t.cast(str | None, password), change_by_user)
            if locked_time is not ...:
                user.locked_until = t.cast(AwareDateTime | None, locked_time)
            if api_access is not ...:
                user.allow_api_access = 'Y' if api_access is True else 'N'
            if enabled is not ...:
                user.status = UserStatus.ACTIVE if enabled is True else UserStatus.INACTIVE
            db.update_object(user)
            db.commit()

    def create_api_key(self, username: str, key_identifier: str, expiry_days: int) -> str:
        with self.nodb as db:
            user = NODBUser.find_by_username(db, username)
            if not user:
                raise AccessManagementError("User does not exist", 1200)
            if user.allow_api_access == 'N':
                raise AccessManagementError("User does not have API access", 1201)
            access_key = NODBAccessToken.find_by_identifier(db, username, key_identifier)
            if access_key:
                raise AccessManagementError("Access key already exists", 1202)
            access_key = NODBAccessToken(
                username=username,
                identifier=key_identifier,
                is_active='Y'
            )
            header = self._update_key(access_key, username, key_identifier, expiry_days)
            db.insert_object(access_key)
            db.commit()
            return header

    def update_api_key(self, username: str, key_identifier: str, is_active: bool) -> str:
        with self.nodb as db:
            access_key = NODBAccessToken.find_by_identifier(db, username, key_identifier)
            if not access_key:
                raise AccessManagementError("Access key already exists", 1400)
            access_key.is_active = 'Y' if is_active is True else 'N'
            db.update_object(access_key)
            db.commit()


    def _update_key(self, access_key: NODBAccessToken, username: str, key_identifier: str, expiry_days: int, old_expiry_days: int = 0) -> str:
            api_key = secure.generate_secure_key()
            access_key.set_key(api_key, expiry_days * 3600 * 24, old_expiry_days * 3600 * 24)
            return ".".join((
                "api",
                quote(username),
                quote(key_identifier),
                base64.b64encode(api_key).decode('ascii')
            ))

    def rotate_api_key(self, username: str, key_identifier: str, expiry_days: int, leave_old_active_days: int = 0) -> str:
        with self.nodb as db:
            access_key = NODBAccessToken.find_by_identifier(db, username, key_identifier)
            if not access_key:
                raise AccessManagementError("Access key already exists", 1300)
            header = self._update_key(access_key, username, key_identifier, expiry_days, leave_old_active_days)
            db.update_object(access_key)
            db.commit()
            return header


