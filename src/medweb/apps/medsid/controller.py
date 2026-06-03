import base64
import typing as t
from types import EllipsisType

from autoinject import injector

from gcflask.i18n import TranslatableError
from medsutil.awaretime import AwareDateTime
from medsutil.email import EmailController
from nodb.access import NODBUser, UserStatus, NODBAccessToken
from nodb.interface import NODB, LockType, NODBInstance
from medsutil import secure
from urllib.parse import quote


class AccessManagementError(TranslatableError): CODE_SPACE = "ACCESS-MANAGEMENT"


@injector.injectable
@injector.construct
class AccessController:
    nodb: NODB
    smtp: EmailController

    def load_user_by_id(self, user_id: int) -> NODBUser | None:
        with self.nodb as db:
            return NODBUser.find_by_identifier(db, user_id)

    def load_user_by_name(self, username: str) -> NODBUser | None:
        with self.nodb as db:
            return NODBUser.find_by_username(db, username)

    def _verify_username_not_in_use(self, db, username: str, exclude_user_id: int | None = None):
        user = NODBUser.find_by_username(db, username, key_only=True)
        if user and (exclude_user_id is None or exclude_user_id != user.identifier):
            raise AccessManagementError("medsid.access.error.username_exists", 1000)

    def _verify_email_not_in_use(self, db, email: str, exclude_user_id: int | None = None):
        user = NODBUser.find_by_email(db, email, key_only=True)
        if user and (exclude_user_id is None or exclude_user_id != user.identifier):
            raise AccessManagementError("medsid.access.error.email_exists", 1001)

    def _require_user(self, db: NODBInstance, username: str | EllipsisType = ..., user_id: int | None = None) -> NODBUser:
            user = None
            if user_id is not None:
                user = NODBUser.find_by_identifier(db, user_id, lock_type=LockType.FOR_NO_KEY_UPDATE)
            elif username is not ...:
                user = NODBUser.find_by_username(db, t.cast(str, username), lock_type=LockType.FOR_NO_KEY_UPDATE)
            if not user:
                raise AccessManagementError("User does not exist", 1100)
            return user

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

    def create_user(self,
                    username: str,
                    password: str | None = None,
                    email: str | None = None,
                    display_name: str | None = None,
                    allow_api_access: bool = False,
                    status: str = "active",
                    language_pref: str = 'en'):
        if not username:
            raise AccessManagementError("medsid.access.error.username_missing", 1400)
        if language_pref not in ("en", "fr"):
            raise AccessManagementError("medsid.access.error.invalid_language_pref", 1401)
        if status not in ("active", "inactive"):
            raise AccessManagementError("medsid.access.error.invalid_status", 1402)
        if not email:
            email = f"{username}@"

        with self.nodb as db:
            self._verify_username_not_in_use(db, username)
            self._verify_email_not_in_use(db, t.cast(str, email))
            new_user = NODBUser(
                username=username,
                display=display_name or username,
                email=email,
                language_pref=language_pref,
                allow_api_access='Y' if allow_api_access else 'N',
                status=UserStatus(status.upper())
            )
            self._set_password(new_user, password)
            db.insert_object(new_user)
            db.commit()

    def update_user(self,
                    username: str | EllipsisType = ...,
                    password: str | None | EllipsisType = ...,
                    display: str | EllipsisType = ...,
                    locked_time: AwareDateTime | None | EllipsisType = ...,
                    api_access: bool | EllipsisType = ...,
                    email: str | EllipsisType = ...,
                    enabled: bool | EllipsisType = ...,
                    language_pref: str | EllipsisType = ...,
                    user_id: int | None = None,
                    change_by_user: bool = False):
        with self.nodb as db:
            user = self._require_user(db, username, user_id)
            if user_id is not None and username is not ...:
                self._verify_username_not_in_use(db, t.cast(str, username), user.identifier)
                user.username = t.cast(str, username)
            if email is not ...:
                self._verify_email_not_in_use(db, t.cast(str, email), user.identifier)
                user.email = t.cast(str, email)
            if display is not ...:
                user.display = t.cast(str, display)
            if password is not ...:
                self._set_password(user, t.cast(str | None, password), change_by_user)
            if locked_time is not ...:
                user.locked_until = t.cast(AwareDateTime | None, locked_time)
            if api_access is not ...:
                user.allow_api_access = 'Y' if api_access is True else 'N'
            if enabled is not ...:
                user.status = UserStatus.ACTIVE if enabled is True else UserStatus.INACTIVE
            if language_pref is not ...:
                user.language_pref = t.cast(str, language_pref)
            db.update_object(user)
            db.commit()

    def create_api_key(self, username: str, key_identifier: str, expiry_days: int) -> str:
        with self.nodb as db:
            user = self._require_user(db, username)
            if user.allow_api_access == 'N':
                raise AccessManagementError("User does not have API access", 1201)
            access_key = NODBAccessToken.find_by_identifier(db, user.identifier, key_identifier)
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
            user = self._require_user(db, username)
            access_key = NODBAccessToken.find_by_identifier(db, user.identifier, key_identifier)
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

    def assign_role(self, username: str, role_name: str):
        with self.nodb as db:
            user = self._require_user(db, username)
            user.assign_role(db, role_name)
            db.commit()

    def unassign_role(self, username: str, role_name: str):
        with self.nodb as db:
            user = self._require_user(db, username)
            user.assign_role(db, role_name)
            db.commit()

    def grant_permission(self, role_name: str, permission_name: str):
        with self.nodb as db:
            db.grant_permission(role_name, permission_name)
            db.commit()

    def remove_permission(self, role_name: str, permission_name: str):
        with self.nodb as db:
            db.remove_permission(role_name, permission_name)
            db.commit()
