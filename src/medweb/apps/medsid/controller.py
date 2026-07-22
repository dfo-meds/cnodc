import base64
import typing as t
from types import EllipsisType

from autoinject import injector

from gcflask.i18n import TranslatableError
from medsutil.awaretime import AwareDateTime
from medsutil.sendmail import EmailController
from nodb.access import NODBUser, UserStatus, NODBAccessToken
from nodb.interface import NODB, LockType, NODBInstance
from medsutil import secure
import zirconium as zr
from urllib.parse import quote


class AccessManagementError(TranslatableError): CODE_SPACE = "ACCESS-MANAGEMENT"


@injector.injectable
@injector.construct
class AccessController:
    nodb: NODB
    smtp: EmailController
    config: zr.ApplicationConfig

    def load_user_by_id(self, user_id: int) -> NODBUser | None:
        with self.nodb as db:
            return NODBUser.find_by_identifier(db, user_id)

    def load_user_by_name(self, username: str) -> NODBUser | None:
        with self.nodb as db:
            return NODBUser.find_by_username(db, username)

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

    def create_temporary_access_token(self, username: str, password: str, lifetime_seconds: int = None) -> tuple[str, AwareDateTime]:
        with self.nodb as db:
            user = self._require_user(db, username)
            if not user.check_password(password):
                raise AccessManagementError("Invalid password", 1500)
            if user.allow_api_access == 'N':
                raise AccessManagementError("User does not have API access", 1501)
            name = secure.generate_random_token_name()
            max_iterations = 10
            while NODBAccessToken.find_by_identifier(db, user.identifier, name) is not None and max_iterations > 0:
                name = secure.generate_random_token_name()
            if max_iterations <= 0:
                raise AccessManagementError("Could not generate unique token", 1502)
            return self._create_api_key(db, user.identifier, name, lifetime_seconds)

    def remove_temporary_access_token(self, access_token: str):
        user_id, key_identifier = self._verify_access_token(access_token)
        with self.nodb as db:
            access_key = self._require_access_token(db, user_id, key_identifier)
            db.delete_object(access_key)
            db.commit()

    def renew_temporary_access_token(self, access_token: str, lifetime_seconds: int = None) -> tuple[str, AwareDateTime]:
        user_id, key_identifier = self._verify_access_token(access_token)
        with self.nodb as db:
            access_key = self._require_access_token(db, user_id, key_identifier)
            header = self._update_key(
                access_key=access_key,
                expiry_seconds=lifetime_seconds,
                old_expiry_seconds=0
            )
            db.update_object(access_key)
            db.commit()
            return header

    def create_api_key(self, username: str, key_identifier: str, expiry_days: int) -> str:
        with self.nodb as db:
            user = self._require_user(db, username)
            if user.allow_api_access == 'N':
                raise AccessManagementError("User does not have API access", 1201)
            access_key = NODBAccessToken.find_by_identifier(db, user.identifier, key_identifier)
            if access_key:
                raise AccessManagementError("Access key already exists", 1202)
            return self._create_api_key(db, user.identifier, key_identifier, expiry_days)[0]

    def update_api_key(self, username: str, key_identifier: str, is_active: bool):
        with self.nodb as db:
            user = self._require_user(db, username)
            access_key = self._require_access_token(db, user.identifier, key_identifier)
            access_key.is_active = 'Y' if is_active is True else 'N'
            db.update_object(access_key)
            db.commit()

    def rotate_api_key(self, user_id: int, key_identifier: str, expiry_days: int, leave_old_active_days: int = 0) -> str:
        with self.nodb as db:
            access_key = self._require_access_token(db, user_id, key_identifier)
            header = self._update_key(
                access_key=access_key,
                expiry_seconds=expiry_days * 60 * 60 * 24,
                old_expiry_seconds=leave_old_active_days * 60 * 60 * 24
            )
            db.update_object(access_key)
            db.commit()
            return header[0]

    def _require_access_token(self, db, user_id: int, key_identifier: str) -> NODBAccessToken:
        access_key = NODBAccessToken.find_by_identifier(db, user_id, key_identifier)
        if not access_key:
            raise AccessManagementError("Access key does not exist", 1300)
        return access_key

    def _verify_username_not_in_use(self, db, username: str, exclude_user_id: int | None = None):
        user = NODBUser.find_by_username(db, username, key_only=True)
        if user and (exclude_user_id is None or exclude_user_id != user.identifier):
            raise AccessManagementError("medsid.access.error.username_exists", 1000)

    def _verify_email_not_in_use(self, db, email: str, exclude_user_id: int | None = None):
        user = NODBUser.find_by_email(db, email, key_only=True)
        if user and (exclude_user_id is None or exclude_user_id != user.identifier):
            raise AccessManagementError("medsid.access.error.email_exists", 1001)

    def _verify_access_token(self, access_token: str) -> tuple[int, str]:
        token_pieces = access_token.split(".")
        if len(token_pieces) != 4:
            raise AccessManagementError("Invalid access token", 1600)
        if token_pieces[0] != "api":
            raise AccessManagementError("Invalid access token, bad type", 1601)
        if not token_pieces[1].isdigit():
            raise AccessManagementError("Invalid access token, bad user id", 1602)
        return int(token_pieces[0]), token_pieces[1]

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

    def _create_api_key(self, db, user_id: int, key_identifier: str, expiry_seconds: int | None = None) -> tuple[str, AwareDateTime]:
        access_key = NODBAccessToken(
            user_id=user_id,
            identifier=key_identifier,
            is_active='Y'
        )
        header = self._update_key(access_key, expiry_seconds)
        db.insert_object(access_key)
        db.commit()
        return header

    def _update_key(self, access_key: NODBAccessToken, expiry_seconds: int | None = None, old_expiry_seconds: int | None = None) -> tuple[str, AwareDateTime]:
        if expiry_seconds is None:
            expiry_seconds = self.config.as_int(("medsid", "default_access_key_expiry_seconds"), default=3600)
        if old_expiry_seconds is None:
            old_expiry_seconds = self.config.as_int(("medsid", "default_old_access_key_expiry_seconds"), default=0)
        api_key = secure.generate_secure_key()
        access_key.set_key(api_key, t.cast(int, expiry_seconds), t.cast(int, old_expiry_seconds))
        return ".".join((
            "api",
            str(access_key.user_id),
            quote(access_key.identifier),
            base64.b64encode(api_key).decode('ascii')
        )), t.cast(AwareDateTime, access_key.expiry)

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
