import base64
import binascii
from urllib.parse import unquote

import flask
from autoinject import injector
import zirconium as zr


from gcflask.auth import AuthenticationHandler, AuthError
import gcflask.forms as forms
from gcflask.user import AuthenticatedUser
from gcflask.util import flasht
from nodb.access import NODBUser, NODBAccessToken
from nodb.interface import NODB


class LocalMedsIDHandler(AuthenticationHandler):

    nodb: NODB = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, handler_name: str, **kwargs):
        super().__init__(handler_name, True, **kwargs)
        self._max_failures = self.config.get('medsid', 'max_login_failures', default=0)
        self._max_failure_window_seconds = self.config.get('medsid', 'max_login_failure_window_seconds', default=5 * 60)
        self._user_lockout_time_seconds = self.config.get('medsid', 'user_lockout_time_seconds', default=3600)

    def login_page(self):
        form = LoginForm()
        if form.validate_on_submit():
            user = self._attempt_login_from_password(form.username.data or '', form.password.data or '')
            if user:
                return self._auth_manager.login_user(user, self._handler_name)
            else:
                flasht("medsid.auth.page.form_login.error", "error")
        return flask.render_template('form.html', form=form)

    def _create_login_record(self,
                             success: bool,
                             username: str | None = None,
                             from_api: bool = False,
                             error_message: str | None = None,
                             lockable: bool = False):
        with self.nodb as db:
            db.record_login(
                username=username,
                success=success,
                from_api=from_api,
                message=error_message,
                remote_addr=flask.request.remote_addr if flask.has_request_context() else None,
                max_failure_window_seconds=self._max_failure_window_seconds
            )
            db.commit()

    def load_user(self, user_id: int):
        with self.nodb as db:
            user = NODBUser.find_by_identifier(db, user_id)
            if user:
                return self._build_user(db, user)
            return None

    def _attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        with self.nodb as db:
            user = NODBUser.find_by_username(db, username)
            if user is None:
                raise AuthError("Invalid username", 8200, is_lockable=False, username=username)
            elif not user.can_login():
                raise AuthError("Invalid user, locked", 8202, is_lockable=False, username=username)
            elif not user.check_password(password):
                raise AuthError("Invalid password", 8201, is_lockable=True, username=username)
            return self._build_user(db, user)

    def _attempt_login_from_token(self, token: str) -> AuthenticatedUser | None:
        pieces = token.split(".")
        if pieces[0] == "api":
            if len(pieces) != 4:
                raise AuthError("Invalid token format", 8000, create_record=False)
            return self._attempt_login_from_api_token(unquote(pieces[1]), unquote(pieces[2]), pieces[3])
        raise AuthError("Invalid token type", 8001, create_record=False)

    def _attempt_login_from_api_token(self, username: str, identifier: str, api_key: str) -> AuthenticatedUser | None:
        try:
            api_key_bytes = base64.b64decode(api_key)
        except binascii.Error as ex:
            raise AuthError("Invalid API key encoding", 8100, username=username) from ex
        with self.nodb as db:
            access_token = NODBAccessToken.find_by_identifier(db, username, identifier)
            if not access_token:
                raise AuthError("Invalid access token", 8101, username=username)
            user = access_token.load_user(db)
            if not user:
                raise AuthError("Invalid user, does not exist", 8102, username=username)
            if not user.can_login():
                raise AuthError("Invalid user, not active", 8103, username=username)
            if not access_token.check_key(api_key_bytes):
                raise AuthError("Invalid access key", 8102, is_lockable=True, username=username)
            return self._build_user(db, user)

    def _build_user(self, db, user: NODBUser) -> AuthenticatedUser:
        print(user.identifier)
        return AuthenticatedUser(user.identifier, user.display, user.email, user.permissions(db))



class LoginForm(forms.GCFlaskForm):

    username = forms.StringField(
        delayed_label="gcflask.common.username",
        validators=[forms.NoControlCharacters(), forms.InputRequired()],
    )

    password = forms.PasswordField(
        delayed_label="gcflask.common.password",
        validators=[forms.InputRequired()],
    )

    submit = forms.SubmitField()
