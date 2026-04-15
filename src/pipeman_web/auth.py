import typing as t
import flask
import datetime

import zrlog

from autoinject import injector

from medsutil.exceptions import CodedError
from nodb import NODBSession, NODBUser
import medsutil.awaretime as awaretime
from pipeman.users import UserController


class AuthError(CodedError): CODE_SPACE='AUTH'


@injector.injectable_global
class LoginController:

    users: UserController = None

    @injector.construct
    def __init__(self):
        self._logger = zrlog.get_logger("cnodc.loginctrl")

    def do_login(self, username: str, password: str) -> NODBSession:
        if not flask.has_request_context():
            self._logger.error(f"Login failed for user [{username}], no request context")
            raise AuthError("Login only available in request context", 1000)
        flask_config = flask.current_app.config
        if 'INSTANCE_NAME' not in flask_config:
            self._logger.error(f"Login failed for user [{username}], no instance name")
            raise AuthError("Missing instance name", 1001)
        if self.current_user() is not None:
            self._logger.error(f"Login failed for user [{username}], another user is already logged in")
            raise AuthError("Already logged in", 1002)
        return self.users.login(username, password, self._get_session_time(), flask.request.remote_addr, flask_config.get('INSTANCE_NAME'))

    def renew_session(self) -> NODBSession:
        if not flask.has_request_context():
            self._logger.error(f"Renewal for current user failed, no request context")
            raise AuthError("Session renewal only available in request context", 1100)
        session = self.current_session()
        if session is None:
            raise AuthError("No session available", 1101)
        session_time = self._get_session_time()
        return self.users.update_session_expiry(session, awaretime.utc_now() + datetime.timedelta(seconds=session_time))

    def destroy_session(self):
        if not flask.has_request_context():
            self._logger.error(f"Destruction of current session, no request context")
            raise AuthError("Session destruction only available in request context", 1200)
        session = self.current_session()
        if session is None:
            self._logger.warning("Attempted to terminate non-existent session")
            raise AuthError("Attempted to terminate non-existent session", 1201)
        self.users.destroy_session(session)

    def _get_session_time(self) -> int:
        session_time = flask.current_app.config.get('PERMANENT_SESSION_LIFETIME')
        if session_time < 1:
            self._logger.warning(f"Session time is configured to be less than 1")
            session_time = 86400
        return int(session_time)

    def generate_token(self, session: NODBSession) -> str:
        return self.users.get_session_token(session)

    def verify_token(self) -> bool:
        if not flask.has_request_context():
            return False
        if 'token_checked' not in flask.g:
            flask.g.token_checked = True
            session_id = self.users.verify_auth_header(flask.request.headers.get('Authorization', None))
            return self._load_session(session_id)
        else:
            return 'user' in flask.g and flask.g.user is not None

    def _load_session(self, session_id: str | None) -> bool:
        flask.g.session, flask.g.user, flask.g.permissions = self.users.load_session_info(session_id)
        if flask.g.session is None or flask.g.user is None or flask.g.permissions is None:
            return False
        self._logger.debug(f"User session validated:: [{flask.g.user.username}]; roles: [{';'.join(flask.g.user.roles or [])}]; permissions: [{';'.join(flask.g.permissions)}]")
        return True

    def current_session(self) -> t.Optional[NODBSession]:
        if flask.has_request_context() and self.verify_token():
            return flask.g.session
        return None

    def current_user(self) -> t.Optional[NODBUser]:
        if flask.has_request_context() and self.verify_token():
            return flask.g.user
        return None

    def current_permissions(self) -> set:
        if flask.has_request_context() and self.verify_token():
            return flask.g.permissions
        return set()

    def change_current_user_password(self, password: str):
        user = self.current_user()
        if user is None:
            raise AuthError("Cannot change password, no logged in user", 1100)
        self.users.update_user(user.username, password=password)






