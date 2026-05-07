import wtforms

from gcflask.auth import AuthenticationHandler
from gcflask.forms import GCFlaskForm
from gcflask.user import AuthenticatedUser


class LocalMedsIDHandler(AuthenticationHandler):

    def __init__(self, **kwargs):
        super().__init__('medsid', True, **kwargs)

    def login_page(self):
        form = LoginForm()
        if form.validate_on_submit():
            ...


    def logout(self):
        pass

    def load_user(self, user_id):
        ...

    def _attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        ...

    def _attempt_login_from_token(self, token: str) -> AuthenticatedUser | None:
        ...


class LoginForm(GCFlaskForm):
    ...