import flask

from gcflask.auth import AuthenticationHandler
import gcflask.forms as forms
from gcflask.user import AuthenticatedUser


class LocalMedsIDHandler(AuthenticationHandler):

    def __init__(self, **kwargs):
        super().__init__('medsid', True, **kwargs)

    def login_page(self):
        form = LoginForm()
        if form.validate_on_submit():
            ...
        return flask.render_template('form.html', form=form)

    def logout(self):
        pass

    def load_user(self, user_id):
        ...

    def _attempt_login_from_password(self, username: str, password: str) -> AuthenticatedUser | None:
        ...

    def _attempt_login_from_token(self, token: str) -> AuthenticatedUser | None:
        ...


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
