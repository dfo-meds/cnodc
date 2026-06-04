import datetime

import flask
import zrlog

from autoinject import injector, auto

from gcflask.forms import GCFlaskForm, StringField, SubmitField, PasswordField, SelectField, InputRequired, \
    NoControlCharacters, BooleanField
from gcflask.i18n import TString
from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission, api_error_handling, web_error_handling
from gcflask.user import current_user
from gcflask.util import flasht, FlaskRequestJsonData
from medsutil.awaretime import AwareDateTime
from medweb.apps.medsid.controller import AccessController, AccessManagementError

user = MultiLanguageBlueprint('user', __name__, url_prefix="/medsid")


@user.route('/me')
@require_permission(authenticated_only=True)
@injector.inject
def me(ac: AccessController = auto()):
    c_user = ac.load_user_by_id(current_user().get_id())
    return flask.render_template("myself.html", user=c_user, title=c_user.display)

# TODO: should be in configuration
TOKEN_LIFETIME_SECONDS = 3600

@user.route('/api/create-access-token', methods='POST')
@require_permission(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def create_access_token(ac: AccessController = auto()):
    data = FlaskRequestJsonData()
    expiry = AwareDateTime.utcnow() + datetime.timedelta(seconds=TOKEN_LIFETIME_SECONDS)
    return flask.jsonify({
        'token': ac.create_temporary_access_token(
            data.get("username"),
            data.get("password"),
            TOKEN_LIFETIME_SECONDS
        ),
        'expiry': expiry.isoformat()
    })


@user.route('/api/renew-access-token', methods='POST')
@require_permission(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def renew_access_token(ac: AccessController = auto()):
    data = FlaskRequestJsonData()
    expiry = AwareDateTime.utcnow() + datetime.timedelta(seconds=TOKEN_LIFETIME_SECONDS)
    return flask.jsonify({
        'token': ac.renew_temporary_access_token(
            data.get("token"),
            TOKEN_LIFETIME_SECONDS
        ),
        'expiry': expiry.isoformat()
    })


@user.route('/api/remove-access-token', methods='POST')
@require_permission(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def remove_access_token(ac: AccessController = auto()):
    data = FlaskRequestJsonData()
    ac.remove_temporary_access_token(data.get("token"))
    return flask.jsonify({
        'success': True,
    })


@user.route('/me/edit', methods=['GET', 'POST'])
@require_permission(authenticated_only=True)
@web_error_handling
@injector.inject
def edit(ac: AccessController = auto()):
    c_user = ac.load_user_by_id(current_user().get_id())
    form = EditMyselfForm(
        username=c_user.username,
        language_pref=c_user.language_pref,
        display=c_user.display,
        email=c_user.email
    )
    if form.validate_on_submit():
        try:
            ac.update_user(
                user_id=c_user.user_id,
                username=form.username.data or '',
                display=form.display.data or '',
                email=form.email.data or '',
                change_by_user=True
            )
            flasht("medsid.forms.edit.success", "success")
            return flask.redirect(flask.url_for("user.me"))
        except AccessManagementError as e:
            flasht(e.message_key, "error")
            zrlog.get_logger("medsid.web").exception(f"Error when a user tried to update themselves")
    return flask.render_template("form.html", form=form)


@user.route('/me/change-password', methods=['GET', 'POST'])
@require_permission(authenticated_only=True)
@web_error_handling
@injector.inject
def change_password(ac: AccessController = auto()):
    form = ChangePasswordForm()
    if form.validate_on_submit():
        try:
            ac.update_user(
                user_id=current_user().get_id(),
                password=form.new_password.data or '',
                change_by_user=True
            )
            flasht("medsid.forms.change_password.success", "success")
            return flask.redirect(flask.url_for("user.me"))
        except AccessManagementError as e:
            flasht(e.message_key, "error")
            zrlog.get_logger("medsid.web").exception(f"Error when a user tried to change their password")
    return flask.render_template("form.html", form=form)


@user.route('/users/<username>')
@require_permission("medsid.user_management.view")
@web_error_handling
@injector.inject
def view_user(username: str, ac: AccessController = auto()):
    c_user = ac.load_user_by_name(username)
    if c_user is None:
        return flask.abort(404)
    return flask.render_template("user.html", user=c_user, title=c_user.display)


@user.route('/users/create', methods=['GET', 'POST'])
@require_permission("medsid.user_management.edit")
@web_error_handling
@injector.inject
def create_user(ac: AccessController = auto()):
    form = EditUserForm()
    if form.validate_on_submit():
        try:
            ac.create_user(
                username=form.username.data or '',
                password=None,
                email=form.email.data,
                display_name=form.display.data,
                allow_api_access=form.allow_api_access.data,
                status=form.status.data,
                language_pref=form.language_pref.data,
            )
            flasht("medsid.forms.create_user.success", "success")
            return flask.redirect(flask.url_for("user.view_user", username=form.username.data))
        except AccessManagementError as ex:
            flasht(ex.message_key, "error")
            zrlog.get_logger("medsid.web").exception("Error when a user tried to create another account")
    return flask.render_template("form.html", form=form)


@user.route('/users/<username>/edit', methods=['GET', 'POST'])
@require_permission("medsid.user_management.edit")
@web_error_handling
def edit_user(username: str, ac: AccessController = auto()):
    c_user = ac.load_user_by_name(username)
    form = EditUserForm(
        username=c_user.username,
        display=c_user.display,
        email=c_user.email,
        allow_api_access=c_user.allow_api_access == 'Y',
        status=c_user.status.value,
        language_pref=c_user.language_pref
    )
    if form.validate_on_submit():
        try:
            ac.update_user(
                username=form.username.data or '',
                password=None,
                email=form.email.data or '',
                display=form.display.data or '',
                api_access=form.allow_api_access.data,
                enabled=form.status.data == 'active',
                language_pref=form.language_pref.data,
                user_id=c_user.user_id
            )
            flasht("medsid.forms.edit_user.success", "success")
            return flask.redirect(flask.url_for("user.view_user", username=form.username.data))
        except AccessManagementError as ex:
            flasht(ex.message_key, "error")
            zrlog.get_logger("medsid.web").exception("Error when a user tried to edit another account")
    return flask.render_template("form.html", form=form)


@user.route('/api/users')
@require_permission("medsid.user_management.view", is_api=True)
@api_error_handling
def api_list_users():
    return flask.abort(404)


@user.route('/users')
@require_permission("medsid.user_management.view")
@web_error_handling
def list_users():
    return flask.abort(404)


class EditMyselfForm(GCFlaskForm):
    username = StringField(delayed_label="medsid.user.username", validators=[InputRequired(), NoControlCharacters()])
    display = StringField(delayed_label="medsid.user.display_name", validators=[InputRequired(), NoControlCharacters()])
    email = StringField(delayed_label="medsid.user.email", validators=[InputRequired(), NoControlCharacters()])
    language_pref = SelectField(delayed_label="medsid.user.language_pref", validators=[InputRequired()], choices=[
        ("en", TString("gcflask.common.en")),
        ("fr", TString("gcflask.common.fr")),
    ])
    submit = SubmitField()


class ChangePasswordForm(GCFlaskForm):
    new_password = PasswordField(delayed_label="medsid.user.new_password", validators=[InputRequired(), NoControlCharacters()])
    new_password_repeat = PasswordField(delayed_label="medsid.user.new_password_repeat", validators=[InputRequired(), NoControlCharacters()])
    submit = SubmitField()


class EditUserForm(GCFlaskForm):
    username = StringField(delayed_label="medsid.user.username", validators=[InputRequired(), NoControlCharacters()])
    display = StringField(delayed_label="medsid.user.display_name", validators=[InputRequired(), NoControlCharacters()])
    email = StringField(delayed_label="medsid.user.email", validators=[InputRequired(), NoControlCharacters()])
    allow_api_access = BooleanField(delayed_label="medsid.user.allow_api_access")
    status = SelectField(delayed_label="medsid.user.status", validators=[InputRequired()], choices=[
        ("active", TString("medsid.user.active")),
        ("inactive", TString("medsid.user.inactive")),
    ])
    language_pref = SelectField(delayed_label="medsid.user.language_pref", validators=[InputRequired()], choices=[
        ("en", TString("gcflask.common.en")),
        ("fr", TString("gcflask.common.fr")),
    ])
    submit = SubmitField()
