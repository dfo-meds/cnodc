import flask
import zrlog

from autoinject import injector, auto

from gcapp import i18n
from gcflask.action_list import ActionList
from gcflask.datatables import DataTable, DataQuery, ObjectProperty
from gcflask.datatables.table import ActionListColumn
from gcflask.forms import GCFlaskForm, StringField, SubmitField, PasswordField, SelectField, InputRequired, \
    NoControlCharacters, BooleanField
from gcapp.i18n.base import TString
from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import security_check, api_error_handling, web_error_handling
from gcflask.user import current_user
from gcflask.util import flasht, json_param
from medsutil.secure import generate_secure_random_password
from medweb.apps.medsid.controller import AccessController, AccessManagementError
from nodb.access import NODBUser

user = MultiLanguageBlueprint('user', __name__, url_prefix="/medsid")


@user.route('/api/create-access-token', methods=['POST'])
@security_check(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def create_access_token(ac: AccessController = auto()):
    token, expiry = ac.create_temporary_access_token(
        json_param("username"),
        json_param("password"),
    )
    c_user = current_user()
    return flask.jsonify({
        'success': True,
        'token': token,
        'expiry': expiry.isoformat(),
        'access': ac.list_available_operations(),
        'username': c_user.get_username() if hasattr(c_user, 'get_username') else None,
        'display': c_user.display_name
    })


@user.route('/api/renew-access-token', methods=['POST'])
@security_check(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def renew_access_token(ac: AccessController = auto()):
    token, expiry = ac.renew_temporary_access_token(
        json_param("token")
    )
    return flask.jsonify({
        'success': True,
        'token': token,
        'expiry': expiry.isoformat()
    })


@user.route('/api/remove-access-token', methods=['POST'])
@security_check(is_api=True, anonymous_only=True)
@api_error_handling
@injector.inject
def remove_access_token(ac: AccessController = auto()):
    ac.remove_temporary_access_token(json_param("token"))
    return flask.jsonify({
        'success': True,
    })


@user.route('/me')
@security_check(authenticated_only=True)
@injector.inject
def me(ac: AccessController = auto()):
    c_user = ac.load_user_by_id(current_user().get_id())
    return flask.render_template("myself.html", user=c_user, title=c_user.display)


@user.route('/me/edit', methods=['GET', 'POST'])
@security_check(authenticated_only=True)
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
@security_check(authenticated_only=True)
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


@user.route('/users')
@security_check("medsid.user_management.view")
@web_error_handling
def list_users():
    links = []
    if current_user().require_all(["medsid.user_management.edit"]):
        links.append((
            flask.url_for("user.create_user"),
            i18n.tr("medsid.page.create_user.link")
        ))
    return flask.render_template(
        "data_table.html",
        table=_users_table(),
        side_links=links,
    )


@user.route('/ajax/users')
@security_check("medsid.user_management.view")
@api_error_handling
def list_users_ajax():
    return _users_table().build_ajax()


@user.route('/users/<username>')
@security_check("medsid.user_management.view")
@web_error_handling
@injector.inject
def view_user(username: str, ac: AccessController = auto()):
    c_user = ac.load_user_by_name(username)
    if c_user is None:
        return flask.abort(404)
    return flask.render_template(
        "user.html",
        user=c_user,
        title=c_user.display,
        sidebar=_user_actions(c_user, False)
    )


@user.route('/users/create', methods=['GET', 'POST'])
@security_check("medsid.user_management.edit")
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
@security_check("medsid.user_management.edit")
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


@user.route("/users/<username>/reset")
@security_check("medsid.user_management.edit")
@web_error_handling
def reset_password(username: str, ac: AccessController = auto()):
    form = ConfirmResetForm()
    if form.validate_on_submit():
        try:
            ac.update_user(
                username=username,
                password=generate_secure_random_password()
            )
            flasht("medsid.forms.reset_password.success", "success")
            return flask.redirect(flask.url_for("user.view_user", username=username))
        except AccessManagementError as ex:
            flasht(ex.message_key, "error")
            zrlog.get_logger("medsid.web").exception("Error when a user tried to reset another user's password")
    return flask.render_template("form.html", form=form)


@user.route("/reset-password")
@security_check(anonymous_only=True)
@web_error_handling
def reset_my_password(ac: AccessController = auto()):
    form = ResetMyPasswordForm()
    if form.validate_on_submit():
        try:
            ac.update_user(
                username=form.username.data or '',
                password=generate_secure_random_password()
            )
            flasht("medsid.forms.reset_password.success", "success")
            return flask.redirect(flask.url_for("auth.login"))
        except AccessManagementError as ex:
            flasht(ex.message_key, "error")
            zrlog.get_logger("medsid.web").exception("Eerror when a user tried to reset their own password")
    return flask.render_template("form.html", form=form)


class ConfirmResetForm(GCFlaskForm):
    submit = SubmitField()


class ResetMyPasswordForm(GCFlaskForm):
    username = StringField(delayed_label="medsid.user.username", validators=[InputRequired(), NoControlCharacters()])
    submit = SubmitField()


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


def _users_table() -> DataTable:
    table = DataTable(
        table_id="user_list",
        query=DataQuery(NODBUser),
        ajax_route=flask.url_for("user.list_users_ajax"),
        default_order=[("username", False)],
        page_size=25
    )
    table.add_column(ObjectProperty(
        name="identifier",
        header_text=i18n.tr("medsid.user.identifier"),
        allow_order=True,
    ))
    table.add_column(ObjectProperty(
        name="username",
        header_text=i18n.tr("medsid.user.username"),
        allow_order=True,
        allow_search=True,
    ))
    table.add_column(ObjectProperty(
        name="display",
        header_text=i18n.tr("medsid.user.display_name"),
        allow_order=True,
        allow_search=True,
    ))
    table.add_column(ObjectProperty(
        name="email",
        header_text=i18n.tr("medsid.user.email"),
        allow_order=True,
        allow_search=True,
    ))
    table.add_column(ActionListColumn(_user_actions))
    return table


def _user_actions(user: NODBUser, for_action_table: bool = False) -> ActionList:
    actions = ActionList()
    kwargs = {
        "username": user.username
    }
    if for_action_table:
        if current_user().require_all(["medsid.user_management.view"]):
            actions.add_action("medsid.page.view_user.link", "user.view_user", **kwargs)
    if current_user().require_all(["medsid.user_management.edit"]):
        actions.add_action("medsid.page.edit_user.link", "user.edit_user", **kwargs)
        actions.add_action("medsid.page.reset_password.link", "user.reset_password", **kwargs)
    return actions
