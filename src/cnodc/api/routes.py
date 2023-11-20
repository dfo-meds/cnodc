import flask
from autoinject import injector
from .auth import LoginController, UserController
from .util import require_login, require_inputs, require_permission, json_api


cnodc = flask.Blueprint("cnodc", __name__)


@cnodc.route("/login", methods=["POST"])
@require_inputs(["username", "password"])
@injector.inject
def login(login_controller: LoginController = None):
    session = login_controller.do_login(
        flask.request.json["username"],
        flask.request.json["password"]
    )
    return {
        'token': login_controller.generate_token(session),
        'expiry': session.expiry_time.isoformat()
    }


@cnodc.route('/renew', methods=["POST"])
@json_api
@require_login
@injector.inject
def renew(login_controller: LoginController = None):
    session = login_controller.renew_session()
    return {
        'token': login_controller.generate_token(session),
        'expiry': session.expiry_time.isoformat()
    }


@cnodc.route('/change-password', methods=["POST"])
@require_inputs(['password'])
@require_login
@injector.inject
def change_password():
    uc = UserController()
    uc.change_password(flask.request.json["password"])


@cnodc.route('/users/<username>/create', methods=["POST"])
@require_inputs(['password'])
@require_permission('manage_users')
def create_user(username):
    uc = UserController()
    uc.create_user(username, flask.request.json['password'])


@cnodc.route('/users/<username>/update', methods=["POST"])
@require_inputs([])
@require_permission('manage_users')
def update_user(username: str):
    uc = UserController()
    uc.update_user(
        username,
        flask.request.json['password'] if 'password' in flask.request.json else None,
        flask.request.json['is_active'] if 'is_active' in flask.request.json else None
    )


@cnodc.route('/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def assign_role(username: str):
    uc = UserController()
    uc.assign_role(username, flask.request.json['role_name'])


@cnodc.route('/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def unassign_role(username: str):
    uc = UserController()
    uc.unassign_role(username, flask.request.json['role_name'])
