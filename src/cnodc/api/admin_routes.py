import flask
from autoinject import injector
from .auth import LoginController, UserController
from .uploads import UploadController
from .util import require_login, require_inputs, require_permission, json_api


admin = flask.Blueprint("admin", __name__)


@admin.route('/users/<username>/create', methods=["POST"])
@require_inputs(['password'])
@require_permission('manage_users')
def create_user(username):
    uc = UserController()
    uc.create_user(username, flask.request.json['password'])


@admin.route('/users/<username>/update', methods=["POST"])
@require_inputs([])
@require_permission('manage_users')
def update_user(username: str):
    uc = UserController()
    uc.update_user(
        username,
        flask.request.json['password'] if 'password' in flask.request.json else None,
        flask.request.json['is_active'] if 'is_active' in flask.request.json else None
    )


@admin.route('/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def assign_role(username: str):
    uc = UserController()
    uc.assign_role(username, flask.request.json['role_name'])


@admin.route('/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def unassign_role(username: str):
    uc = UserController()
    uc.unassign_role(username, flask.request.json['role_name'])


@admin.route('/workflow/<workflow_name>', methods=["POST"])
@require_inputs([])
@require_permission('manage_workflows')
def set_workflow_configuration(workflow_name):
    uc = UploadController(workflow_name)
    uc.update_workflow_config(
        flask.request.json['configuration'] if 'configuration' in flask.request.json else None,
        flask.request.json['is_active'] if 'is_active' in flask.request.json else None
    )
    return {'success': True}
