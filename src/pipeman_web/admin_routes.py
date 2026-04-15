import flask
from autoinject import injector

from medsutil.exceptions import CodedError
from nodb import NODBUploadWorkflow, LockType
from pipeman.users import UserController
from nodb.interface import NODB
from pipeman_web.util import require_inputs, require_permission


admin = flask.Blueprint("admin", __name__)


@admin.route('/private/api/users/<username>/create', methods=["POST"])
@require_inputs(['password'])
@require_permission('manage_users')
def create_user(username):
    uc = UserController()
    uc.create_user(username, flask.request.json['password'])


@admin.route('/private/api/users/<username>/update', methods=["POST"])
@require_inputs([])
@require_permission('manage_users')
def update_user(username: str):
    uc = UserController()
    uc.update_user(
        username,
        flask.request.json['password'] if 'password' in flask.request.json else None,
        int(flask.request.json['old_expiry_seconds']) if 'old_expiry_seconds' in flask.request.json else 0,
        flask.request.json['is_active'] if 'is_active' in flask.request.json else None
    )


@admin.route('/private/api/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def assign_role(username: str):
    uc = UserController()
    uc.assign_role(username, flask.request.json['role_name'])


@admin.route('/private/api/users/<username>/assign-role', methods=["POST"])
@require_inputs(['role_name'])
@require_permission('manage_users')
def unassign_role(username: str):
    uc = UserController()
    uc.unassign_role(username, flask.request.json['role_name'])


@admin.route('/private/api/workflow/<workflow_name>', methods=["POST"])
@require_inputs([])
@require_permission('manage_workflows')
@injector.inject
def set_workflow_configuration(workflow_name: str, nodb: NODB):
    with nodb as db:
        workflow = NODBUploadWorkflow.find_by_name(db, workflow_name, lock_type=LockType.FOR_NO_KEY_UPDATE)
        if workflow is None:
            raise CodedError('Workflow not found', 1000, code_space='WORKFLOWUPDATE')
        if 'configuration' in flask.request.json:
            workflow.set_config(flask.request.json['configuration'])
        if 'is_active' in flask.request.json:
            workflow.is_active = flask.request.json['is_active']
        db.upsert_object(workflow)
        db.commit()
    return {'success': True}
