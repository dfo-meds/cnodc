import flask
import functools
import typing as t

import nodb.web.auth as auth
import nodb.web.workflows as workflows
from cnodc.exc import CNODCError

nodb = flask.Blueprint("nodb", __name__)


def require_login(cb: callable):
    return _access_wrapper(cb)


def require_permission(permission_names: t.Union[str, t.Iterable[str]]):
    if isinstance(permission_names, str):
        permission_names = [permission_names]

    def _outer_wrapper(cb):
        return _access_wrapper(cb, permission_names)

    return _outer_wrapper


def _access_wrapper(cb: callable, permission_names: t.Optional[list[str]] = None):

    @functools.wraps(cb)
    def _inner_wrapper(*args, **kwargs):
        auth_header = flask.request.headers.get('Authorization', None)
        if auth_header is None:
            return {'error': 'missing auth header'}
        if not auth_header.lower().startswith('bearer '):
            return {'error': 'invalid auth header'}
        token = auth_header[7:]
        if 'SECRET_KEY' not in flask.current_app.config:
            return {'error': 'no secret key configured'}, 500
        if not auth.verify_user_token(token):
            return {'error': 'token validation failed'}, 403
        current_perms = auth.current_permissions()
        if permission_names and '__admin__' not in current_perms and not all(p in current_perms for p in permission_names):
            return {'error': 'unauthorized access'}, 403
        return cb(*args, **kwargs)

    return _inner_wrapper


def require_json(fields):

    def _wrapper(cb):

        @functools.wraps(cb)
        def _inner(*args, **kwargs):
            if not flask.request.is_json:
                return {'error': 'content type must be JSON'}, 400
            for x in fields:
                if x not in flask.request.json:
                    return {'error': f'missing field [{x}]'}, 400
            return cb(*args, **kwargs)

        return _inner

    return _wrapper


def return_json(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except CNODCError as ex:
            return {"error": str(ex), "code": ex.obfuscated_code()}
        except Exception as ex:
            return {"error": f"{ex.__class__.__name__}: {str(ex)}"}
    return _inner


@nodb.route("/login", methods=["POST"])
@require_json(['username', 'password'])
@return_json
def login():
    return auth.attempt_login(
        flask.request.json['username'],
        flask.request.json['password'],
        flask.request.json['shared'] if 'shared' in flask.request.json else None
    )


@nodb.route('/renew', methods=["POST"])
@require_json
@require_login
@return_json
def renew():
    return auth.renew_login(
        flask.request.json['shared'] if 'shared' in flask.request.json else None
    )


@nodb.route('/change-password', methods=["POST"])
@require_login
@require_json(['new_password'])
@return_json
def change_password():
    return auth.change_password(flask.request.json['new_password'])


@nodb.route('/users', methods=["GET"])
@require_permission("manage_users")
@return_json
def list_users():
    # TODO
    pass


@nodb.route("/users/<username>", methods=["GET"])
@require_permission("manage_users")
@return_json
def user_info(username: str):
    # TODO
    pass


@nodb.route('/users/<username>/create', methods=["POST"])
@require_permission("manage_users")
@require_json(['password'])
@return_json
def create_user(username):
    return auth.create_user(
        username,
        flask.request.json['password']
    )


@nodb.route('/users/<username>/update', methods=["POST"])
@require_permission("manage_users")
@return_json
def update_user(username: str):
    return auth.update_user(
        username,
        flask.request.json['password'] if 'password' in flask.request.json else None,
        flask.request.json['is_active'] if 'is_active' in flask.request.json else None
    )


@nodb.route('/users/<username>/assign-role', methods=["POST"])
@require_permission("manage_users")
@require_json(['role_name'])
@return_json
def assign_role(username: str):
    return auth.assign_role(
        username,
        flask.request.json['role_name']
    )


@nodb.route('/users/<username>/unassign-role', methods=["POST"])
@require_permission("manage_users")
@require_json(['role_name'])
@return_json
def unassign_role(username: str):
    return auth.unassign_role(
        username,
        flask.request.json['role_name']
    )


@nodb.route('/submit/<workflow_name>', methods=["POST"])
@require_permission("submit_files")
@return_json
def submit_file(workflow_name: str):
    return workflows.handle_file_upload(workflow_name, {
        x: flask.request.headers[x] for x in flask.request.headers.keys(True)
    }, flask.request.data)


@nodb.route('/submit/<workflow_name>/<request_id>', methods=["POST"])
@require_permission("submit_files")
@return_json
def submit_file_to_request(workflow_name: str, request_id: str = None):
    return workflows.handle_file_upload(workflow_name, {
        x: flask.request.headers[x] for x in flask.request.headers.keys(True)
    }, flask.request.data, request_id)


@nodb.route('/submit/<workflow_name>/<request_id>', methods=["POST"])
@require_permission("submit_files")
@return_json
def cancel_file_submission(workflow_name, request_id):
    return workflows.cancel_file_upload(
        workflow_name,
        request_id,
        flask.request.headers.get('x-cnodc-token', None)
    )


@nodb.route('/submit/<workflow_name>', methods=["GET"])
@require_permission("submit_files")
@return_json
def submission_info(workflow_name):
    workflows.properties(workflow_name)
    return {
        'max_chunk_size': flask.current_app.config['MAX_CONTENT_LENGTH']
    }
