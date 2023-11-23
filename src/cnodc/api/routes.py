import flask
from autoinject import injector
from .auth import LoginController, UserController
from .util import require_login, require_inputs, require_permission, json_api
from .uploads import UploadResult, UploadController

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


@cnodc.route('/submit/<workflow_name>', methods=["POST"])
@json_api
@require_permission("submit_files")
def submit_file(workflow_name: str):
    uc = UploadController(workflow_name)
    res = uc.upload_request(flask.request.data, {
        x: flask.request.headers.get(x)
        for x in flask.request.headers.keys(True)
    })
    if res == UploadResult.CONTINUE:
        args = {
            'workflow_name': workflow_name,
            'request_id': uc.request_id
        }
        return {
            'headers': {
                'x-cnodc-token': uc.token
            },
            'next_uri': flask.url_for('cnodc.submit_next_file', **args, _external=True),
            'cancel_uri': flask.url_for('cnodc.cancel_request', **args, _external=True)
        }
    return {'success': True}


@cnodc.route('/submit/<workflow_name>/<request_id>', methods=["POST"])
@json_api
@require_permission("submit_files")
def submit_next_file(workflow_name: str, request_id: str):
    uc = UploadController(workflow_name, request_id, flask.request.headers.get('x-cnodc-token', None))
    res = uc.upload_request(flask.request.data, {
        x: flask.request.headers.get(x)
        for x in flask.request.headers.keys(True)
    })
    if res == UploadResult.CONTINUE:
        args = {
            'workflow_name': workflow_name,
            'request_id': uc.request_id
        }
        return {
            'headers': {
                'x-cnodc-token': uc.token,
            },
            'next_uri': flask.url_for('cnodc.submit_next_file', **args, _external=True),
            'cancel_uri': flask.url_for('cnodc.cancel_request', **args, _external=True)
        }
    return {'success': True}


@cnodc.route('/submit/<workflow_name>/<request_id>/cancel', methods=['POST'])
@json_api
@require_permission("submit_files")
def cancel_request(workflow_name: str, request_id: str):
    uc = UploadController(workflow_name, request_id, flask.request.headers.get('x-cnodc-token', None))
    uc.cancel_request()
    return {'success': True}


@cnodc.route('/submit/<workflow_name>', methods=['GET'])
@json_api
@require_permission("submit_files")
def workflow_info(workflow_name):
    uc = UploadController(workflow_name)
    uc.check_access()
    return {
        'max_chunk_size': flask.current_app.config['MAX_CONTENT_LENGTH']
    }
