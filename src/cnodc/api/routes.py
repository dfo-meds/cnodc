import flask
from autoinject import injector
from .auth import LoginController, UserController
from .nodb import NODBWebController
from .util import require_login, require_inputs, require_permission, json_api, has_access
from .uploads import UploadResult, UploadController
import typing as t

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
        'expiry': session.expiry_time.isoformat(),
        'username': flask.request.json['username'],
        'access': _build_access_list()
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


@cnodc.route('/access', methods=['GET'])
@json_api
@require_login
def list_access():
    return _build_access_list()


@injector.inject
def _build_access_list() -> list[str]:
    from .uploads import list_all_workflows_with_access
    options = ['change-password', 'renew', 'access']
    if has_access('handle_queue_items'):
        if has_access('handle_decode_failures'):
            options.append('queue:decode-failure')
        if has_access('handle_ocean_reviews'):
            options.append('queue:ocean-review')
        if has_access('handle_integrity_failures'):
            options.append('queue:integrity-failure')
        if has_access('handle_station_failures'):
            options.append('queue:station-failure')
    if has_access('submit_files'):
        for workflow_name in list_all_workflows_with_access():
            options.append(f'submit:{workflow_name}')
    return options


@cnodc.route('/change-password', methods=["POST"])
@require_inputs(['password'])
@require_login
@injector.inject
def change_password():
    uc = UserController()
    uc.change_password(flask.request.json["password"])
    return {
        'success': True
    }


@cnodc.route('/submit/<workflow_name>', methods=["POST"])
@json_api
@require_permission("submit_files")
def submit_file(workflow_name: str):
    uc = UploadController(workflow_name)
    res = uc.upload_request(flask.request.data, {
        x.lower(): flask.request.headers.get(x)
        for x in flask.request.headers.keys(True)
    })
    if res == UploadResult.CONTINUE:
        args = {
            'workflow_name': workflow_name,
            'request_id': uc.request_id
        }
        return {
            'headers': {
                'X-CNODC-Token': uc.token
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
        x.lower(): flask.request.headers.get(x)
        for x in flask.request.headers.keys(True)
    })
    if res == UploadResult.CONTINUE:
        args = {
            'workflow_name': workflow_name,
            'request_id': uc.request_id
        }
        return {
            'headers': {
                'X-CNODC-Token': uc.token,
            },
            'next_uri': flask.url_for('cnodc.submit_next_file', **args, _external=True),
            'cancel_uri': flask.url_for('cnodc.cancel_request', **args, _external=True),
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
    info = {
        'max_chunk_size': flask.current_app.config['MAX_CONTENT_LENGTH'],
    }
    info.update(uc.properties())
    return info


@cnodc.route('/next/decode-failure', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_decode_failures")
@injector.inject
def next_decode_failure(nodb_web: NODBWebController = None):
    return nodb_web.get_next_queue_item(
        queue_name='nodb_decode_failure'
    )


@cnodc.route('/next/integrity-failure', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_integrity_failures")
@injector.inject
def next_decode_failure(nodb_web: NODBWebController = None):
    return nodb_web.get_next_queue_item(
        queue_name='nodb_integrity_review'
    )


@cnodc.route('/next/station-failure', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_station_failures")
@injector.inject
def next_decode_failure(nodb_web: NODBWebController = None):
    return nodb_web.get_next_queue_item(
        queue_name='nodb_station_review'
    )


@cnodc.route('/next/ocean-review', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_ocean_reviews")
@injector.inject
def next_decode_failure(nodb_web: NODBWebController = None):
    return nodb_web.get_next_queue_item(
        queue_name='nodb_manual_review',
    )


@cnodc.route('/queue-item/<queue_item_uuid>/renew', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
def renew_queue_lock(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.renew_queue_item_lock(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/release', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
def release_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.release_queue_item_lock(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/complete', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
def complete_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.mark_queue_item_complete(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/fail', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
def fail_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.mark_queue_item_failed(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )
