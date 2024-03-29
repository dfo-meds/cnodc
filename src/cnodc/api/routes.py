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
def login(login_controller: LoginController = None, nodb_web: NODBWebController = None):
    session = login_controller.do_login(
        flask.request.json["username"],
        flask.request.json["password"]
    )
    return {
        'token': login_controller.generate_token(session),
        'expiry': session.expiry_time.isoformat(),
        'username': flask.request.json['username'],
        'access': nodb_web.access_list()
    }


@cnodc.route('/logout', methods=["POST"])
@json_api
@require_login
@injector.inject
def logout(login_controller: LoginController = None):
    login_controller.destroy_session()
    return {
        'success': True
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
@injector.inject
def list_access(nodb_web: NODBWebController):
    return nodb_web.access_list()


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


@cnodc.route('/stations', methods=['GET'])
@require_permission('handle_nodb_station_failure')
@injector.inject
def list_stations(nodb_web: NODBWebController = None):
    return nodb_web.list_stations(), {'Content-Type': 'application/octet-stream'}


@cnodc.route('/stations/new', methods=['POST'])
@require_inputs(['station'])
@require_permission('handle_nodb_station_failure')
@injector.inject
def create_station(nodb_web: NODBWebController = None):
    return nodb_web.create_station(flask.request.json['station'])


# TODO: update station


@cnodc.route('/queue-item/next/<queue_service_name>', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def next_queue_item(queue_service_name, nodb_web: NODBWebController = None):
    return nodb_web.get_next_queue_item(
        service_name=queue_service_name
    )


@cnodc.route('/queue-item/<queue_item_uuid>/stream-batch', methods=['GET'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def download_batch(queue_item_uuid: str, nodb_web: NODBWebController = None):
    return nodb_web.stream_batch_working_records(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    ), {'Content-Type': 'application/octet-stream'}


@cnodc.route('/queue-item/<queue_item_uuid>/apply-changes', methods=['POST'])
@require_inputs(['app_id', 'operations'])
@require_permission("handle_queue_items")
@injector.inject
def apply_changes(queue_item_uuid: str, nodb_web: NODBWebController = None):
    return nodb_web.save_updates(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id'],
        update_json=flask.request.json['operations']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/clear-actions', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def reset_actions(queue_item_uuid: str, nodb_web: NODBWebController = None):
    return nodb_web.reset_actions(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/retry-decode', methods=['POST'])
@require_inputs(['app_id', 'operations'])
@require_permission("handle_queue_items")
@injector.inject
def retry_decode(queue_item_uuid: str, nodb_web: NODBWebController = None):
    return nodb_web.retry_decode(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/renew', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def renew_queue_lock(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.renew_queue_item_lock(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/release', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def release_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.release_queue_item_lock(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/complete', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def complete_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.mark_queue_item_complete(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )


@cnodc.route('/queue-item/<queue_item_uuid>/fail', methods=['POST'])
@require_inputs(['app_id'])
@require_permission("handle_queue_items")
@injector.inject
def fail_queue_item(queue_item_uuid: str, nodb_web: NODBWebController = None):
    nodb_web.mark_queue_item_failed(
        item_uuid=queue_item_uuid,
        enc_app_id=flask.request.json['app_id']
    )
