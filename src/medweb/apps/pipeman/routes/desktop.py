from types import EllipsisType

import flask
import typing as t

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import security_check, web_error_handling, require_permission
from autoinject import injector

from medweb.apps.pipeman.nodb_manager import NODBController, ReviewResult

desktop = MultiLanguageBlueprint("desktop", __name__)


def json_param[T](param_name: str, coerce: t.Callable[[t.Any], T] | None = None, default: T | EllipsisType = ...) -> T:
    if not flask.request.is_json:
        return flask.abort(400, "Request must be JSON formatted")
    if not isinstance(flask.request.json, dict):
        flask.abort(400, "Request must contain a JSON mapping payload")
    if param_name not in flask.request.json and default is ...:
        flask.abort(400, "Missing mandatory parameter")
    try:
        x = flask.request.json.get(param_name, default)
        if x is not None and coerce is not None:
            x = coerce(x)
        return x
    except (ValueError, TypeError, IndexError) as e:
        flask.abort(400, f"Invalid parameter for [{param_name}]: {e}")



@desktop.route("/internal/queues/next", methods=["POST"])
@security_check("pipeman.lock_queue_items")
@web_error_handling
@injector.inject
def lock_next_queue_item(nodb: NODBController = None):
    # Check request parameters
    app_id = json_param("app_id", str)
    queue_name = json_param("queue_name", str)
    subqueue_name = json_param("subqueue_name", str, None)
    escalation_level = json_param("escalation_level", int, 0)

    # Security checks
    require_permission([f"pipeman.lock_queue_items.{queue_name}", "pipeman.lock_queue_items.all"], require_any=True)
    if escalation_level > 0:
        require_permission(f"pipeman.lock_queue_items.escalated")

    # Delegate to controller
    return nodb.fetch_next_queue_item(
        queue_name,
        escalation_level,
        app_id,
        subqueue_name
    )


@desktop.route("/internal/queues/queue_uuid>/renew", methods=["POST"])
@security_check("pipeman.lock_queue_items")
@web_error_handling
@injector.inject
def renew_queue_item(queue_uuid: str, nodb: NODBController = None):
    return nodb.renew_queue_item(queue_uuid)


@desktop.route("/internal/queues/<queue_uuid>/close-qc", methods=["POST"])
@security_check("pipeman.lock_queue_items")
@web_error_handling
@injector.inject
def close_qc_queue_item(queue_uuid: str, nodb: NODBController = None):
    return nodb.close_qc_item(
        queue_uuid,
        json_param("app_id", str),
        json_param("result", ReviewResult),
    )


@desktop.route("/internal/working/<record_uuid>", methods=["POST"])
@security_check("pipeman.working.view")
@web_error_handling
@injector.inject
def download_working_record(record_uuid: str, nodb: NODBController = None):
    return nodb.serve_working_record(record_uuid)
