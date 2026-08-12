from types import EllipsisType

import flask
import typing as t

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import security_check, web_error_handling, require_permission, api_error_handling
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medweb.apps.pipeman.nodb_manager import NODBController, ReviewResult
from nodb.observations import PlatformStatus

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


@desktop.route("/internal/queues/ready", methods=["GET"])
@security_check("pipeman.handle_queue_items")
@api_error_handling
@injector.inject
def get_queue_report(nodb: NODBController = None):
    return nodb.get_queue_report()


@desktop.route("/internal/queues/next", methods=["POST"])
@security_check("pipeman.handle_queue_items")
@api_error_handling
@injector.inject
def lock_next_queue_item(nodb: NODBController = None):
    # Check request parameters
    app_id = json_param("app_id", str)
    queue_name = json_param("queue_name", str)
    subqueue_name = json_param("subqueue_name", str, None)
    escalation_level = json_param("escalation_level", int, 0)

    # Security checks
    require_permission([f"pipeman.handle_queue_items.{queue_name}", "pipeman.handle_queue_items.all"], require_any=True)
    if escalation_level > 0:
        require_permission([f"pipeman.handle_queue_items.escalated.{queue_name}", "pipeman.handle_queue_items.escalated.all"], require_any=True)

    # Delegate to controller
    return nodb.fetch_next_queue_item(
        queue_name,
        escalation_level,
        app_id,
        subqueue_name
    )


@desktop.route("/internal/queues/queue_uuid>/renew", methods=["POST"])
@security_check("pipeman.handle_queue_items")
@api_error_handling
@injector.inject
def renew_queue_item(queue_uuid: str, nodb: NODBController = None):
    return nodb.renew_queue_item(queue_uuid)


@desktop.route("/internal/queues/<queue_uuid>/close-qc", methods=["POST"])
@security_check("pipeman.handle_queue_items")
@api_error_handling
@injector.inject
def close_qc_queue_item(queue_uuid: str, nodb: NODBController = None):
    return nodb.close_qc_item(
        queue_uuid,
        json_param("app_id", str),
        json_param("result", ReviewResult),
    )


@desktop.route("/internal/queues/<queue_uuid>/download-file", methods=["GET"])
@security_check("pipeman.download_files")
@api_error_handling
@injector.inject
def download_file(queue_uuid: str, nodb: NODBController = None):
    return nodb.download_file(queue_uuid)

@desktop.route("/internal/queues/<queue_uuid>/file-info", methods=["POST"])
@security_check("pipeman.view_file_info")
@api_error_handling
@injector.inject
def stream_file_information(queue_uuid: str, nodb: NODBController = None):
    return nodb.stream_file_information(queue_uuid, json_param("app_id", str))


@desktop.route("/internal/queues/<queue_uuid>/stream", methods=["POST"])
@security_check("pipeman.view_working_records")
@api_error_handling
@injector.inject
def stream_queue_item_records(queue_uuid: str, nodb: NODBController = None):
    return nodb.stream_queue_working_records(queue_uuid, json_param("app_id", str))


@desktop.route("/internal/working", methods=["GET"])
@security_check("pipeman.view_working_records")
@api_error_handling
@injector.inject
def find_working_record(nodb: NODBController = None):
    return nodb.stream_working_record(json_param("working_record_uuid", coerce=str))


@desktop.route("/internal/working/<record_uuid>", methods=["GET"])
@security_check("pipeman.view_working_records")
@api_error_handling
@injector.inject
def fetch_working_record(record_uuid: str, nodb: NODBController = None):
    return nodb.stream_working_record(record_uuid)


@desktop.route("/internal/working/<record_uuid>", methods=["POST"])
@security_check("pipeman.save_working_records")
@api_error_handling
@injector.inject
def save_working_record(record_uuid: str, nodb: NODBController = None):
    return nodb.save_record_actions(
        record_uuid,
        json_param("actions")
    )


@desktop.route("/internal/queues/<queue_uuid>/observations", methods=["GET"])
@security_check("pipeman.view_observations")
@api_error_handling
@injector.inject
def stream_queue_observations(queue_uuid: str, nodb: NODBController):
    return nodb.stream_queue_observations(queue_uuid, json_param("app_id", str))


@desktop.route("/internal/observations/<record_date>/<record_uuid>", methods=["GET"])
@security_check("pipeman.view_observations")
@api_error_handling
@injector.inject
def fetch_observation(record_uuid: str, record_date: str, nodb: NODBController = None):
    return nodb.stream_observation(record_uuid, record_date)


@desktop.route("/internal/platforms", methods=["GET"])
@security_check("pipeman.view_platforms")
@api_error_handling
@injector.inject
def find_platform_by_uuid( nodb: NODBController = None):
    return nodb.fetch_platform(json_param("platform_uuid", coerce=str))


@desktop.route("/internal/platforms/<platform_uuid>", methods=["GET"])
@security_check("pipeman.view_platforms")
@api_error_handling
@injector.inject
def fetch_platform_record(platform_uuid: str, nodb: NODBController = None):
    return nodb.fetch_platform(platform_uuid)


@desktop.route("/internal/platforms/create", methods=["POST"])
@security_check("pipeman.create_platforms")
@api_error_handling
@injector.inject
def create_platform_record(nodb: NODBController = None):
    return nodb.create_platform(
        wmo_id=json_param("wmo_id", coerce=str, default=None),
        wigos_id=json_param("wigos_id", coerce=str, default=None),
        platform_name=json_param("platform_name", coerce=str, default=None),
        platform_id=json_param("platform_id", coerce=str, default=None),
        platform_type=json_param("platform_type", coerce=str, default=None),
        start_date=json_param("start_date", coerce=AwareDateTime.fromisoformat, default=None),
        end_date=json_param("end_date", coerce=AwareDateTime.fromisoformat, default=None),
        status=json_param("status", coerce=PlatformStatus),
        embargo_data_days=json_param("embargo_data_days", coerce=int, default=None),
        skip_speed_check=json_param("skip_speed_check", coerce=bool, default=False),
        skip_land_check=json_param("skip_land_check", coerce=bool, default=False),
        mandatory_review=json_param("mandatory_review", coerce=bool, default=False),
        dedupe_time_window=json_param("dedupe_time_window", coerce=float, default=None),
        dedupe_distance_window=json_param("dedupe_distance_window", coerce=float, default=None),
        top_speed=json_param("top_speed", coerce=str, default=None),
        map_to_uuid=json_param("map_to_uuid", coerce=str, default=None),
    )


@desktop.route("/internal/platforms/<platform_uuid>", methods=["POST"])
@security_check("pipeman.update_platforms")
@api_error_handling
@injector.inject
def update_platform_record(platform_uuid: str, nodb: NODBController = None):
    return nodb.update_platform(
        platform_uuid=platform_uuid,
        wmo_id=json_param("wmo_id", coerce=str, default=None),
        wigos_id=json_param("wigos_id", coerce=str, default=None),
        platform_name=json_param("platform_name", coerce=str, default=None),
        platform_id=json_param("platform_id", coerce=str, default=None),
        platform_type=json_param("platform_type", coerce=str, default=None),
        start_date=json_param("start_date", coerce=AwareDateTime.fromisoformat, default=None),
        end_date=json_param("end_date", coerce=AwareDateTime.fromisoformat, default=None),
        status=json_param("status", coerce=PlatformStatus),
        embargo_data_days=json_param("embargo_data_days", coerce=int, default=None),
        skip_speed_check=json_param("skip_speed_check", coerce=bool, default=False),
        skip_land_check=json_param("skip_land_check", coerce=bool, default=False),
        mandatory_review=json_param("mandatory_review", coerce=bool, default=False),
        dedupe_time_window=json_param("dedupe_time_window", coerce=float, default=None),
        dedupe_distance_window=json_param("dedupe_distance_window", coerce=float, default=None),
        top_speed=json_param("top_speed", coerce=str, default=None),
        map_to_uuid=json_param("map_to_uuid", coerce=str, default=None),
    )

@desktop.route("/internal/platforms/search", methods=["GET"])
@security_check("pipeman.view_platforms")
@api_error_handling
@injector.inject
def search_platforms(nodb: NODBController = None):
    return nodb.search_stations(
        wmo_id=json_param("wmo_id", coerce=str, default=None),
        wigos_id=json_param("wigos_id", coerce=str, default=None),
        platform_id=json_param("platform_id", coerce=str, default=None),
        platform_name=json_param("platform_name", coerce=str, default=None),
        time_frame=json_param("time_frame", coerce=AwareDateTime.fromisoformat, default=None),
    )
