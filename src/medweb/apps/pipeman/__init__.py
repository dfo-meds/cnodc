import typing as t

from gcflask.flasksystem import APIOperation
from gcflask.user import PermissionType

if t.TYPE_CHECKING:
    from gcapp.system import System


def init_plugin(s: System):
    from gcflask.flasksystem import FlaskSystemMixin
    if isinstance(s, FlaskSystemMixin):
        s.register_blueprint("medweb.apps.pipeman.routes.vocabularies", "vocabularies")
        s.register_blueprint("medweb.apps.pipeman.routes.desktop", "desktop")
        s.register_api_operation("desktop.queue_items_ready", "desktop.get_queue_report", ["pipeman.lock_queue_items"])
        s.register_api_operation("desktop.find_working_record", "desktop.find_working_record", ["pipeman.view_working_records"])
        s.register_api_operation("desktop.search_platforms", "desktop.search_platforms", ["pipeman.view_platforms"])
        s.register_api_operation("desktop.create_platform", "desktop.create_platform_record", ["pipeman.create_platforms"])
        s.register_api_operation("desktop.find_platform", "desktop.find_platform_by_uuid", ["pipeman.view_platforms"])
        s.register_dynamic_api_operation_builder("desktop", get_qc_actions)


def get_qc_actions() -> dict[str, APIOperation]:
    from autoinject import injector, auto
    from zirconium import ApplicationConfig

    @injector.inject
    def _get_qc_actions(config: ApplicationConfig = auto()) -> dict[str, APIOperation]:
        import flask
        qc = config.as_dict(("medweb", "pipeman", "batch_queues"))
        if qc is not None:
            actions = {}
            for qc_name, item in qc.items():
                queue_name = item.get("queue_name")
                escalation_level = item.get("escalation_level", 0)
                supported_results = item.get("supported_results", [])
                permissions = [
                    "pipeman.handle_queue_items",
                    f"pipeman.handle_queue_items.all | pipeman.handle_queue_items.{queue_name}",
                ]
                if escalation_level > 0:
                    permissions.append(f"pipeman.handle_queue_items.escalated.{queue_name} | pipeman.handle_queue_items.escalated.all")
                actions[f"batch_qc.{qc_name}.open"] = {
                    "endpoint": flask.url_for("desktop.lock_next_queue_item", _external=True),
                    "permissions": permissions,
                    "request_kwargs": {
                        "queue_name": queue_name,
                        "subqueue_name": item.get("subqueue_name", None),
                        "escalation_level": escalation_level
                    },
                    "metadata": {
                        "allowed_qc_results": supported_results or []
                    }
                }
            return actions
        return {}
    return _get_qc_actions()

