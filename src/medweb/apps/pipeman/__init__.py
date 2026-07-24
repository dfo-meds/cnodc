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
        s.register_dynamic_api_operation_builder("desktop", get_qc_actions)


def get_qc_actions() -> dict[str, APIOperation]:
    from autoinject import injector, auto
    from zirconium import ApplicationConfig

    @injector.inject
    def _get_qc_actions(config: ApplicationConfig = auto()) -> dict[str, APIOperation]:
        qc = config.as_dict(("medweb", "pipeman", "endpoints"))
        if qc is not None:
            actions = {}
            for action, item in qc.items():
                endpoint: str = str(item.get("endpoint"))
                permissions: PermissionType = item.get("permissions", None)
                request_kwargs: dict[str, t.Any] = item.get("request_kwargs", {})
                url_kwargs: dict[str, t.Any] = item.get("url_kwargs", {})
                actions[action] = {
                    "endpoint": endpoint,
                    "permissions": permissions,
                    "request_kwargs": request_kwargs,
                    "url_kwargs": url_kwargs,
                }
            return actions
        return {}
    return _get_qc_actions()

