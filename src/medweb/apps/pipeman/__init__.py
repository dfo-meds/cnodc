import typing as t
if t.TYPE_CHECKING:
    from gcapp.system import System

def init_plugin(s: System):
    from gcflask.flasksystem import FlaskSystemMixin
    if isinstance(s, FlaskSystemMixin):
        s.register_blueprint("medweb.apps.pipeman.routes.vocabularies", "vocabularies")
