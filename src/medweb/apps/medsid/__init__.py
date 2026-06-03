import pathlib
import typing as t

if t.TYPE_CHECKING:
    from gcapp.system import System


BASE_DIR = pathlib.Path(__file__).resolve().absolute().parent

def init_plugin(s: System):
    from gcflask.flasksystem import FlaskSystemMixin
    if isinstance(s, FlaskSystemMixin):
        s.register_blueprint("medweb.apps.medsid.routes.auth", "auth")
        s.register_blueprint("medweb.apps.medsid.routes.user", "user")
        s.register_blueprint("medweb.apps.medsid.routes.base", "base")
        s.on_flask_init(_init_flask_app)
        s.register_template_directory(BASE_DIR / 'templates')
    from gcclick.clicksystem import ClickSystemMixin
    if isinstance(s, ClickSystemMixin):
        s.register_cli("medweb.apps.medsid.cli", "access")



def _init_flask_app(app):
    from gcflask.auth import init_app as auth_init_app
    auth_init_app(app)
