from autoinject import injector

from medweb.system import MedWebSystem


@injector.inject
def init_plugin(s: MedWebSystem = None):
    s.register_blueprint("medweb.apps.medsid.routes.auth", "auth")
    s.register_blueprint("medweb.apps.medsid.routes.user", "user")
    s.register_blueprint("medweb.apps.medsid.routes.base", "base")
    s.register_cli("medweb.apps.medsid.cli.user", "user")
    s.on_flask_init(_init_flask_app)


def _init_flask_app(app):
    from gcflask.auth import init_app as auth_init_app
    auth_init_app(app)
