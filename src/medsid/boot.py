import typing as t


if t.TYPE_CHECKING:
    from gcflask.flasksystem import FlaskSystemMixin


def boot_medsid() -> FlaskSystemMixin:
    from gcapp.boot import boot_system
    system: FlaskSystemMixin = boot_system('medsid', system_cls="gcflask.flasksystem.FlaskSystemMixin", init_hooks=[
        _register_blueprints,
        _register_menu_items
    ])
    system.on_flask_init(_init_flask_app)
    return system


def _init_flask_app(app):
    from gcflask.auth import init_app as auth_init_app
    auth_init_app(app)


def _register_blueprints(s: FlaskSystemMixin):
    s.register_blueprint("medsid.routes.auth", "auth")
    s.register_blueprint("medsid.routes.user", "user")
    s.register_blueprint("medsid.routes.base", "base")


def _register_menu_items(s: FlaskSystemMixin):
    ...
