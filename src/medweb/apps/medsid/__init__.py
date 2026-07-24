import pathlib
import typing as t


if t.TYPE_CHECKING:
    from gcapp.system import System


BASE_DIR = pathlib.Path(__file__).resolve().absolute().parent

def init_plugin(s: System):
    from gcflask.flasksystem import FlaskSystemMixin
    if isinstance(s, FlaskSystemMixin):
        from gcflask.nav import NavItem
        from gcapp.i18n import TString
        s.register_blueprint("medweb.apps.medsid.routes.auth", "auth")
        s.register_blueprint("medweb.apps.medsid.routes.user", "user")
        s.register_blueprint("medweb.apps.medsid.routes.base", "base")
        s.on_flask_init(_init_flask_app)
        s.register_template_directory(BASE_DIR / 'templates')
        s.register_menu_item("topnav", "home", NavItem(
            TString("medsid.menu.topnav.home"),
            "base.home",
            require_permissions=["__authenticated__"],
        ))
        s.register_menu_item("topnav", "myself", NavItem(
            TString("medsid.menu.user.myself"),
            "user.me",
            require_permissions=["__authenticated__"],
        ))
        s.register_menu_item("topnav", "change_password", NavItem(
            TString("medsid.menu.topnav.change_password"),
            "user.change_password",
            require_permissions=["__authenticated__"],
        ))
        s.register_menu_item("topnav", "edit", NavItem(
            TString("medsid.menu.topnav.edit"),
            "user.edit",
            require_permissions=["__authenticated__"],
        ))
        s.register_menu_item("topnav", "login", NavItem(
            TString("medsid.menu.topnav.login"),
            "auth.login",
            require_permissions=["__anonymous__"],
        ))
        s.register_menu_item("topnav", "logout", NavItem(
            TString("medsid.menu.topnav.logout"),
            "auth.logout",
            require_permissions=["__authenticated__"],
        ))
    from gcclick.clicksystem import ClickSystemMixin
    if isinstance(s, ClickSystemMixin):
        s.register_cli("medweb.apps.medsid.cli", "access")



def _init_flask_app(app):
    from gcflask.auth import init_app as auth_init_app
    auth_init_app(app)
