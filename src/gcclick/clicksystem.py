import click

from gcapp.system import System
from medsutil.dynamic import dynamic_object
from medsutil.exceptions import CodedError


class GCClickError(CodedError): CODE_SPACE='GCCLICK'


class ClickApp(click.MultiCommand):

    def __init__(self, app=None):
        self._commands: dict[str, click.Command] = {}
        self._app = app

    def add_command(self, name, command: click.Command):
        self._commands[name] = command

    def list_commands(self, ctx):
        return self._commands.keys()

    def get_command(self, ctx, name):
        try:
            return self._commands[name]
        except KeyError:
            return None

    def __call__(self, *args, **kwargs):
        if self._app:
            with self._app.app_context():
                with self._app.test_request_context():
                    super().__call__(*args, **kwargs)
        else:
            super().__call__(*args, **kwargs)


class ClickSystemMixin(System):

    def __init__(self):
        super().__init__()
        self._click_app = None
        self._click_groups: list[tuple[str, str, str]] = []

    def register_cli(self, module: str, command_name: str, register_as: str = None):
        """Register a click command group to add to the main CLI application."""
        self._click_groups.append((module, command_name, register_as or command_name))

    def init(self, *args, cli: ClickApp, app=None, **kwargs):
        self._click_app = cli
        if app is not None:
            self._click_app._app = app
        super().init(*args, app=app, cli=cli, **kwargs)

    def _subclass_init(self):
        super()._subclass_init()
        self.events.fire("init.click.before")
        for module_name, module_object, registry_name in self._click_groups:
            self._click_app.add_command(registry_name, dynamic_object(f"{module_name}.{registry_name}"))
        self.events.fire("init.click")
        self.events.fire("init.click.after")
