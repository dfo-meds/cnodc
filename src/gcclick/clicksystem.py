import click

from gcapp.system import System
from medsutil.dynamic import dynamic_object
from medsutil.exceptions import CodedError


class GCClickError(CodedError):
    """ Error class for the system. """
    CODE_SPACE='GCCLICK'


class ClickApp(click.MultiCommand):
    """ Stores multiple commands and displays them. """

    def __init__(self, app=None):
        """ Constructor.

            If app is not None, then it should be a flask app and we will build a fake request context around it.
        """
        self._commands: dict[str, click.Command] = {}
        self._app = app
        super().__init__()

    def add_command(self, name: str, command: click.Command):
        """ Adds a command or command group to the app."""
        self._commands[name] = command

    def list_commands(self, ctx):
        """ Lists the commands in the app. """
        return self._commands.keys()

    def get_command(self, ctx, name):
        """ Retrieves a command from the app. """
        try:
            return self._commands[name]
        except KeyError:
            return None

    def __call__(self, *args, **kwargs):
        """ Wraps MultiCommand's __call__ if _app was provided to start a test request context. """
        if self._app:
            with self._app.app_context():
                with self._app.test_request_context():
                    super().__call__(*args, **kwargs)
        else:
            super().__call__(*args, **kwargs)


class ClickSystemMixin(System):
    """ Extension of System to support click commands. """

    def __init__(self):
        super().__init__()
        self._click_app = None
        self._click_groups: list[tuple[str, str, str]] = []

    def register_cli(self, module: str, command_name: str, register_as: str = None):
        """Registers a click command group to add to the main CLI application."""
        self._click_groups.append((module, command_name, register_as or command_name))

    def init_click_app(self, click_app: ClickApp):
        """ Initializes a click app. """
        self._click_app = click_app
        self.events.fire("init.click.before")

        # add all the commands we registered
        for module_name, module_object, registry_name in self._click_groups:
            self._click_app.add_command(registry_name, dynamic_object(f"{module_name}.{registry_name}"))

        # default setup command
        self._click_app.add_command("setup", click.Command("setup", callback=self.setup))

        # default cleanup command
        self._click_app.add_command("cleanup", click.Command("cleanup", callback=self.cleanup))

        # additional setup and cleanup
        self.events.fire("init.click")
        self.events.fire("init.click.after")
