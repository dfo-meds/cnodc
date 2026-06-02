import getpass

import click
from autoinject import injector, auto

from medweb.apps.medsid.controller import AccessController


@click.group()
def user(): ...


@user.command
@click.option("--display", default="")
@click.option("--entry-mode", default="input")
@click.option("--password", default=None)
@click.option("--email", default=None)
@click.argument("username")
@injector.inject
def create(username: str,
           email: str,
           display: str = "",
           password: str | None = None,
           entry_mode: str = "input",
           ac: AccessController = auto()):
    """Create a user account."""
    ac.create_user(username, _get_password(entry_mode, password), email, display or username)


@user.command
@click.option("--display", default="")
@click.option("--entry-mode", default="input")
@click.option("--password", default=None)
@click.option("--email", default=None)
@click.argument("username")
@injector.inject
def reset_password(username: str,
                   password: str | None = None,
                   entry_mode: str = "input",
                   ac: AccessController = auto()):
    """Create a user account."""
    ac.update_user(
        username=username,
        password=_get_password(entry_mode, password)
    )


@user.command
@click.argument("username")
@injector.inject
def unlock(username: str, ac: AccessController = auto()):
    ac.update_user(
        username=username,
        locked_time=None
    )


@user.command
@click.argument("username")
@injector.inject
def enable_api_access(username: str, ac: AccessController = auto()):
    ac.update_user(
        username=username,
        api_access=True
    )


@user.command
@click.argument("username")
@injector.inject
def enable_api_access(username: str, ac: AccessController = auto()):
    ac.update_user(
        username=username,
        api_access=False
    )


@user.command
@click.argument("username")
@injector.inject
def enable(username: str, ac: AccessController = auto()):
    ac.update_user(
        username=username,
        enabled=True
    )


@user.command
@click.argument("username")
@injector.inject
def disable(username: str, ac: AccessController = auto()):
    ac.update_user(
        username=username,
        enabled=False
    )


@user.command
@click.argument("username")
@click.argument("identifier")
@click.argument("expiry_days", type=click.INT)
@injector.inject
def create_api_key(username: str, identifier: str, expiry_days: int, ac: AccessController = auto()):
    key = ac.create_api_key(username, identifier, expiry_days)
    print(f"Access Key: {key}")


@user.command
@click.argument("username")
@click.argument("identifier")
@click.argument("expiry_days", type=click.INT)
@click.argument("leave_old_active_days", type=click.INT)
@injector.inject
def rotate_api_key(username: str, identifier: str, expiry_days: int, leave_old_active_days: int, ac: AccessController = auto()):
    key = ac.rotate_api_key(username, identifier, expiry_days, leave_old_active_days)
    print(f"Access Key: {key}")


@user.command
@click.argument("username")
@click.argument("identifier")
@injector.inject
def deactivate_api_key(username: str, identifier: str, ac: AccessController = auto()):
    ac.update_api_key(username, identifier, False)


@user.command
@click.argument("username")
@click.argument("identifier")
@injector.inject
def reactivate_api_key(username: str, identifier: str, ac: AccessController = auto()):
    ac.update_api_key(username, identifier, True)


def _get_password(entry_mode: str, password: str | None = None) -> str | None:
    """Retrieve a password either from the input, at random, or from the passed argument.

    Parameters
    ----------
    entry_mode: str
        One of "random", "input" or "argument". If random, a random password is generated. If input, the password is
        requested using getpass.getpass(). Otherwise, the argument is used.
    password: str
        The password from the option.
    Returns
    -------
    A password to use for the user.
    """
    if entry_mode == "random":  # noqa: S105
        return None
    elif entry_mode == "input":  # noqa: S105
        pw_test = getpass.getpass("Password: ")
        pw_test2 = getpass.getpass("Retype Password: ")
        while not pw_test == pw_test2:
            print("Passwords do not match, please try again")
            pw_test = getpass.getpass("Password: ")
            pw_test2 = getpass.getpass("Retype Password: ")
        return pw_test
    else:
        return password