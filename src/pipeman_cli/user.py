from datetime import timedelta
import click


@click.group()
def main(): ...


@main.command
@click.argument("username")
def create(username):
    from pipeman.users import UserController
    from medsutil import secure
    uc = UserController()
    password = secure.generate_secure_random_password()
    uc.create_user(username, password)
    print(f"User created, password: {password}")


@main.command
@click.argument("username")
@click.argument("expiry_seconds", type=int)
def rotate_password(username, expiry_seconds):
    from pipeman.users import UserController
    from medsutil.awaretime import AwareDateTime
    from medsutil import secure
    uc = UserController()
    password = secure.generate_secure_random_password()
    if expiry_seconds is None or expiry_seconds < 0:
        uc.update_user(username, password)
        dt = None
    else:
        uc.update_user(username, password, old_expiry_seconds=expiry_seconds)
        dt = AwareDateTime.now() + timedelta(seconds=expiry_seconds)
    print(f"User password rotated, password: {password}")
    if dt is not None:
        print(f"  Old password valid until: {dt.isoformat()}")


@main.command
@click.argument("username")
def activate(username):
    from pipeman.users import UserController
    uc = UserController()
    uc.update_user(username, is_active=True)
    print(f"User activated")


@main.command
@click.argument("username")
def deactivate(username):
    from pipeman.users import UserController
    uc = UserController()
    uc.update_user(username, is_active=False)
    print(f"User deactivated")


@main.command
@click.argument("username")
@click.argument("role_name")
def assign(username, role_name):
    from pipeman.users import UserController
    uc = UserController()
    uc.assign_role(username, role_name)
    print(f"User {username} assigned to {role_name}")


@main.command
@click.argument("username")
@click.argument("role_name")
def unassign(username, role_name):
    from pipeman.users import UserController
    uc = UserController()
    uc.unassign_role(username, role_name)
    print(f"User {username} unassigned from {role_name}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def grant(role_name, permission):
    from pipeman.users import UserController
    uc = UserController()
    uc.grant_permission(role_name, permission)
    print(f"Role {role_name} granted {permission}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def ungrant(role_name, permission):
    from pipeman.users import UserController
    uc = UserController()
    uc.remove_permission(role_name, permission)
    print(f"Role {role_name} ungranted {permission}")
