import click
import secrets


@click.group
def main():
    pass


@main.command
@click.argument("username")
def create_user(username):
    from cnodc.api.auth import UserController
    uc = UserController()
    password = secrets.token_urlsafe(16)
    uc.create_user(username, password)
    print(f"User created, password: {password}")


@main.command
@click.argument("username")
@click.argument("role_name")
def assign_role(username, role_name):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.assign_role(username, role_name)
    print(f"User {username} assigned to {role_name}")


@main.command
@click.argument("username")
@click.argument("role_name")
def unassign_role(username, role_name):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.unassign_role(username, role_name)
    print(f"User {username} unassigned from {role_name}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def grant_permission(role_name, permission):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.grant_permission(role_name, permission)
    print(f"Role {role_name} granted {permission}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def remove_permission(role_name, permission):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.remove_permission(role_name, permission)
    print(f"Role {role_name} ungranted {permission}")
