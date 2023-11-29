import pathlib

import click
import secrets

import yaml


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


@main.command
@click.argument("workflow_name")
@click.argument("config_file")
def update_workflow(workflow_name, config_file):
    _update_from_config_dir_file(workflow_name, pathlib.Path(config_file))


@main.command
@click.argument("config_directory")
def update_all_workflows(config_directory: str):
    p = pathlib.Path(config_directory)
    if not p.exists():
        print("config directory doesn't exist")
        exit(1)
    if not p.is_dir():
        print("config directory isn't a directory")
        exit(2)
    for f in p.iterdir():
        if f.name.endswith(".yaml"):
            _update_from_config_dir_file(f.name[:-5], f)
        elif f.name.endswith(".yml"):
            _update_from_config_dir_file(f.name[:-4], f)


def _update_from_config_dir_file(workflow_name: str, config_file: pathlib.Path):
    from cnodc.api.uploads import UploadController
    uc = UploadController(workflow_name)
    if not config_file.exists():
        print("config file doesn't exist")
        exit(1)
    try:
        with open(config_file, "r") as h:
            uc.update_workflow_config(yaml.safe_load(h) or {})
    except Exception as ex:
        print(f"An exception occurred while processing {config_file}")
        print(f"{ex.__class__.__name__}: {str(ex)}")
