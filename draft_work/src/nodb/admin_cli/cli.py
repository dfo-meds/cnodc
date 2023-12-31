import pathlib

import click
import yaml
import zirconium as zr
import zrlog
from autoinject import injector
from cnodc.nodb import NODBController
import secrets

from cnodc.nodb.structures import UserStatus


@zr.configure
def register(config: zr.ApplicationConfig):
    config.register_file("./cnodc.toml")


@click.group
def main():
    zrlog.init_logging()


@main.command
def daemon():
    from nodb.server.run import LaunchManager
    lm = LaunchManager()
    lm.launch()


@main.command
@injector.inject
def web_setup(nodb: NODBController = None):
    import cnodc.nodb.structures as structures
    with nodb as db:
        db.grant_permission('_admin', '__admin__')
        admin_user = db.load_user("admin")
        if admin_user is None:
            admin_user = structures.NODBUser()
            admin_user.username = "admin"
        password = secrets.token_hex(16)
        admin_user.set_password(password)
        print(f"Admin password set to |{password}|")
        admin_user.status = UserStatus.ACTIVE
        admin_user.assign_role("_admin")
        db.save_user(admin_user)
        db.commit()


@main.command
@click.argument("config_name")
@click.argument("config_file")
@injector.inject
def update_upload_config(config_name: str, config_file: str):
    from nodb.web.workflows import update_workflow_config
    file = pathlib.Path(config_file).resolve()
    if not file.exists():
        raise ValueError(f"No such file: {config_file}")
    with open(file, "r") as h:
        config_data = yaml.safe_load(h.read())
        update_workflow_config(config_name, config_data)
