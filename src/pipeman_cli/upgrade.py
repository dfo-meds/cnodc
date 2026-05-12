from pprint import pprint
import pathlib

import click
import zrlog

from nodb.controller import NODBPostgresController
import zirconium as zr
from autoinject import injector


@click.command
@injector.inject
def upgrade(config: zr.ApplicationConfig = None):
    try:
        # Install default workflows
        wf_config_dir = config.get("pipeman", "workflows", "config_directory", default=None)
        zrlog.get_logger("cli.upgrade").notice("Updating configuration from %s", wf_config_dir or '-')
        if wf_config_dir:
            from pipeman_cli.workflow import _update_from_config_directory
            _update_from_config_directory(wf_config_dir)

        # DB Upgrade
        zrlog.get_logger("cli.upgrade").notice("Checking for database updates")
        pgc = NODBPostgresController()
        with pgc as db:
            from nodb._upgrade import Upgrader
            ug = Upgrader(db)
            ug.upgrade()

    except Exception as ex:
        zrlog.get_logger("upgrade").exception("exception during upgrade")
        raise SystemExit(1) from ex





