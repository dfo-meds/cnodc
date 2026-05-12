import click
import zrlog

from nodb.controller import NODBPostgresController
import zirconium as zr
from autoinject import injector

@click.command
@injector.inject
def upgrade(config: zr.ApplicationConfig = None):
    try:

        # Config Info


        # DB Upgrade
        zrlog.get_logger("cli.upgrade").notice("Checking for database updates")
        pgc = NODBPostgresController()
        with pgc as db:
            from nodb._upgrade import Upgrader
            ug = Upgrader(db)
            ug.upgrade()

        # Install default workflows
        wf_config_dir = config.get(("pipeman", "workflows", "config_directory"), None)
        zrlog.get_logger("cli.upgrade").notice("Updating configuration from %s", wf_config_dir or 'N/A')
        from pipeman_cli.workflow import _update_from_config_directory
        _update_from_config_directory(wf_config_dir)

    except Exception as ex:
        raise SystemExit(1) from ex





