import click
from nodb.controller import NODBPostgresController
import zirconium as zr

@click.command
def upgrade(config: zr.ApplicationConfig):
    try:

        # DB Upgrade
        pgc = NODBPostgresController()
        with pgc as db:
            from nodb._upgrade import Upgrader
            ug = Upgrader(db)
                ug.upgrade()

        # Install default workflows
        wf_config_dir = config.get(("pipeman", "workflows", "config_directory"), None)
        if wf_config_dir:
            from pipeman_cli.workflow import _update_from_config_directory
            _update_from_config_directory(wf_config_dir)

    except Exception as ex:
        raise SystemExit(1) from ex





