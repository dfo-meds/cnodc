
import click
import zirconium as zr
import zrlog
from autoinject import injector

from nodb.controller import NODBPostgresController


@click.command
@injector.inject
def upgrade(config: zr.ApplicationConfig = None):
    try:
        # DB Upgrade
        zrlog.get_logger("cli.upgrade").notice("Checking for database updates")
        pgc = NODBPostgresController()
        with pgc as db:
            from nodb._upgrade import Upgrader

            ug = Upgrader(db)
            ug.upgrade()

        # Install default workflows
        wf_config_dir = config.get(
            "pipeman", "workflows", "config_directory", default=None
        )
        zrlog.get_logger("cli.upgrade").notice(
            "Updating configuration from %s", wf_config_dir or "-"
        )
        if wf_config_dir:
            from pipeman_cli.workflow import _update_from_config_directory

            _update_from_config_directory(wf_config_dir)

        # Install default products
        pd_config_dir = config.get(
            "pipeman", "products", "config_directory", default=None
        )
        if pd_config_dir:
            from pipeman_cli.products import _update_from_product_file

            _update_from_product_file(pd_config_dir)

    except Exception as ex:
        zrlog.get_logger("upgrade").exception("exception during upgrade")
        raise SystemExit(1) from ex
