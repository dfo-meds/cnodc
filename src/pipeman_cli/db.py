import click
from nodb.controller import NODBPostgresController

@click.command
def upgrade():
    pgc = NODBPostgresController()
    with pgc as db:
        from nodb._upgrade import Upgrader
        ug = Upgrader(db)
        try:
            ug.upgrade()
        except Exception:
            exit(1)
