import typing as t


def build_cli():
    from pipeman.boot import init_pipeman

    init_pipeman("cli", no_mp=True)
    import click

    commands: dict[str, click.Command | click.Group] = {}

    import pipeman_cli.user as users

    commands["user"] = t.cast(click.Group, users.main)

    import pipeman_cli.workflow as workflows

    commands["workflow"] = t.cast(click.Group, workflows.main)

    import pipeman_cli.glider as gliders

    commands["glider"] = t.cast(click.Group, gliders.main)

    from pipeman_cli import transcode

    commands["transcode"] = t.cast(click.Command, transcode.transcode)

    import pipeman_cli.service as process

    commands["service"] = t.cast(click.Group, process.service)

    import pipeman_cli.upgrade as db

    commands["upgrade"] = db.upgrade

    from pipeman_cli import dbman

    commands["db"] = t.cast(click.Group, dbman.db)

    from pipeman_cli import report

    commands["report"] = t.cast(click.Group, report.main)

    from pipeman_cli import products

    commands["products"] = t.cast(click.Group, products.products)

    from pipeman_cli import fastgrid

    commands["fastgrid"] = t.cast(click.Group, fastgrid.fastgrid)

    from pipeman_cli import platforms

    commands["platforms"] = t.cast(click.Group, platforms.platforms)

    from gcclick.clicksystem import ClickApp

    click_app = ClickApp(None)
    click_app.update_commands(commands)
    return click_app


if __name__ == "__main__":
    cli = build_cli()
    cli()
