import typing as t
import click


def build_cli():
    from pipeman.boot import init_pipeman

    init_pipeman('cli', no_mp=True)

    commands: dict[str, click.Command | click.Group] = {}

    import pipeman_cli.user as users
    commands["user"] = t.cast(click.Group, users.main)

    import pipeman_cli.workflow as workflows
    commands["workflow"] = t.cast(click.Group, workflows.main)

    import pipeman_cli.glider as gliders
    commands["glider"] = t.cast(click.Group, gliders.main)

    import pipeman_cli.transcode as transcode
    commands['transcode'] = t.cast(click.Command, transcode.transcode)

    import pipeman_cli.service as process
    commands['service'] = t.cast(click.Group, process.service)

    import pipeman_cli.upgrade as db
    commands['upgrade'] = db.upgrade

    import pipeman_cli.dbman as dbman
    commands['db'] = t.cast(click.Group, dbman.db)

    from medsutil.multiclick import CommandLineInterface
    return CommandLineInterface(None, commands)


if __name__ == "__main__":
    cli = build_cli()
    cli()
