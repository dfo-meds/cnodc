import typing as t
import click


def build_cli():
    from pipeman.boot import init_cnodc
    from medsutil.multiclick import CommandLineInterface

    init_cnodc('cli')

    commands: dict[str, click.Command | click.Group] = {}

    import pipeman_cli.user as users
    commands["user"] = t.cast(click.Group, users.main)

    import pipeman_cli.workflow as workflows
    commands["workflow"] = t.cast(click.Group, workflows.main)

    import pipeman_cli.glider as gliders
    commands["glider"] = t.cast(click.Group, gliders.main)

    import pipeman_cli.transcode as transcode
    commands['transcode'] = t.cast(click.Command, transcode.transcode)

    import pipeman_cli.process as process
    commands['run_process'] = t.cast(click.Command, process.run_process)

    return CommandLineInterface(None, commands)


if __name__ == "__main__":
    cli = build_cli()
    cli()
