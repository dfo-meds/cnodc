import click

from pipeman.programs.platforms.jcomm import import_from_jcomm_file


@click.group()
def platforms(): ...


@platforms.command
@click.argument("jcomm_file")
def import_jcomm(jcomm_file):
    import_from_jcomm_file(jcomm_file)
