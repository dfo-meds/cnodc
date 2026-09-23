import click


@click.group()
def platforms(): ...


@platforms.command
@click.argument("jcomm_file")
def import_jcomm(jcomm_file):
    from pipeman.programs.platforms.jcomm import import_from_jcomm_file
    import_from_jcomm_file(jcomm_file)


@platforms.command
def download_oceanops():
    from pipeman.programs.platforms.oceanops import download_ocean_ops
    download_ocean_ops()


@platforms.command
def download_ccgs():
    from pipeman.programs.platforms.ccgs import download_ccgs_ships
    download_ccgs_ships()


@platforms.command
def download_ices():
    from pipeman.programs.platforms.ices import download_ices_codes
    download_ices_codes()


