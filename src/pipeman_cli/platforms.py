import click


@click.group()
def platforms(): ...


@platforms.command
def download():
    from pipeman.programs.platforms.worker import all_code_downloads
    for f in all_code_downloads:
        f()

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


