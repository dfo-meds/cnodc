import sys, pathlib
import click

if __name__ == "__main__":

    sys.path.append(str(pathlib.Path(__file__).absolute().parent / "src"))
    from cnodc.cli.cli import main as main_cli
    from nodb.admin_cli.cli import main as admin_cli
    group = click.Group(commands={"user": main_cli, "admin": admin_cli})
    group()
