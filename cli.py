import pathlib
import sys
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

if __name__ == "__main__":
    from pipeman_cli.cli import build_cli
    cli = build_cli()
    cli()
