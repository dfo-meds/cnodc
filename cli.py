import pathlib
import sys
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

if __name__ == "__main__":
    from cnodc.boot.boot import init_cnodc
    from cnodc.cli.commands import main

    init_cnodc("cli")

    main()
