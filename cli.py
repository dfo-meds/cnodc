import sys, pathlib

if __name__ == "__main__":

    sys.path.append(str(pathlib.Path(__file__).absolute().parent / "src"))
    from cnodc.cli.cli import main
    main()
