import pathlib

ROOT_DIR = pathlib.Path(__file__).absolute().resolve()
while ROOT_DIR.name in ('__init__.py', 'medsutil', 'src'):
    ROOT_DIR = ROOT_DIR.parent
ROOT_DIR = ROOT_DIR.absolute().resolve()
