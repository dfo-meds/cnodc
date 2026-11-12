import pathlib
import sys

TEST_DIR = pathlib.Path(__file__).absolute().parent

sys.path.append(str(TEST_DIR.parent / "src"))
