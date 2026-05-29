import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from medsid.app import app

if __name__ == "__main__":
    app.run()