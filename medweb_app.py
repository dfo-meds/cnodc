import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from medweb.app import create_app

if __name__ == "__main__":
    app = create_app()
    app.run()
