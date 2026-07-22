import pathlib
import sys
ROOT_DIR = pathlib.Path(__file__).parent.absolute().resolve()
sys.path.append(str(ROOT_DIR / "src"))

from medweb.app import create_app
app = create_app()

