import pathlib
import sys
ROOT_DIR = pathlib.Path(__file__).parent.absolute().resolve()
sys.path.append(str(ROOT_DIR / "src"))

from pipeman_web.boot import build_cnodc_webapp
app = build_cnodc_webapp(__name__, sys.argv[0].replace("\\", "/").endswith("flask/__main__.py"))
