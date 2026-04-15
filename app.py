import pathlib
import sys
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from pipeman_web.boot import build_cnodc_webapp
app = build_cnodc_webapp(__name__)
