import pathlib
import sys
import flask
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from cnodc.boot.boot import build_cnodc_webapp
app = build_cnodc_webapp(__name__)

