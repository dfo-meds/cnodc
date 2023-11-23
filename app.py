import pathlib
import sys
import flask
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from cnodc.boot.boot import init_cnodc, init_flask

init_cnodc("web")

app = flask.Flask(__name__)

init_flask(app)
