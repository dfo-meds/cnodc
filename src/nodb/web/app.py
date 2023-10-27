import flask
from autoinject import injector

app = flask.Flask(__name__)


@app.before_request
@injector.inject
def before_request():
    pass


@app.teardown_request
def teardown_request(ex=None):
    pass
