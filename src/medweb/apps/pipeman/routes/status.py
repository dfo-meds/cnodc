from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import web_error_handling
from autoinject import injector
import flask

from nodb import NODB

status = MultiLanguageBlueprint("status", __name__)


@status.route("/internal/status", methods=["GET"])
@web_error_handling
@injector.inject
def dashboard(db: NODB):
    with db as session:
        return flask.render_template("status.html")

@status.route("/internal/status/<workflow_name>", methods=["GET"])
@web_error_handling
@injector.inject
def status(workflow_name, db: NODB):
    with db as session:
        return flask.render_template("workflow_status.html")
