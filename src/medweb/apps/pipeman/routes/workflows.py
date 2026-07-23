import flask

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import security_check

workflow = MultiLanguageBlueprint('workflow', __name__, url_prefix='/pipeman')

@workflow.route("/api/submit/<workflow_name>", methods=["POST"])
@security_check("pipeman.submit_files", is_admin=True)
def submit_file(workflow_name: str):
    return flask.abort(404)


@workflow.route("/api/submit/<workflow_name>/<request_id>", methods=["POST"])
@security_check("pipeman.submit_files", is_api=True)
def submit_next_file(workflow_name: str, request_id: str):
    return flask.abort(404)


@workflow.route("/api/submit/<workflow_name>/<request_id>/cancel", methods=["POST"])
@security_check("pipeman.submit_files", is_api=True)
def cancel(workflow_name: str, request_id: str):
    return flask.abort(404)
