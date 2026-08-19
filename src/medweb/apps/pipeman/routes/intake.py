import flask
from autoinject import injector

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import security_check, api_error_handling
from medweb.apps.pipeman.intake import IntakeManager

intake = MultiLanguageBlueprint('intake', __name__)

@intake.route("/intake", methods=["GET"])
@security_check("pipeman.submit_files")
@api_error_handling
@injector.inject
def list_available_workflows(im: IntakeManager = None):
    return im.list_workflows()

@intake.route("/intake/<workflow_name>", methods=["GET"])
@security_check("pipeman.submit_files")
@api_error_handling
@injector.inject
def workflow_info(workflow_name: str, im: IntakeManager = None):
    return im.workflow_info(workflow_name)

@intake.route("/intake/<workflow_name>", methods=["POST"])
@security_check("pipeman.submit_files")
@api_error_handling
@injector.inject
def submit_file(workflow_name: str, im: IntakeManager = None):
    return im.submit_file(
        workflow_name,
        flask.request.data,
        {
            x.lower(): flask.request.headers.get(x) or ''
            for x in flask.request.headers.keys()
        }
    )

@intake.route("/intake/<workflow_name>/<request_id>", methods=["POST"])
@security_check("pipeman.submit_files")
@api_error_handling
@injector.inject
def submit_followup_file(workflow_name: str, request_id: str, im: IntakeManager = None):
    return im.submit_file(
        workflow_name,
        flask.request.data,
        {
            x.lower(): flask.request.headers.get(x) or ''
            for x in flask.request.headers.keys()
        },
        request_id=request_id
    )

@intake.route("/intake/<workflow_name>/<request_id>/cancel", methods=["POST"])
@security_check("pipeman.submit_files")
@api_error_handling
@injector.inject
def cancel_upload(workflow_name: str, request_id: str, im: IntakeManager = None):
    return im.cancel_upload(workflow_name, request_id)