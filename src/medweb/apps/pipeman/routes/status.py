from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import web_error_handling
from autoinject import injector
import flask

from nodb import NODB
from nodb.workflow import NODBUploadWorkflow

status = MultiLanguageBlueprint("status", __name__)


@status.route("/internal/status", methods=["GET"])
@web_error_handling
@injector.inject
def dashboard(nodb: NODB):
    with nodb as db:
        processes = [
            process
            for process in db.fetch_processes()
            if process['exited'] != 'Y'
        ]
        items = db.fetch_queue_summary()
        workflow_links = []
        for tag in db.fetch_queue_tags():
            workflow_links.append((tag, flask.url_for("status.status", workflow_name=tag)))
        return flask.render_template(
            "status.html",
            title="Pipeman Status",
            processes=processes,
            items=items,
            item_keys=sorted(items.keys()),
            workflow_links=workflow_links
        )

@status.route("/internal/status/<workflow_name>", methods=["GET"])
@web_error_handling
@injector.inject
def status(workflow_name, nodb: NODB):
    with nodb as db:
        workflow = NODBUploadWorkflow.find_by_name(db, workflow_name)
        if not workflow:
            return flask.abort(404)
        items = db.fetch_queue_summary(workflow_name)
        return flask.render_template(
            "workflow_status.html",
            title=workflow_name,
            items=items,
            item_keys=sorted(items.keys()),
        )
