from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission, web_error_handling, deny_permission
from autoinject import injector

from medweb.apps.pipeman.nodb_manager import NODBController

qc = MultiLanguageBlueprint("qc", __name__)

@qc.route("/internal/queues/next/<queue_name>", methods=["POST"])
@require_permission("handle_queue_items")
@web_error_handling
@injector.inject
def lock_next_queue_item(queue_name: str, nodb: NODBController = None):
    if not nodb.has_queue_access(queue_name):
        return deny_permission(is_api=True)



