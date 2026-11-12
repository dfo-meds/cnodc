
from autoinject import injector

from nodb.queue import NODBQueueItem
from pipeman.exceptions import CNODCError
from pipeman.processing.queue_worker import QueueItemResult, QueueWorker
from pipeman.programs.erddap import ErddapController, ReloadFlag


class ERDDAPReloadWorker(QueueWorker):
    erddap: ErddapController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(process_name="erddap_reload", process_version="1.0", **kwargs)
        self.set_defaults({"queue_name": "erddap_reload", "default_cluster": None})

    def process_queue_item(self, item: NODBQueueItem) -> QueueItemResult | None:
        if "dataset_id" not in item.data or not item.data["dataset_id"]:
            raise CNODCError("Missing dataset ID", "ERDDAPRELOAD", 1000, False)
        flag = ReloadFlag.SOFT
        if item.data.get("flag"):
            if item.data["flag"] == 1:
                flag = ReloadFlag.BAD_FILES
            elif item.data["flag"] == 2:
                flag = ReloadFlag.HARD
            else:
                self._log.warning(
                    f"Unknown flag [{item.data['flag']}], defaulting to SOFT"
                )
        cluster_name = self.get_config("default_cluster", None)
        if "cluster_name" in item.data:
            cluster_name = item.data["cluster_name"]
        self.erddap.reload_dataset(
            item.data["dataset_id"], flag=flag, cluster_name=cluster_name
        )
