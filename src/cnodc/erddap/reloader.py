import typing as t

from autoinject import injector

from cnodc.erddap import ErddapController, ReloadFlag
from cnodc.nodb import structures as structures
from cnodc.process import QueueWorker, QueueItemResult
from draft_work.src.cnodc.exc import CNODCError


class ERDDAPReloadWorker(QueueWorker):

    erddap: ErddapController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="erddap_reload",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'erddap_reload',
            'default_cluster': None
        })

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[QueueItemResult]:
        if 'dataset_id' not in item.data or not item.data['dataset_id']:
            raise CNODCError(f"Missing dataset ID", "ERDDAPRELOAD", 1000, False)
        flag = ReloadFlag.SOFT
        if 'flag' in item.data and item.data['flag']:
            if item.data['flag'] == 1:
                flag = ReloadFlag.BAD_FILES
            elif item.data['flag'] == 2:
                flag = ReloadFlag.HARD
        cluster_name = item.data['cluster_name'] if 'cluster_name' in item.data else self.get_config('default_cluster', None)
        if not self.erddap.reload_dataset(item.data['dataset_id'], flag=flag, cluster_name=cluster_name):
            return QueueItemResult.RETRY