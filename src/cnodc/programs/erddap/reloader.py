import typing as t

from autoinject import injector

from cnodc.programs.erddap import ErddapController, ReloadFlag
import cnodc.nodb as nodb
from cnodc.processing.workers.queue_worker import QueueWorker, QueueItemResult
from cnodc.util.exceptions import CNODCError


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

    def process_queue_item(self, item: nodb.NODBQueueItem) -> t.Optional[QueueItemResult]:
        if 'dataset_id' not in item.data or not item.data['dataset_id']:
            raise CNODCError(f"Missing dataset ID", "ERDDAPRELOAD", 1000, False)
        flag = ReloadFlag.SOFT
        if 'flag' in item.data and item.data['flag']:
            if item.data['flag'] == 1:
                flag = ReloadFlag.BAD_FILES
            elif item.data['flag'] == 2:
                flag = ReloadFlag.HARD
            else:
                self._log.warning(f'Unknown flag [{item.data['flag']}], defaulting to SOFT')
        cluster_name = self.get_config('default_cluster', None)
        if 'cluster_name' in item.data:
            cluster_name = item.data['cluster_name']
        self.erddap.reload_dataset(item.data['dataset_id'], flag=flag, cluster_name=cluster_name)
