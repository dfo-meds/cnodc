from cnodc.codecs.gts import GtsCodec
from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t
from cnodc.nodb.loader import NODBLoader


class GtsLoaderWorker(QueueWorker):

    NAME = "gts_loader"
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(log_name=GtsLoaderWorker.NAME, *args, **kwargs)
        self._loader: t.Optional[NODBLoader] = None
        self.set_defaults({
            'error_directory': None,
        })

    def on_start(self):
        self._loader = NODBLoader(
            log_name="cnodc.gts_loader",
            processor_name=GtsLoaderWorker.NAME,
            processor_uuid=self.process_uuid,
            processor_version=GtsLoaderWorker.VERSION,
            error_directory=self.get_config('error_directory'),
            decoder=GtsCodec(halt_flag=self.halt_flag),
            default_metadata={
                'CNODCSource': 'gts',
                'CNODCProgram': 'gtspp',
                'CNODCStatus': 'UNVERIFIED',
                'CNODCLevel': 'ADJUSTED',
            },
            halt_flag=self.halt_flag
        )

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        return self._loader.process_queue_item(item)
