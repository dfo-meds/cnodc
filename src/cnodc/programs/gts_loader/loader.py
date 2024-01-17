from cnodc.codecs.gts import GtsCodec
from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t
from cnodc.programs.nodb_intake.loader import NODBLoader


class GTSLoadWorker(QueueWorker):

    NAME = "gts_loader"
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.gts_loader_worker", *args, **kwargs)
        self._loader: t.Optional[NODBLoader] = None
        self.set_defaults({
            'queue_name': 'gts_load',
            'error_directory': None,
        })

    def on_start(self):
        self._loader = NODBLoader(
            log_name="cnodc.gts_loader",
            processor_name=GTSLoadWorker.NAME,
            processor_uuid=self.process_uuid,
            processor_version=GTSLoadWorker.VERSION,
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
