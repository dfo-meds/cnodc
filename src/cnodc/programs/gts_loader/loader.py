from cnodc.codecs.gts import GtsCodec
from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t
from cnodc.nodb.loader import NODBLoader


class GtsLoaderWorker(QueueWorker):

    NAME = "gts_loader"
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loader = None
        self.set_defaults({
            'error_directory': None,
        })

    def on_start(self):
        self._loader = NODBLoader(
            process_name=GtsLoaderWorker.NAME,
            process_uuid=self.process_uuid,
            process_version=GtsLoaderWorker.VERSION,
            error_directory=self.get_config('error_directory'),
            decoder=GtsCodec(halt_flag=self.halt_flag),
            default_values={
                'source_name': 'gts',
                'program_name': 'gtspp',
                'status': structures.ObservationStatus.UNVERIFIED,
                'processing_level': structures.ProcessingLevel.ADJUSTED,
            },
            halt_flag=self.halt_flag
        )

    def process_queue_item(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        return self._loader.load_file_from_queue(item)
