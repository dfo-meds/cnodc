import pathlib
import typing as t

from medsutil.ocproc2.codecs import GtsCodec
from pipeman.processing.payload_worker import FileWorkflowWorker
from pipeman.processing.payloads import FilePayload
from pipeman.processing.queue_worker import QueueItemResult


class GTSSeparatorWorker(FileWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="gts_separator",
            process_version="1.0",
            **kwargs
        )

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        file = self.download_to_temp_file()
        self.split_into_messages(file)

    def split_into_messages(self, file: pathlib.Path):
        decoder = GtsCodec()
        with open(file, "rb") as h:
            for x in decoder.buffered_decode_messages(self._halt_flag.read_all(h), no_decode=True):
                if x.original:
                    self.handle_new_message(x.original)

    def handle_new_message(self, message: bytes | bytearray):
        header, message = message.split(b"\n", maxsplit=1)
        


