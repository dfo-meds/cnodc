import pathlib

from autoinject import injector, auto

from medsutil.awaretime import AwareDateTime
from medsutil.byteseq import ByteSequenceReader
from medsutil.exceptions import CodedError
from medsutil.storage import StorageController, FilePath
from medsutil.wis import WISMessageParser, WISMessage
from pipeman.processing.payload_worker import FileWorkflowWorker
from pipeman.processing.payloads import FilePayload
from pipeman.processing.queue_worker import QueueItemResult


class WISSeparatorWorker(FileWorkflowWorker):

    storage: StorageController = auto()

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="wis_separator",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            "next_queue": "workflow_continue",
            "upload_directory": None,
        })

    def process_payload(self, payload: FilePayload) -> QueueItemResult | None:
        file = self.download_to_temp_file()
        should_continue_original = self.split_into_messages(file)
        if not should_continue_original:
            self.prevent_default_progression()

    def split_into_messages(self, file: pathlib.Path) -> bool:
        with open(file, "rb") as h:
            parser = WISMessageParser(ByteSequenceReader(self._halt_flag.read_all(h)))
            first_message = None
            second_message = None
            iterator = iter(parser.split_messages())
            try:
                first_message = next(iterator)
                second_message = next(iterator)
            except StopIteration:
                pass
            if first_message is None:
                return False
            if second_message is None:
                return True
            with self.storage.handle(self.get_config("upload_directory")) as handle:
                if not (handle.exists() and handle.is_dir()):
                    raise CodedError("Storage directory does not exist or is not a directory", 1000, code_space="WIS-SEPARATOR")
                self._save_new_message(first_message, handle)
                self._save_new_message(second_message, handle)
                for message in iterator:
                    self._save_new_message(message, handle)
                return False

    def _save_new_message(self, message: WISMessage, directory: FilePath) -> None:
        basename = message.basename()
        index = 0
        handle = directory.child(basename)
        while handle.exists():
            index += 1
            handle = directory.child(f"{basename}.{index}")
        temp_file = self.temp_dir() / handle.name
        with open(temp_file, "wb") as h:
            h.write(message.original_header())
            h.write(b"\n")
            h.write(message.content)
            h.write(b"\n")
        handle.upload(temp_file)
        next_payload = self.file_payload_from_path(handle.path(), AwareDateTime.utcnow())
        self.progress_payload(next_payload)
