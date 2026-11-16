import hashlib
import pathlib

from autoinject import injector, auto

from medsutil.awaretime import AwareDateTime
from medsutil.storage import StorageController
from nodb.wis import WISIncomingMessage
from pipeman.processing.payload_worker import FileWorkflowWorker
from pipeman.processing.payloads import FilePayload
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager


class WISMessageDeduplicationWorker(FileWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="wis_deduplicator",
            process_version="1.0",
            **kwargs
        )

    def process_payload(self, payload: FilePayload) -> QueueItemResult | None:
        file = self.download_to_temp_file()
        self.check_for_duplicates(file, payload)
        self.prevent_default_progression()

    def check_for_duplicates(self, file: pathlib.Path, payload: FilePayload):
        header, content = self.get_header_and_content(file)
        content_hash = hashlib.sha256(content, usedforsecurity=False).hexdigest()
        if not self.is_duplicate(header, content, content_hash):
            self._create_source_file(payload, header, content_hash)

    def get_header_and_content(self, file: pathlib.Path) -> tuple[str, bytes]:
        with open(file, "rb") as h:
            content = h.read()
            pos = content.find(b"\n")
            header = ""
            if pos != -1:
                header = content[:pos].decode("ascii", "replace")
                content = content[pos + 1:]
            return header, content

    def is_duplicate(self, header: str, content: bytes, content_hash: str) -> bool:
        comparables = [
            x for x in WISIncomingMessage.find_by_hash(self.db, content_hash)
        ]
        for idx, comparable in enumerate(comparables):
            with self.get_handle(comparable.path, True) as handle:
                local = self.temp_dir() / f"temp.{idx}"
                handle.download(local)
                _, other_content = self.get_header_and_content(local)
                if other_content == content:
                    return True
        return False

    def _create_source_file(self, payload: FilePayload, header: str, content_hash: str):
        message_date = payload.last_modified_date or AwareDateTime.utcnow().strftime("%Y-%m-%d")
        source_file = NODBRecordManager.ensure_source_file(
            self.db,
            payload.file_path,
            payload.filename,
            payload.get_metadata("source-name", ""),
            payload.get_metadata("program-name", ""),
            payload.get_metadata("source-file-identifier", ""),
            payload.last_modified_date,
        )
        wis = WISIncomingMessage(
            wis_version="1.0",
            message_id=header,
            path=payload.file_path,
            hash=content_hash,
            received_date=message_date,
            bundled=0,
            source_file_uuid=source_file.source_uuid
        )
        self.db.insert_object(wis)
        next_payload = self.source_payload_from_nodb(source_file)
        self.progress_payload(next_payload, prevent_default_progression=True)
        self.db.commit()
