import gzip
from cnodc.nodb import NODBController, NODBControllerInstance
import cnodc.nodb.structures as structures
import typing as t
import tempfile
from autoinject import injector
from cnodc.storage import StorageController
from cnodc.util import CNODCError, HaltFlag
import pathlib
import zrlog

from cnodc.workflow.workflow import WorkflowPayload, FilePayload, ItemPayload, SourceFilePayload, BatchPayload, \
    WorkflowController


class PayloadProcessor:

    nodb: NODBController = None
    storage: StorageController = None

    @injector.construct
    def __init__(self,
                 log_name: str,
                 processor_name: str,
                 processor_version: str,
                 processor_uuid: str,
                 require_type=None,
                 halt_flag: t.Optional[HaltFlag] = None,
                 **kwargs):
        self._require_type = require_type
        self._processor_name = processor_name
        self._processor_version = processor_version
        self._processor_uuid = processor_uuid
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        self._halt_flag = halt_flag
        self._db: t.Optional[NODBControllerInstance] = None
        self._current_payload: t.Optional[WorkflowPayload] = None
        self._current_item: t.Optional[structures.NODBQueueItem] = None
        self._log = zrlog.get_logger(log_name)

    def process_queue_item(self, item: structures.NODBQueueItem):
        try:
            payload = WorkflowPayload.build(item)
            if self._require_type is not None and not isinstance(payload, self._require_type):
                raise CNODCError('Payload is not of valid type', 'PAYLOAD', 1000)
            with self.nodb as self._db:
                self._current_payload = payload
                self._process()
                item.mark_complete(self._db)
                self._current_payload = None
                self._db.commit()
        finally:
            self._cleanup()

    def tempdir(self) -> pathlib.Path:
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        return pathlib.Path(self._temp_dir.name)

    def _process(self):
        raise NotImplementedError

    def _cleanup(self):
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

    def create_item_payload(self,
                            observation: t.Union[structures.NODBObservation, structures.NODBObservationData],
                            for_next_step: bool = True):
        payload = ItemPayload(
            item_uuid=observation.obs_uuid,
            item_received=observation.received_date,
            source_file_uuid=observation.source_file_uuid,
            metadata={
                'source_name': self._processor_name,
                'source_version': self._processor_version,
                'source_id': self._processor_uuid,
            }
        )
        self._current_payload.update_for_propagation(payload, for_next_step)
        return payload

    def create_source_payload(self,
                              source_file: structures.NODBSourceFile,
                              for_next_step: bool = True):
        payload = SourceFilePayload(
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            metadata={
                'source_name': self._processor_name,
                'source_version': self._processor_version,
                'source_id': self._processor_uuid,
            }
        )
        self._current_payload.update_for_propagation(payload, for_next_step)
        return payload

    def create_batch_payload(self,
                             batch_uuid: str,
                             for_next_step: bool = True):
        payload = BatchPayload(
            batch_uuid=batch_uuid,
            metadata={
                'source_name': self._processor_name,
                'source_version': self._processor_version,
                'source_id': self._processor_uuid
            }
        )
        self._current_payload.update_for_propagation(payload, for_next_step)
        return payload

    def load_source_from_payload(self) -> structures.NODBSourceFile:
        if not isinstance(self._current_payload, SourceFilePayload):
            raise CNODCError('Invalid payload type')
        source_file = structures.NODBSourceFile.find_by_uuid(
            self._db,
            self._current_payload.source_uuid,
            self._current_payload.received_date
        )
        if source_file is None:
            raise CNODCError('Invalid payload, no such UUID')
        return source_file

    def load_batch_from_payload(self) -> structures.NODBBatch:
        if not isinstance(self._current_payload, BatchPayload):
            raise CNODCError('Invalid payload type')
        batch = structures.NODBBatch.find_by_uuid(
            self._db,
            self._current_payload.batch_uuid
        )
        if batch is None:
            raise CNODCError('Invalid batch, no such UUID')
        return batch

    def load_workflow_controller(self) -> t.Optional[WorkflowController]:
        workflow_obj = structures.NODBUploadWorkflow.find_by_name(self._db, self._current_payload.workflow_name)
        if workflow_obj:
            return WorkflowController(workflow_obj.workflow_name, workflow_obj.configuration, halt_flag=self._halt_flag)
        return None

    def download_file_payload(self) -> pathlib.Path:
        if isinstance(self._current_payload, FilePayload):
            file_info = self._current_payload.file_info
            handle = self.storage.get_handle(file_info.file_path, halt_flag=self._halt_flag)
            if not handle.exists():
                raise CNODCError(f"Upload file does not exist", "NODBLOAD", 1001)
            temp_file1 = self.tempdir() / 'file.1'
            handle.download(temp_file1)
            if file_info.is_gzipped:
                temp_file2 = self.tempdir() / 'file.2'
                with gzip.open(temp_file2, "wb") as dest:
                    with open(temp_file1, "rb") as src:
                        chunk = src.read(2097152)
                        while chunk != b'':
                            dest.write(chunk)
                            chunk = src.read(2097152)
                return temp_file2
            else:
                return temp_file1
        else:
            raise CNODCError("Invalid payload type to download", "PAYLOAD", 10002)
