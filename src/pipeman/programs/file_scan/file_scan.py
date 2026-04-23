import functools
import uuid
import typing as t

from nodb import NODB, NODBQueueItem, ScannedFileStatus, NODBError
from pipeman.processing.payload_worker import PayloadWorker
from pipeman.processing.scheduled_task import ScheduledTask
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.processing.workflow import WorkflowController
from pipeman.processing.payloads import NewFilePayload
from medsutil.storage import StorageController, FilePath
import nodb as nodb
from pipeman.exceptions import CNODCError
from autoinject import injector
import medsutil.awaretime as awaretime
from medsutil.awaretime import AwareDateTime


class FileScanTask(ScheduledTask):

    nodb: NODB = None
    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name='file_scanner',
            process_version='1.0',
            **kwargs
        )
        self.set_defaults({
            'scan_target': None,
            'workflow_name': None,
            'queue_name': 'file_download',
            'pattern': '*',
            'recursive': False,
            'remove_downloaded_files': False,
            'reprocess_updated_files': False,
            'metadata': None,
            'downloader_config': None,
        })
        self._scan_target: t.Optional[FilePath] = None
        self._remove_when_complete = None
        self._reprocess_updated_files = None
        self._headers = None
        self._pattern = None
        self._recursive = None
        self._workflow_name = None
        self._queue_name = None

    def on_start(self):
        scan_target = self.get_config('scan_target')
        if not scan_target:
            raise CNODCError(f'Scan target is not configured', 'FILESCAN', 1000)
        self._workflow_name = self.get_config('workflow_name')
        if not self._workflow_name:
            raise CNODCError(f'Workflow name is not configured', 'FILESCAN', 1002)
        self._queue_name = self.get_config('queue_name')
        if not self._queue_name:
            raise CNODCError(f'Queue name is not configured', 'FILESCAN', 1003)
        self._scan_target = self.get_handle(scan_target, True)
        self._remove_when_complete = bool(self.get_config("remove_downloaded_files"))
        self._reprocess_updated_files = bool(self.get_config("reprocess_updated_files"))
        self._headers = self.get_config("metadata", {})
        self._pattern = self.get_config('pattern', '*')
        self._recursive = bool(self.get_config('recursive', False))
        super().on_start()

    def execute(self):
        with self.nodb as db:
            self.scan_files(db)

    def scan_files(self, db):
        batch_id = str(uuid.uuid4())
        self._log.info(f'Scanning [%s]', self._scan_target.path())
        full_path = None
        mod_time = None
        for file in self._scan_target.search(self._pattern, self._recursive):
            db.create_savepoint('FILE_INSERT')
            try:
                full_path = file.path()
                mod_time = file.modified_datetime() if self._reprocess_updated_files else None
                status = db.scanned_file_status(full_path, mod_time)
                if status is ScannedFileStatus.NOT_PRESENT:
                    self._log.info("Found new file [%s][%s]", full_path, mod_time)
                    db.note_scanned_file(full_path, mod_time)
                    payload = NewFilePayload.from_handle(file,
                                                         workflow_name=self._workflow_name,
                                                         remove_when_complete=self._remove_when_complete,
                                                         modified_time=mod_time)
                    payload.metadata.update(self._headers)
                    payload.metadata.update({
                        'source': self.process_id,
                        'scan-batch': batch_id,
                        'scan-target': self._scan_target.path(),
                        'scanned-time': awaretime.utc_now().isoformat()
                    })
                    payload.set_worker_config('file_downloader', self.get_config('downloader_config', {}))
                    payload.enqueue(db, self._queue_name)
                    db.commit()
                else:
                    self._log.info(f"Skipping old file [%s][%s]", full_path, mod_time)
            except NODBError as ex:

                # Serialization or unique key failure means we have one of two issues:
                # - The file path was inserted between our own checking and inserting
                # - The queue UUID was duplicated (unlikely)
                # In either case, we can ignore it for now as long as we rollback.
                # If the file doesn't get properly recorded, it will be checked on the next pass
                if ex.is_serialization_error():
                    db.rollback_to_savepoint('FILE_INSERT')
                    self._log.warning("Exception while creating database entry for scanned file [%s][%s]", full_path, mod_time, exc_info=True)

                # Other errors indicate a bigger issue, we want to raise those
                else:
                    raise
            finally:
                full_path = None
                mod_time = None


class FileDownloadWorker(PayloadWorker[NewFilePayload]):

    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name='file_downloader',
            process_version='1.0',
            require_type=NewFilePayload,
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'file_download',
            'allow_file_deletes': False,
        })

    def process_payload(self, payload: NewFilePayload):
        self.handle_queued_file(
            payload.workflow_name,
            payload.file_path,
            payload.modified_time,
            payload.metadata,
            payload.correlation_id,
            self.get_config('allow_file_deletes', False) and payload.remove_when_complete
        )
        return QueueItemResult.SUCCESS

    def after_failure(self, item: NODBQueueItem, ex: Exception):
        if self._current_payload is not None and isinstance(self.current_payload, NewFilePayload):
            self.db.mark_scanned_item_failed(self.current_payload.file_path, self.current_payload.modified_time or None)
            self.db.commit()
        super().after_failure(item, ex)

    def get_workflow(self, workflow_name: str):
        self._log.debug('Looking for workflow [%s]', workflow_name)
        workflow = nodb.NODBUploadWorkflow.find_by_name(self.db, workflow_name)
        if workflow is None:
            raise CNODCError(f'Workflow [{workflow_name}] not found', 'FILEFLOW', 1002)
        return WorkflowController(workflow_name, workflow.configuration, halt_flag=self._halt_flag)

    def handle_queued_file(self,
                           workflow_name: str,
                           file_path: str,
                           modified_time: t.Optional[AwareDateTime],
                           metadata: dict,
                           correlation_id: str | None,
                           remove_when_finished: bool = False):
        if file_path is None or file_path == '':
            raise CNODCError('Missing file_path key', 'FILEFLOW', 1004)
        current_status = self.db.scanned_file_status(file_path, modified_time)
        if current_status == ScannedFileStatus.UNPROCESSED:
            self._log.info('Processing scanned file [%s][%s]', file_path, modified_time)
            self._log.debug('Building file handle for [%s]', file_path)
            wf_controller = self.get_workflow(workflow_name)
            handle = self.get_handle(file_path, True)
            self._update_payload_metadata(metadata, handle)
            local_path = self.download_to_temp_file()
            wf_controller.handle_incoming_file(
                local_path=local_path,
                metadata=metadata,
                success_hook=functools.partial(self._on_success_hook, file_path=file_path, mod_time=handle.modified_datetime()),
                db=self.db,
                correlation_id=correlation_id
            )
            if remove_when_finished:
                handle.remove()
        elif current_status == ScannedFileStatus.PROCESSED and remove_when_finished:
            self._log.info(f"Item [%s] already processed [result %s], checking for removal", file_path, current_status)
            handle = self.get_handle(file_path, True)
            if handle.exists():
                handle.remove()
        elif current_status == ScannedFileStatus.NOT_PRESENT:
            self._log.warning(f"Item [%s] was not registered!, skipping", file_path)
        else:
            self._log.info(f"Item [%s] already processed or errored [result %s], skipping", file_path, current_status)

    def _on_success_hook(self, file_path, mod_time):
        self.db.mark_scanned_item_success(file_path, mod_time)

    def _update_payload_metadata(self, metadata: dict, handle: FilePath):
        if 'source' not in metadata:
            metadata['source'] = self.process_id
        metadata['filename'] = handle.name
        md = handle.modified_datetime()
        if md is None:  # pragma: no coverage (fallback for weird edge cases when the modified time can't be determined)
            if 'scanned-time' in metadata:
                metadata['last-modified-date'] = metadata['scanned-time']
            else:
                metadata['last-modified-date'] = awaretime.utc_now()
        else:
            metadata['last-modified-date'] = md.isoformat()

