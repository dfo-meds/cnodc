import datetime
import uuid

from cnodc.ocproc2.codecs.base import BaseCodec, DecodeResult
from cnodc.nodb import LockType
import cnodc.ocproc2 as ocproc2
import cnodc.nodb as nodb
import typing as t

from cnodc.processing.workers.payload_worker import WorkflowWorker
from cnodc.storage import StorageController, BaseStorageHandle
from cnodc.util import CNODCError, HaltInterrupt, dynamic_object

from cnodc.processing.workflow.payloads import WorkflowPayload, FilePayload, SourceFilePayload
from cnodc.processing.workers.queue_worker import QueueItemResult
from cnodc.programs.nodb.record_manager import NODBRecordManager
import cnodc.util.awaretime as awaretime


class NODBDecodeLoadWorker(WorkflowWorker):

    storage: StorageController = None

    def __init__(self, **kwargs):
        super().__init__(
            process_name='decoder',
            process_version='1.0',
            **kwargs
        )
        self.set_defaults({
            'queue_name': None,
            'next_queue': 'workflow_continue',
            'failure_queue': 'nodb_decode_failure',
            'error_directory': None,
            'default_metadata': {},
            'decoder_class': None,
            'decode_kwargs': {},
            'allow_reprocessing': False,
            'autocomplete_records': False,
        })
        self._error_dir_handle: t.Optional[BaseStorageHandle] = None
        self._decoder: t.Optional[BaseCodec] = None
        self._decoder_kwargs = {}
        self._record_manager: t.Optional[NODBRecordManager] = None
        self._memory: t.Optional[dict] = None

    def on_start(self):
        super().on_start()
        self._record_manager = NODBRecordManager()
        err_dir = self.get_config('error_directory')
        if err_dir is None:
            raise CNODCError(f"Specified error directory is not a directory", "NODB-LOAD", 1001)
        self._error_dir_handle = self.storage.get_handle(err_dir, self._halt_flag)
        if not (self._error_dir_handle.is_dir() and self._error_dir_handle.exists()):
            raise CNODCError(f"Specified error directory is not a directory", "NODB-LOAD", 1003)
        self._decoder = dynamic_object(self.get_config('decoder_class'))(halt_flag=self._halt_flag)
        self._decoder_kwargs = self.get_config('decoder_kwargs', {})
        if not self._decoder.is_decoder:
            raise CNODCError(f"Specified codec [{self._decoder.__class__.__name__}] is not a decoder", "NODB-LOAD", 1002)

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:

        self._memory = {}

        # Find the source file
        source_file = self._fetch_source_file(payload)
        allow_reprocessing = self.get_config('allow_reprocessing')

        # If it is already completed, then don't process it again
        if source_file.status == nodb.SourceFileStatus.COMPLETE and not allow_reprocessing:
            self._log.info(f"Source file already processed, skipping")
            return QueueItemResult.HANDLED

        if source_file.status == nodb.SourceFileStatus.ERROR:
            self._log.info(f"Source file contains errors, skipping")
            return QueueItemResult.FAILED

        # TODO: consider if it is worth loading the most recent message idx from the error and obs_data tables
        # and making the decoder skip ahead more efficiently.
        skip_to_message_idx = None

        # Mark the source file as in progress
        source_file.status = nodb.SourceFileStatus.IN_PROGRESS
        self._db.update_object(source_file)
        self._db.commit()

        # Download the file
        temp_file = self.download_to_temp_file()

        total_created = 0
        total_skipped = 0
        had_any_errors = False
        was_single_file = False

        # Decode each entry and save them
        with open(temp_file, "rb") as h:
            for result in self._decoder._buffered_decode_records(
                    self._decoder._read_in_chunks(h),
                    include_skipped=False,
                    skip_to_message_idx=skip_to_message_idx,
                    **self._decoder_kwargs):
                success, skipped, had_error = self._create_nodb_record_from_result(source_file, result)
                total_created += success
                total_skipped += skipped
                had_any_errors = had_any_errors or had_error
                was_single_file = result.single_message
                self.breakpoint()

        self._log.info(f"{total_created} records created, {total_skipped} skipped")

        create_next_queue = total_created > 0
        if had_any_errors and was_single_file:
            source_file.status = nodb.SourceFileStatus.ERROR
            create_next_queue = False
        else:
            source_file.status = nodb.SourceFileStatus.COMPLETE
        self._db.update_object(source_file)

        if create_next_queue:
            self.progress_payload(self.source_payload_from_nodb(source_file), prevent_default_progression=True)

    def _fetch_source_file(self, payload: WorkflowPayload) -> nodb.NODBSourceFile:
        if isinstance(payload, FilePayload):
            file_info = payload.file_info
            source_file = nodb.NODBSourceFile.find_by_source_path(
                self._db,
                file_info.file_path,
                lock_type=LockType.FOR_NO_KEY_UPDATE
            )
            if source_file is None:
                rdate = (
                    file_info.last_modified_date.date()
                    if file_info.last_modified_date is not None else
                    awaretime.utc_now()
                )
                source_file = nodb.NODBSourceFile()
                source_file.source_path = file_info.file_path
                source_file.received_date = rdate
                source_file.status = nodb.SourceFileStatus.NEW
                source_file.file_name = file_info.filename
                self._db.insert_object(source_file)
                self._db.commit()
            return source_file
        elif isinstance(payload, SourceFilePayload):
            return payload.load_source_file(self._db)
        else:
            raise CNODCError('invalid payload type', 'NODB-LOAD', 2000)

    def _create_nodb_record_from_result(self,
                                        source_file: nodb.NODBSourceFile,
                                        result: DecodeResult) -> tuple[int, int, bool]:
        success = 0
        skipped = 0
        had_error = False
        make_completed_records = self.get_config('autocomplete_records', False)
        self._before_message(source_file, result)
        if result.success:
            try:
                for record_idx, record in enumerate(result.records):
                    self._before_record(source_file, record)
                    if self._create_nodb_record(source_file, result.message_idx, record_idx, record, make_completed_records):
                        success += 1
                    else:
                        skipped += 1
                self._after_success(source_file, result)
                self._db.commit()
            except CNODCError as ex:
                if ex.is_recoverable:
                    raise ex from ex
                else:
                    self._handle_decode_failure(source_file, result, ex)
                    self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
                    had_error = True
            except Exception as ex:
                self._handle_decode_failure(source_file, result, ex)
                self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
                had_error = True
        else:
            self._handle_decode_failure(source_file, result)
            self._log.error(f"An error occurred while decoding file [{source_file.source_uuid}] message [{result.message_idx}]")
            had_error = True
        return success, skipped, had_error

    def _create_nodb_record(self,
                            source_file: nodb.NODBSourceFile,
                            message_idx: int,
                            record_idx: int,
                            record: ocproc2.ParentRecord,
                            make_completed_records):
        if make_completed_records:
            return self._record_manager.create_completed_entry(self._db, record, source_file.source_uuid, source_file.received_date, message_idx, record_idx, self._memory)
        else:
            return self._record_manager.create_working_entry(self._db, record, source_file.source_uuid, source_file.received_date, message_idx, record_idx)

    def _handle_decode_failure(self,
                               source_file: nodb.NODBSourceFile,
                               result: DecodeResult,
                               additional_exception: Exception = None):
        self._db.rollback()
        self._after_error(source_file, result, additional_exception)
        mode = self._db.update_object
        if result.single_message:
            child_file = source_file
        else:
            child_file = nodb.NODBSourceFile.find_by_original_info(
                self._db,
                source_file.original_uuid,
                source_file.received_date,
                result.message_idx
            )
            if child_file is None:
                file = self._error_dir_handle.child(f'{source_file.source_uuid}-{result.message_idx}.bin')
                file.upload(result.original, allow_overwrite=True)
                child_file = nodb.NODBSourceFile()
                child_file.source_uuid = str(uuid.uuid4())
                child_file.original_idx = result.message_idx
                child_file.original_uuid = source_file.source_uuid
                child_file.received_date = source_file.received_date
                child_file.file_name = source_file.file_name
                child_file.source_path = file.path()
                mode = self._db.insert_object

        child_file.status = nodb.SourceFileStatus.ERROR
        if result.from_exception:
            child_file.report_error(
                f"Decode error: {result.from_exception.__class__.__name__}: {str(result.from_exception)}",
                self._process_name,
                self._process_version,
                self._process_uuid
            )
        if additional_exception:
            child_file.report_error(
                f"Save error: {additional_exception.__class__.__name__}: {str(additional_exception)}",
                self._process_name,
                self._process_version,
                self._process_uuid
            )
        failure_queue = self.get_config('failure_queue')
        if failure_queue is not None:
            payload = self.source_payload_from_nodb(child_file)
            payload.metadata['decoder-class'] = self._decoder.__class__.__name__
            payload.set_followup_queue(self.get_config('next_queue'))
            self.progress_payload(payload, failure_queue, prevent_default_progression=True)
        mode(child_file)
        self._db.commit()

    def _before_message(self, source_file: nodb.NODBSourceFile, result: DecodeResult):
        pass

    def _before_record(self, source_file, record):
        pass

    def _after_success(self, source_file, result):
        pass

    def _after_error(self, source_file, result, additional_exception):
        pass
