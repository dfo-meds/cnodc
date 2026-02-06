import datetime
import uuid

from cnodc.codecs.base import BaseCodec, DecodeResult
from cnodc.nodb import LockType
import cnodc.ocproc2 as ocproc2
import cnodc.nodb.structures as structures
import typing as t

from cnodc.process.payload_worker import WorkflowWorker
from cnodc.storage import StorageController, BaseStorageHandle
from cnodc.util import CNODCError, HaltInterrupt, dynamic_object

from cnodc.workflow.workflow import FilePayload, WorkflowPayload, SourceFilePayload
from cnodc.process.queue_worker import QueueWorker, QueueItemResult
from cnodc.programs.nodb_intake.record_manager import NODBRecordManager


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
            'before_message': None,
            'after_success': None,
            'after_error': None,
            'allow_reprocessing': False,
            'autocomplete_records': False,
        })
        self._error_dir_handle: t.Optional[BaseStorageHandle] = None
        self._decoder: t.Optional[BaseCodec] = None
        self._decoder_kwargs = {}
        self._before_message_hook: t.Optional[callable] = None
        self._after_success_hook: t.Optional[callable] = None
        self._after_error_hook: t.Optional[callable] = None
        self._record_manager: t.Optional[NODBRecordManager] = None


    def on_start(self):
        self._record_manager = NODBRecordManager()
        self._error_dir_handle = self.storage.get_handle(
            self.get_config('error_directory'),
            self._halt_flag
        )
        self._decoder= dynamic_object(self.get_config('decoder_class'))(halt_flag=self._halt_flag)
        self._decoder_kwargs = self.get_config('decoder_kwargs', {})
        if not (self._error_dir_handle.is_dir() and self._error_dir_handle.exists()):
            raise CNODCError(f"Specified error directory is not a directory", "NODBLOAD", 1003)
        if not self._decoder.is_decoder:
            raise CNODCError(f"Specified codec [{self._decoder.__class__.__name__}] is not a decoder", "NODBLOAD", 1002)
        self._before_message_hook = self.get_config('before_message')
        if self._before_message_hook is not None:
            self._before_message_hook = dynamic_object(self._before_message_hook)
        self._after_success_hook = self.get_config('after_success')
        if self._after_success_hook is not None:
            self._after_success_hook = dynamic_object(self._after_success)
        self._after_error_hook = self.get_config('after_error')
        if self._after_error_hook is not None:
            self._after_error_hook = dynamic_object(self._after_error)

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:

        # Find the source file
        source_file = self._fetch_source_file(payload)
        allow_reprocessing = self.get_config('allow_reprocessing')

        # If it is already completed, then don't process it again
        if source_file.status == structures.SourceFileStatus.COMPLETE and not allow_reprocessing:
            self._log.info(f"Source file already processed, skipping")
            return QueueItemResult.HANDLED

        if source_file.status == structures.SourceFileStatus.ERROR:
            self._log.info(f"Source file contains errors, skipping")
            return QueueItemResult.FAILED

        # TODO: consider if it is worth loading the most recent message idx from the error and obs_data tables
        # and making the decoder skip ahead more efficiently.
        skip_to_message_idx = None

        # Mark the source file as in progress
        source_file.status = structures.SourceFileStatus.IN_PROGRESS
        self._db.update_object(source_file)
        self._db.commit()

        # Download the file
        temp_file = self.download_to_temp_file()

        total_created = 0

        # Decode each entry and save them
        with open(temp_file, "rb") as h:
            for result in self._decoder.decode_to_results(
                    self._decoder._read_in_chunks(h),
                    include_skipped=False,
                    skip_to_message_idx=skip_to_message_idx,
                    **self._decoder_kwargs):
                total_created += self._create_nodb_record_from_result(source_file, result)

        self._log.info(f"{total_created} records created")

        # Mark the file as complete and queue it for record verification
        source_file.status = structures.SourceFileStatus.COMPLETE
        self._db.update_object(source_file)
        # Only need to create a source payload if records were found
        if total_created > 0:
            self.progress_payload(self.source_payload_from_nodb(source_file), prevent_default_progression=True)

    def _fetch_source_file(self, payload: WorkflowPayload) -> structures.NODBSourceFile:
        if isinstance(payload, FilePayload):
            file_info = payload.file_info
            source_file = structures.NODBSourceFile.find_by_source_path(
                self._db,
                file_info.file_path,
                lock_type=LockType.FOR_NO_KEY_UPDATE
            )
            if source_file is None:
                rdate = (
                    file_info.last_modified_date.date()
                    if file_info.last_modified_date is not None else
                    datetime.datetime.now(datetime.timezone.utc)
                )
                source_file = structures.NODBSourceFile()
                source_file.source_path = file_info.file_path
                source_file.received_date = rdate
                source_file.status = structures.SourceFileStatus.NEW
                source_file.file_name = file_info.filename
                self._db.insert_object(source_file)
                self._db.commit()
            return source_file
        elif isinstance(payload, SourceFilePayload):
            source_file = payload.load_source_file(self._db)
            if source_file is None:
                raise ValueError('invalid payload')
            return source_file
        else:
            raise ValueError('invalid payload type')

    def _create_nodb_record_from_result(self,
                                        source_file: structures.NODBSourceFile,
                                        result: DecodeResult) -> int:
        total_success = 0
        errored = False
        make_completed_records = self.get_config('autocomplete_records', False)
        self._before_message(source_file, result)
        if self._before_message_hook is not None:
            self._before_message_hook(source_file, result)
        if result.success:
            try:
                for record_idx, record in enumerate(result.records):
                    self.breakpoint()
                    if self._create_nodb_record(source_file, result.message_idx, record_idx, record, make_completed_records):
                        total_success += 1
                self._after_success(source_file, result)
                if self._after_success_hook is not None:
                    self._after_success_hook(source_file, result)
                self._db.commit()
            except (HaltInterrupt, KeyboardInterrupt) as ex:
                raise ex from ex
            except CNODCError as ex:
                if ex.is_recoverable:
                    raise ex from ex
                else:
                    self._handle_decode_failure(source_file, result, ex)
                    self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
                    errored = True
            except Exception as ex:
                self._handle_decode_failure(source_file, result, ex)
                self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
                errored = True
        else:
            self._handle_decode_failure(source_file, result)
            self._log.error(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
            errored = True
        return 0 if errored and result.single_message else total_success

    def _create_nodb_record(self,
                            source_file: structures.NODBSourceFile,
                            message_idx: int,
                            record_idx: int,
                            record: ocproc2.ParentRecord,
                            make_completed_records):
        if make_completed_records:
            return self._record_manager.create_completed_entry(self._db, record, source_file.source_uuid, source_file.received_date, message_idx, record_idx)
        else:
            return self._record_manager.create_working_entry(self._db, record, source_file.source_uuid, source_file.received_date, message_idx, record_idx)

    def _handle_decode_failure(self,
                               source_file: structures.NODBSourceFile,
                               result: DecodeResult,
                               additional_exception: Exception = None):
        self._after_error(source_file, result, additional_exception)
        if self._after_error_hook is not None:
            self._after_error_hook(source_file, result, additional_exception)
        self._db.rollback()
        mode = self._db.update_object
        if result.single_message:
            child_file = source_file
        else:
            child_file = structures.NODBSourceFile.find_by_original_info(
                self._db,
                source_file.original_uuid,
                source_file.received_date,
                result.message_idx
            )
            if child_file is None:
                file = self._error_dir_handle.child(f'{source_file.source_uuid}-{result.message_idx}.bin')
                file.upload(result.original, allow_overwrite=True)
                child_file = structures.NODBSourceFile()
                child_file.source_uuid = str(uuid.uuid4())
                child_file.original_idx = result.message_idx
                child_file.original_uuid = source_file.original_uuid
                child_file.received_date = source_file.received_date
                child_file.file_name = source_file.file_name
                child_file.source_path = file.path()
                mode = self._db.insert_object

        child_file.status = structures.SourceFileStatus.ERROR
        if result.from_exception:
            child_file.report_error(
                f"Decode error: {result.from_exception.__class__.__name__}: {str(result.from_exception)}",
                self._process_name,
                self._process_version,
                self._process_uuid
            )
        if additional_exception:
            child_file.report_error(
                f"Decode error: {additional_exception.__class__.__name__}: {str(additional_exception)}",
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

    def _before_message(self, source_file: structures.NODBSourceFile, result: DecodeResult):
        pass

    def _after_success(self, source_file, result):
        pass

    def _after_error(self, source_file, result, additional_exception):
        pass
