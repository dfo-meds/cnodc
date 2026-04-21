import functools
import uuid

from medsutil.ocproc2.codecs.base import BaseCodec, DecodeResult
from nodb import LockType
import medsutil.ocproc2 as ocproc2
import nodb as nodb
import typing as t

from pipeman.processing.payload_worker import WorkflowWorker
from medsutil.storage import StorageController, FilePath
from pipeman.exceptions import CNODCError
from medsutil.dynamic import dynamic_object

from pipeman.processing.payloads import WorkflowPayload, FilePayload, SourceFilePayload
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.programs.nodb.record_manager import NODBRecordManager
from medsutil.awaretime import AwareDateTime


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
        self.add_events(['before_message', 'before_record', 'after_record', 'after_message_success', 'after_decode_error'])
        self._memory = None

    def on_start(self):
        _ = self.error_directory
        super().on_start()

    @property
    def memory(self):
        if self._memory is None:
            self._memory = {}
        return self._memory

    @property
    def record_manager(self):
        return self._with_cache('record_manager', NODBRecordManager)

    @property
    def error_directory(self) -> FilePath:
        return self._with_cache('error_dir_handle', self._error_directory)

    def _error_directory(self) -> FilePath:
        err_dir = self.get_config('error_directory')
        if err_dir is None:
            raise CNODCError(f"Specified error directory is not a directory", "NODB-LOAD", 1001)
        handle = self.storage.get_filepath(err_dir, self._halt_flag)
        if handle is None or not (handle.is_dir() and handle.exists()):
            raise CNODCError(f"Specified error directory is not a directory or doesn't exist", "NODB-LOAD", 1003)
        return handle

    @property
    def decoder(self) -> BaseCodec:
        return self._with_cache('_decoder', self._decoder)

    def _decoder(self) -> BaseCodec:
        cls = dynamic_object(self.get_config('decoder_class', '', coerce=str))
        decoder = cls(halt_flag=self._halt_flag)
        if not decoder.is_decoder:
            raise CNODCError(f"Specified codec [{cls.__name__}] is not a decoder", "NODB-LOAD", 1002)
        return decoder

    def _decode_records(self, h) -> t.Iterable[DecodeResult]:
        yield from self.decoder.buffered_decode_messages(self.decoder._read_in_chunks(h), **self.get_config('decoder_kwargs', {}))

    def process_payload(self, payload: WorkflowPayload) -> t.Optional[QueueItemResult]:
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

        # Mark the source file as in progress
        source_file.status = nodb.SourceFileStatus.IN_PROGRESS
        self.db.update_object(source_file)
        self.db.commit()

        # Download the file
        temp_file = self.download_to_temp_file()

        total_created = 0
        total_skipped = 0
        had_any_errors = False
        was_single_file = False

        # Decode each entry and save them
        with open(temp_file, "rb") as h:
            for result in self._decode_records(h):
                success, skipped, had_error = self._create_nodb_record_from_result(source_file, result)
                total_created += success
                total_skipped += skipped
                had_any_errors = had_any_errors or had_error
                was_single_file = result.single_message or not result.original
                if had_any_errors:
                    break

        self._log.info(f"{total_created} records created, {total_skipped} skipped")

        create_next_queue = total_created > 0
        if had_any_errors and was_single_file:
            source_file.status = nodb.SourceFileStatus.ERROR
            create_next_queue = False
        else:
            source_file.status = nodb.SourceFileStatus.COMPLETE
        self.db.update_object(source_file)

        if create_next_queue:
            self.progress_payload(self.source_payload_from_nodb(source_file), prevent_default_progression=True)

    def _fetch_source_file(self, payload: WorkflowPayload) -> nodb.NODBSourceFile:
        if isinstance(payload, FilePayload):
            source_file = nodb.NODBSourceFile.find_by_source_path(
                self.db,
                payload.file_path,
                lock_type=LockType.FOR_NO_KEY_UPDATE
            )
            if source_file is None:
                source_file = nodb.NODBSourceFile()
                source_file.source_path = payload.file_path
                source_file.received_date = (payload.last_modified_date or AwareDateTime.utcnow()).date()
                source_file.status = nodb.SourceFileStatus.NEW
                source_file.file_name = payload.filename
                source_file.source_name = payload.get_metadata('source_name', '')
                source_file.program_name = payload.get_metadata('program_name', '')
                self.db.insert_object(source_file)
                self.db.commit()
            return source_file
        elif isinstance(payload, SourceFilePayload):
            return payload.load_source_file(self.db)
        else:
            raise CNODCError('invalid payload type', 'NODB-LOAD', 2000)

    def _create_nodb_record_from_result(self,
                                        source_file: nodb.NODBSourceFile,
                                        result: DecodeResult) -> tuple[int, int, bool]:
        success = 0
        skipped = 0
        had_error = False
        make_completed_records = self.get_config('autocomplete_records', False)
        self.before_message(source_file, result)
        if result.success and result.records:
            try:
                for record_idx, record in enumerate(result.records):
                    self.before_record(source_file, record)
                    record_result = self._create_nodb_record(source_file, result.message_idx, record_idx, record, make_completed_records)
                    if record_result:
                        success += 1
                    else:
                        skipped += 1
                    self.after_record(source_file, record, record_result)
                    self.breakpoint()
                self.after_message_success(source_file, result)
                self.renew_item()
                self.db.commit()
            except CNODCError as ex:
                if ex.is_transient:
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
            exc_info = None
            if result.from_exception is not None:
                exc_info = (result.from_exception.__class__, result.from_exception, result.from_exception.__traceback__)
            self._log.error(f"An error occurred while decoding file [{source_file.source_uuid}] message [{result.message_idx}]", exc_info=exc_info)
            had_error = True
        return success, skipped, had_error

    def _create_nodb_record(self,
                            source_file: nodb.NODBSourceFile,
                            message_idx: int,
                            record_idx: int,
                            record: ocproc2.ParentRecord,
                            make_completed_records):
        if make_completed_records:
            return self.record_manager.create_completed_entry_from_source_file(
                db=self.db,
                record=record,
                message_idx=message_idx,
                record_idx=record_idx,
                source_file=source_file,
                memory=self.memory
            )
        else:
            return self.record_manager.create_working_entry_from_source_file(
                db=self.db,
                record=record,
                source_file=source_file,
                message_idx=message_idx,
                record_idx=record_idx
            )

    def _handle_decode_failure(self,
                               source_file: nodb.NODBSourceFile,
                               result: DecodeResult,
                               additional_exception: Exception = None):
        self.db.rollback()
        mode = self.db.update_object
        if result.single_message or result.original is None:
            child_file = source_file
        else:
            child_file = nodb.NODBSourceFile.find_by_original_info(
                self.db,
                source_file.original_uuid,
                source_file.received_date,
                result.message_idx
            )
            if child_file is None:
                file = self.error_directory.child(f'{source_file.source_uuid}-{result.message_idx}.bin')
                file.upload([result.original], allow_overwrite=True)
                child_file = nodb.NODBSourceFile()
                child_file.source_uuid = str(uuid.uuid4())
                child_file.original_idx = result.message_idx
                child_file.original_uuid = source_file.source_uuid
                child_file.received_date = source_file.received_date
                child_file.file_name = source_file.file_name
                child_file.source_name = source_file.source_name
                child_file.program_name = source_file.program_name
                child_file.source_path = file.path()
                mode = self.db.insert_object

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
            payload.metadata['decoder-class'] = self.decoder.__class__.__name__
            payload.followup_queue = self.get_config('next_queue')
            self.progress_payload(payload, failure_queue, prevent_default_progression=True)
        mode(child_file)
        self.after_decode_error(source_file, result, additional_exception)
        self.db.commit()

    def before_message(self, source_file: nodb.NODBSourceFile, result: DecodeResult):
        self.run_hook('before_message', source_file=source_file, result=result)

    def before_record(self, source_file, record):
        self.run_hook('before_record', source_file=source_file, record=record)

    def after_record(self, source_file, record, was_inserted: bool):
        self.run_hook('after_record', source_file=source_file, record=record, was_inserted=was_inserted)

    def after_message_success(self, source_file, result):
        self.run_hook('after_message_success', source_file=source_file, result=result)

    def after_decode_error(self, source_file, result, additional_exception):
        self.run_hook('after_decode_error', source_file=source_file, result=result, exception=additional_exception)
