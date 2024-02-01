import datetime
import statistics
import uuid
from cnodc.codecs.base import BaseCodec, DecodeResult
from cnodc.nodb import LockType
from cnodc.ocproc2 import DataRecord, MultiValue
import cnodc.nodb.structures as structures
import typing as t

from cnodc.process.payload_worker import SourcePayloadWorker, FilePayloadWorker
from cnodc.storage import StorageController
from cnodc.util import CNODCError, HaltInterrupt, dynamic_object

from cnodc.workflow.workflow import FilePayload
from cnodc.process.queue_worker import QueueWorker, QueueItemResult


class NODBDecodeLoadWorker(FilePayloadWorker):

    storage: StorageController = None

    def __init__(self, **kwargs):
        super().__init__(
            process_name='decoder',
            process_version='1.0',
            defaults={
                'queue_name': None,
                'next_queue': 'nodb_station_check',
                'failure_queue': 'nodb_decode_failure',
                'error_directory': None,
                'default_metadata': {},
                'decoder_class': None,
                'decode_kwargs': {},
                'before_message': None,
                'after_success': None,
                'after_error': None
            },
            **kwargs
        )
        self._error_dir_handle = self.storage.get_handle(
            self.get_config('error_directory'),
            self._halt_flag
        )
        self._decoder: BaseCodec = dynamic_object(self.get_config('decoder_class'))(halt_flag=self._halt_flag)
        self._decoder_kwargs = self.get_config('decoder_kwargs', {})
        if not (self._error_dir_handle.is_dir() and self._error_dir_handle.exists()):
            raise CNODCError(f"Specified error directory is not a directory", "NODBLOAD", 1003)
        if not self._decoder.is_decoder:
            raise CNODCError(f"Specified codec [{self._decoder.__class__.__name__}] is not a decoder", "NODBLOAD", 1002)
        self._before_message = self.get_config('before_message')
        if self._before_message is not None:
            self._before_message = dynamic_object(self._before_message)
        self._after_success = self.get_config('after_success')
        if self._after_success is not None:
            self._after_success = dynamic_object(self._after_success)
        self._after_error = self.get_config('after_error')
        if self._after_error is not None:
            self._after_error = dynamic_object(self._after_error)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:

        # Find the source file
        source_file = self._fetch_source_file(payload)

        # If it is already completed, then don't process it again
        if source_file.status in (structures.SourceFileStatus.COMPLETE, structures.SourceFileStatus.ERROR):
            self._log.info(f"Source file already processed, skipping")
            return

        # TODO: consider if it is worth loading the most recent message idx from the error and obs_data tables
        # and making the decoder skip ahead more efficiently.
        skip_to_message_idx = None

        # Mark the source file as in progress
        source_file.status = structures.SourceFileStatus.IN_PROGRESS
        self._db.update_object(source_file)
        self._db.commit()

        # Download the file
        temp_dir = self.temp_dir()
        temp_file = payload.download(temp_dir, self.storage, halt_flag=self._halt_flag)

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
            payload = self.source_payload_from_nodb(source_file)
            payload.enqueue(self._db, self.get_config('next_queue'))
        self._db.commit()

    def _fetch_source_file(self, payload: FilePayload) -> structures.NODBSourceFile:
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

    def _create_nodb_record_from_result(self,
                                        source_file: structures.NODBSourceFile,
                                        result: DecodeResult) -> int:
        total_success = 0
        if self._before_message is not None:
            self._before_message(source_file, result)
        if result.success:
            try:
                for record_idx, record in enumerate(result.records):
                    if self._halt_flag:
                        self._halt_flag.check_continue(True)
                    self._create_nodb_record(source_file, result.message_idx, record_idx, record)
                    total_success += 1
                if self._after_success is not None:
                    self._after_success(source_file, result)
                self._db.commit()
            except (HaltInterrupt, KeyboardInterrupt) as ex:
                raise ex from ex
            except CNODCError as ex:
                if ex.is_recoverable:
                    raise ex from ex
                else:
                    self._handle_decode_failure(source_file, result, ex)
                    self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
            except Exception as ex:
                self._handle_decode_failure(source_file, result, ex)
                self._log.exception(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
        else:
            self._handle_decode_failure(source_file, result)
            self._log.error(f"An error occurred while processing file [{source_file.source_uuid}] message [{result.message_idx}]")
        return total_success

    def _create_nodb_record(self,
                            source_file: structures.NODBSourceFile,
                            message_idx: int,
                            record_idx: int,
                            record: DataRecord):
        obs_data = structures.NODBWorkingRecord.find_by_source_info(
            self._db,
            source_file.source_uuid,
            source_file.received_date,
            message_idx,
            record_idx,
            key_only=True
        )
        if obs_data is not None:
            return
        working_record = structures.NODBWorkingRecord.find_by_source_info(
            self._db,
            source_file.source_uuid,
            source_file.received_date,
            message_idx,
            record_idx,
            key_only=True
        )
        if working_record is not None:
            return
        working_record = structures.NODBWorkingRecord()
        working_record.working_uuid = str(uuid.uuid4())
        working_record.received_date = source_file.received_date
        working_record.message_idx = message_idx
        working_record.record_idx = record_idx
        working_record.source_file_uuid = source_file.source_uuid
        self._populate_observation_data(working_record, record)
        self._db.insert_object(working_record)

    def _populate_observation_data(self, working_record: structures.NODBWorkingRecord, record: DataRecord):
        for metadata_key in self._defaults:
            if metadata_key not in record.metadata:
                record.metadata[metadata_key] = self._defaults[metadata_key]
        working_record.record = record

    def _handle_decode_failure(self,
                               source_file: structures.NODBSourceFile,
                               result: DecodeResult,
                               additional_exception: Exception = None):
        if self._after_error is not None:
            self._after_error(source_file, result, additional_exception)
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
            self._db.insert_object(child_file)
            failure_queue = self.get_config('failure_queue')
            if failure_queue is not None:
                payload = self.source_payload_from_nodb(child_file)
                payload.metadata['decoder-class'] = self._decoder.__class__.__name__
                payload.enqueue(self._db, failure_queue)
            self._db.commit()
        return child_file
