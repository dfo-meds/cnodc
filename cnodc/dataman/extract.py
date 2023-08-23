"""Data extraction process"""
import functools
import logging
import pathlib
import tempfile

from cnodc.util import HaltFlag, HaltInterrupt
from cnodc.exc import CNODCError
from cnodc.nodb.proto import NODBTransaction, LockMode
from cnodc.qc.auto.basic import BasicQualityController
from cnodc.files.files import DirFileHandle
from cnodc.nodb import NODBSourceFile, NODBDatabaseProtocol, NODBQueueProtocol
import typing as t

from cnodc.nodb.structures import SourceFileStatus, NODBWorkingObservation, NODBObservation, ObservationStatus, \
    QualityControlStatus, NODBQCBatch, ObservationWorkingStatus
from cnodc.ocproc2 import DataRecord
from cnodc.util import dynamic_class
from cnodc.decode.common import CodecProtocol, CodecStoreLogger, DecodedMessage
from cnodc.files import FileController
import tempfile as tf
import zirconium as zr
from autoinject import injector
import hashlib


class DataExtractionResults:
    """Stores the results of file parsing and organizes it into batches suitable for starting QC."""

    def __init__(self):
        self._result_batches = {}
        self._bad_station_batches = {}

    def add_working_obs(self, working_obs: NODBWorkingObservation):
        """Add a working observation to the result set."""
        if working_obs.station_uuid is not None:
            # If we set a station UUID, organize results by station
            if working_obs.station_uuid not in self._result_batches:
                self._result_batches[working_obs.station_uuid] = []
            self._result_batches[working_obs.station_uuid].append(working_obs)
        else:
            # Otherwise, organize by cause of not having set a station_uuid
            bad_station_batch_key = self._create_bad_station_batch_key(working_obs)
            if bad_station_batch_key not in self._bad_station_batches:
                self._bad_station_batches[bad_station_batch_key] = []
            self._bad_station_batches[bad_station_batch_key].append(working_obs)

    def _create_bad_station_batch_key(self, working_obs: NODBWorkingObservation) -> str:
        if working_obs.has_qc_code("missing_station_id"):
            # The station ID is missing
            return 'missing_all'
        elif working_obs.has_qc_code("singleton_station_id"):
            # There is no station record
            key = hashlib.sha256(str(working_obs.get_qc_metadata("station_identifiers")))
            return f"singleton_{key}"
        elif working_obs.has_qc_code("ambiguous_station_id"):
            # The station record is ambiguous
            key = hashlib.sha256(str(working_obs.get_qc_metadata("potential_matches")))
            return f"ambiguous_{key}"
        else:
            # Other causes (shouldn't happen right now but fallback in case we forget something)
            return "unknown_all"

    def batch_results(self) -> t.Iterable[bool, str, list[NODBWorkingObservation]]:
        """Iterate through the results in batches."""
        for stn_uuid in self._result_batches:
            if any(x.has_any_qc_code() for x in self._result_batches[stn_uuid]):
                yield "review", stn_uuid, self._result_batches[stn_uuid]
            else:
                yield "good", stn_uuid, self._result_batches[stn_uuid]
        for x in self._bad_station_batches:
            pieces = x.split("_", maxsplit=1)
            yield pieces[0], pieces[1] if len(pieces) > 1 else "all", self._bad_station_batches[x]


class SourceFileObservationCache:
    """Maintain a cache of working observations for a given source file."""

    def __init__(self, source_file_uuid: str, database: NODBDatabaseProtocol, tx: NODBTransaction):
        self.source_file_uuid = source_file_uuid
        self.database = database
        self.tx = tx
        self._cache = None

    def _ensure_cache(self):
        if self._cache is None:
            # Load the cache
            self._cache = {}
            for message_idx, record_idx, working_obs in self.database.find_working_observations_by_source(self.source_file_uuid, with_lock=LockMode.FOR_NO_KEY_UPDATE):
                code = f"{message_idx}_{record_idx}"
                self._cache[code] = working_obs

    def observation_is_basic_complete(self, message_idx: int, record_idx: int) -> bool:
        """Check if a given message/record has already been processed through basic QC"""
        self._ensure_cache()
        code = f"{message_idx}_{record_idx}"
        return code in self._cache and self._cache[code] is None

    def load_observation(self, message_idx: int, record_idx: int) -> t.Optional[NODBWorkingObservation]:
        """Load the working observation for the given message and record"""
        self._ensure_cache()
        code = f"{message_idx}_{record_idx}"
        if code in self._cache:
            return self._cache[code]
        return None


class DataExtractionController:

    config: zr.ApplicationConfig = None
    file_controller: FileController = None
    database: NODBDatabaseProtocol = None
    queues: NODBQueueProtocol = None

    @injector.construct
    def __init__(self,
                 instance_name: str,
                 source_file_uuid: str,
                 halt_flag: HaltFlag):
        self.name = "NODB_EXTRACT"
        self.version = "1_0_0"
        self.instance = instance_name
        self.basic_qc = BasicQualityController(self.instance)
        self.source_file_uuid: str = source_file_uuid
        self.source_file: t.Optional[NODBSourceFile] = None
        self.local_file: t.Optional[pathlib.Path] = None
        self.source_handle: t.Optional[DirFileHandle] = None
        self.obs_cache: t.Optional[SourceFileObservationCache] = None
        self.results: t.Optional[DataExtractionResults] = None
        self.tx: t.Optional[NODBTransaction] = None
        self.no_final_save: bool = False
        self.halt_flag = halt_flag

    def process_file(self):
        """Download and extract all records. """
        try:
            # Start a DB transaction
            self.tx = self.database.start_transaction()

            # Locate the source file and lock it
            self.source_file = self.database.load_source_file(
                self.source_file_uuid,
                with_lock=LockMode.FOR_NO_KEY_UPDATE,
                tx=self.tx
            )

            # Make sure the source file is real
            if self.source_file:

                # Set the status to IN_PROGRESS and save that change at least
                self.source_file.status = SourceFileStatus.IN_PROGRESS
                self.database.save_source_file(self.source_file, tx=self.tx)

                with tf.TemporaryDirectory() as tdir:

                    # Temporary file to hold contents of the file
                    tdir = pathlib.Path(tdir)
                    self.local_file = tdir / self.source_file.file_name

                    # Get the file, from persistent or not persistent storage
                    self._download_source_file()

                    # Create records
                    self._create_records_from_source()

                    # Mark the file as complete
                    self._mark_source_complete()
            else:
                raise CNODCError(f"Source file UUID [{self.source_file_uuid}] not found", "EXTRACT", 1008)

        # In case a halt was requested, we make a note of that fact
        except (KeyboardInterrupt, SystemExit, HaltInterrupt) as ex:
            if self.source_file:
                self.source_file.report_error(f"System halt requested", self.name, self.version, self.instance)
            raise ex

        # Error handling for various cases (non-CNODC errors are treated as unrecoverable errors)
        except CNODCError as ex:
            if self.source_file:
                self.source_file.report_error(ex.pretty(), self.name, self.version, self.instance)
                if not ex.is_recoverable:
                    self.source_file.status = SourceFileStatus.ERROR
            raise from ex
        except Exception as ex:
            ex_str = f"{ex.__class__.__name__}: {str(ex)}"
            if self.source_file:
                self.source_file.report_error(ex_str, self.name, self.version, self.instance)
                self.source_file.status = SourceFileStatus.ERROR
            raise CNODCError(f"Unrecognized exception: {ex_str}", is_recoverable=False) from ex

        # Save the source file and commit the transaction
        finally:
            if self.tx is not None:
                if self.source_file is not None and not self.no_final_save:
                    self.database.save_source_file(self.source_file, tx=self.tx)
                self.tx.commit()
                self.tx.close()
                self.tx = None

    def _download_source_file(self):
        """Download and persist the source file."""
        # If the persistent file is specified and exists, use it
        if self.source_file.persistent_path:
            persistent_handle = self.file_controller.get_handle(self.source_file.persistent_path)
            if persistent_handle.exists():
                persistent_handle.download(self.local_file)
                return None
            else:
                self.source_file.persistent_path = None

        # Download the file
        self.source_handle = self.file_controller.get_handle(self.source_file.source_path)
        self.source_handle.download(self.local_file, halt_flag=self.halt_flag)
        self.halt_flag.check()

        # Persist the downloaded file
        target_dir = self.source_file.get_metadata("target_dir")
        if target_dir is None:
            raise CNODCError("Missing target directory", "EXTRACT", 1001)
        target_dir_handle = self.file_controller.get_handle(target_dir)
        if not target_dir_handle.is_dir():
            raise CNODCError(f"Target directory [{target_dir}] does not exist or is not a directory", "EXTRACT", 1002)
        persistent_handle = target_dir_handle.child(self.source_file.file_name)
        self.halt_flag.check()
        persistent_handle.upload(self.local_file, halt_flag=self.halt_flag)
        self.source_file.persistent_path = str(persistent_handle)
        self.halt_flag.check()

    def _create_records_from_source(self):
        """Create all records that don't already exist."""

        # Setup the decoder
        decoder = self._get_decoder(self.source_file)
        logger_cls = functools.partial(CodecStoreLogger, min_level=logging.WARNING)

        # Helper objects for managing the cache and results
        self.results = DataExtractionResults()
        self.obs_cache = SourceFileObservationCache(self.source_file.pkey, self.database, self.tx)

        # Loop through all messages in the file
        for message in decoder.load_messages(self.local_file, replace_logger_cls=logger_cls):

            # Allow the process to halt in between messages
            self.halt_flag.check()

            # Check if there were warnings or errors during decode
            if message.logger.log_store:

                # If so, we will bail on the extraction for this message and send it to the
                # error queue
                self._handle_message_decode_error(message, self.source_file)

            else:
                # Otherwise, persist the records but allow halting in between records
                for record_idx, record in message.iterate_records():
                    self.halt_flag.check()
                    self._persist_data_record(message, record_idx, record)

        # Handle the extraction results
        self._handle_extraction_results()

    def _handle_message_decode_error(self, message: DecodedMessage, source_file: NODBSourceFile):

        if source_file.get_metadata("parent_file_uuid", default=None) is not None:
            # this file is a message that has been retried, but failed again, so just handle it again.
            self._queue_source_file_decode_error(message, source_file)

        elif self.database.errored_source_file_exists(source_file.pkey, message.message_idx, tx=self.tx):
            # noop, already made, let the error be handled in that file
            return

        else:
            # create a new error file and handle it.
            self._queue_new_source_from_decode_error(message, source_file)

    def _queue_new_source_from_decode_error(self, message: DecodedMessage, source_file: NODBSourceFile):

        # Work in a temporary directory to automatically clean up the local error file when done
        with tempfile.TemporaryDirectory() as tempdir:
            # Save the binary content to the file
            local_error_file = pathlib.Path(tempdir) / "file.err"
            with open(local_error_file, "wb") as h:
                h.write(message.binary_content)

            # Figure out where we should store the error
            error_dir = source_file.get_metadata('error_file_store_dir', None)
            if error_dir is None:
                error_dir = self.config.as_str(('cnodc', 'data_intake', 'error_file_store_dir'))
                if error_dir is None:
                    raise CNODCError("Missing error file store directory", "EXTRACT", 1003)

            # Make sure that location exists
            error_dir_handle = self.file_controller.get_handle(error_dir)
            if not error_dir_handle.is_dir():
                raise CNODCError(f"Error file store directory [{error_dir}] does not exist or is not a directory", "EXTRACT", 1004)

            # Determine the file name (unique to the source file name and the message number within it)
            parent_file_name = source_file.file_name
            ext = "" if "." not in parent_file_name else parent_file_name[parent_file_name.rfind("."):]
            file_name = f"{source_file.pkey}.error.{message.message_idx}{ext}"

            # Upload the file (and check if we should break before since this will be long)
            self.halt_flag.check()
            error_file_handle = error_dir_handle.child(file_name)
            error_file_handle.upload(local_error_file)

            # Create the source file object
            new_source_file = NODBSourceFile()
            new_source_file.file_name = file_name
            new_source_file.persistent_path = str(error_file_handle)
            new_source_file.source_path = source_file.persistent_path
            new_source_file.original_idx = message.message_idx
            new_source_file.original_uuid = source_file.pkey
            new_source_file.status = SourceFileStatus.ERROR
            if hasattr(message.logger, 'to_list'):
                new_source_file.set_metadata('decode_errors', message.logger.to_list())

            # Persist it
            self.database.save_source_file(new_source_file, tx=self.tx)
            try:
                self.queues.queue_source_file_decode_error(new_source_file)
            except Exception as ex:
                new_source_file.status = SourceFileStatus.QUEUE_ERROR
                self.database.save_source_file(new_source_file, tx=self.tx)
                raise from ex

    def _queue_source_file_decode_error(self, message: DecodedMessage, source_file: NODBSourceFile):
        source_file.status = SourceFileStatus.ERROR
        if hasattr(message.logger, 'to_list'):
            source_file.set_metadata('decode_errors', message.logger.to_list())
        try:
            self.queues.queue_source_file_decode_error(source_file)
        except Exception as ex:
            source_file.status = SourceFileStatus.QUEUE_ERROR
            raise from ex

    def _persist_data_record(self, message: DecodedMessage, record_idx: int, record: DataRecord):
        # Completed records need no further processing
        if self.obs_cache.observation_is_basic_complete(message.message_idx, record_idx):
            return

        # Check if the record exists
        working_record = self.obs_cache.load_observation(message.message_idx, record_idx)

        # If not, we build it
        if working_record is None:
            primary_record = self._persist_primary_record(message, record_idx, record)
            working_record = self._create_working_record(primary_record)

        # Otherwise, just make sure the record is updated
        else:
            working_record.store_data_record(record)

        # Apply the basic quality control technique to the new record
        self.basic_qc.initial_basic_quality_control(working_record, tx=self.tx)
        working_record.qc_test_status = QualityControlStatus.PASSED

        # Persist the working record to not repeat QC next time
        self.database.save_working_observation(working_record, tx=self.tx)

    def _persist_primary_record(self, message: DecodedMessage, record_idx: int, record: DataRecord) -> NODBObservation:
        existing_obs = self.database.find_primary_observation_by_source(
            self.source_file.pkey,
            message.message_idx,
            record_idx,
            with_lock=LockMode.FOR_KEY_SHARE,
            tx=self.tx
        )
        if existing_obs:
            return existing_obs
        # Create a primary observation record
        primary_obs = NODBObservation()
        primary_obs.message_idx = message.message_idx
        primary_obs.source_file_uuid = self.source_file.pkey
        primary_obs.record_idx = record_idx
        primary_obs.status = ObservationStatus.UNVERIFIED
        primary_obs.store_data_record(record)
        self.database.save_primary_observation(primary_obs, tx=self.tx)
        return primary_obs

    def _create_working_record(self, primary_record: NODBObservation):
        if primary_record.pkey is None:
            raise CNODCError("Primary record has no primary key yet", "EXTRACT", 1005)
        working_obs = NODBWorkingObservation.create_from_primary(primary_record)
        working_obs.qc_process_name = '__basic_initial__'
        working_obs.qc_current_step = 0
        working_obs.qc_test_status = QualityControlStatus.QUEUED
        working_obs.working_status = ObservationWorkingStatus.IN_PROGRESS
        return working_obs

    def _get_decoder(self, source_file: NODBSourceFile) -> CodecProtocol:
        decoder_cls_name = source_file.get_metadata('decoder_class_name', None)
        if decoder_cls_name is None:
            decoder_cls_name = self._auto_detect_decoder(source_file.file_name)
            if decoder_cls_name is None:
                raise CNODCError("Missing decoder information and could not auto-detect", "EXTRACT", 1000)
        return dynamic_class(decoder_cls_name)()

    def _auto_detect_decoder(self, file_name: str) -> t.Optional[str]:
        file_name = file_name.lower()
        if file_name.endswith(".bufr"):
            return "cnodc.decode.wmo.bufr.GTSBufrStreamCodec"
        return None

    def _handle_extraction_results(self):
        # By this point, each observation in the file has undergone basic QC. If any haven't,
        # they would have raised an exception. Some may have been released already if an error
        # was raised during this process though, but nothing should be behind. Only the ones
        # still to be released are in the results at this point.
        for batch_type, _, working_obs_list in self.results.batch_results():

            self.halt_flag.check()

            batch = NODBQCBatch()
            if batch_type == "good":
                batch.qc_test_status = QualityControlStatus.PASSED
            else:
                batch.qc_test_status = QualityControlStatus.MANUAL_REVIEW
            batch.working_status = ObservationWorkingStatus.IN_PROGRESS
            batch.qc_process_name = '__basic__'
            batch.qc_current_step = 0
            self.database.save_batch_and_assign(batch, working_obs_list, tx=self.tx)

            try:
                if batch_type == "good":
                    self.queues.queue_basic_qc_process(batch)
                else:
                    self.queues.queue_basic_qc_review(batch)
                batch.working_status = ObservationWorkingStatus.QUEUED
            except Exception as ex:
                batch.working_status = ObservationWorkingStatus.QUEUE_ERROR
                batch.set_qc_metadata("queue_error", f"{ex.__class__.__name__}: {str(ex)}")
                raise ex
            finally:
                self.database.save_batch(batch, tx=self.tx)

    def _mark_source_complete(self):
        """Mark the source file as being completed."""
        self.source_file.status = SourceFileStatus.COMPLETE
        if self.source_file.get_metadata('delete_after_download', default=False):
            self.source_handle.delete()
