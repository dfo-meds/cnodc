import datetime
import gzip
import shutil
import statistics
import uuid
from cnodc.codecs.ocproc2bin import OCProc2BinCodec
from cnodc.codecs.base import BaseCodec, DecodeResult
from cnodc.nodb import NODBController, NODBControllerInstance
from cnodc.ocproc2 import DataRecord
from cnodc.ocproc2.structures import AbstractValue, MultiValue
import cnodc.nodb.structures as structures
import typing as t
import tempfile
from autoinject import injector
from cnodc.storage import StorageController
from cnodc.util import CNODCError, HaltFlag
from cnodc.units import UnitConverter
import pathlib
import zrlog


class NODBLoader:

    storage: StorageController = None
    nodb: NODBController = None
    converter: UnitConverter = None

    @injector.inject
    def __init__(self,
                 process_name: str,
                 process_uuid: str,
                 process_version: str,
                 error_directory: str,
                 decoder: BaseCodec,
                 decode_kwargs: dict = None,
                 post_verify_queues: list[str] = None,
                 verification_queue: str = 'nodb_verification',
                 default_values: dict[str, t.Any] = None,
                 halt_flag: HaltFlag = None):
        self._verification_queue = verification_queue
        self._decode_kwargs = decode_kwargs or {}
        self._post_verify_queues = post_verify_queues
        self._process_name = process_name
        self._process_uuid = process_uuid
        self._process_version = process_version
        self._defaults = default_values or {}
        self._decoder: BaseCodec = decoder
        self._encoder = OCProc2BinCodec(halt_flag=halt_flag)
        self._encoder_kwargs = {
            "codec": "JSON",
            "compression": "LZMA7CRC4",
            "correction": "RS32"
        }
        self._error_dir = self.storage.get_handle(error_directory, halt_flag=halt_flag)
        if not (self._error_dir.is_dir() and self._error_dir.exists()):
            raise CNODCError(f"Specified error directory [{error_directory}] is not a directory", "NODBLOAD", 1003)
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        self._temp_file: t.Optional[pathlib.Path] = None
        self._halt_flag = halt_flag
        if not self._decoder.is_decoder:
            raise CNODCError(f"Specified codec [{self._decoder.__class__.__name__}] is not a decoder", "NODBLOAD", 1002)
        self.log = zrlog.get_logger("cnodc.nodb_loader")

    def _cleanup(self):
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
            self._temp_file = None

    def _download_file(self, item: structures.NODBQueueItem) -> pathlib.Path:
        handle = self.storage.get_handle(item.data['upload_file'], halt_flag=self._halt_flag)
        if not handle.exists():
            raise CNODCError(f"Upload file does not exist", "NODBLOAD", 1001)
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        _temp_file = pathlib.Path(self._temp_dir.name) / "file.1"
        handle.download(self._temp_file)
        is_gzipped = 'gzip' in item.data and item.data['gzip']
        if is_gzipped:
            _temp_file_ungzip = pathlib.Path(self._temp_dir.name) / "file.2"
            with gzip.open(_temp_file_ungzip, "wb") as dest:
                with open(_temp_file, "rb") as src:
                    shutil.copyfileobj(src, dest)
            return _temp_file_ungzip
        else:
            return _temp_file

    def load_file_from_queue(self, item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:

        # Check that we have a proper file name here
        if 'upload_file' not in item.data or not item.data['upload_file']:
            raise CNODCError(f"Missing item.data[upload_file]", "NODBLOAD", 1000)

        try:

            with self.nodb as db:

                # Find the source file
                source_file = self._fetch_source_file(
                    db,
                    item.data['upload_file'],
                    item.data['filename'] if 'filename' in item.data['filename'] else '',
                    datetime.datetime.fromisoformat(item.data['last_modified']) if 'last_modified' in item.data else datetime.datetime.now(datetime.timezone.utc)
                )
                if self._halt_flag:
                    self._halt_flag.check_continue(True)

                # If it is already completed, then don't process it again
                if source_file.status == structures.SourceFileStatus.COMPLETE:
                    return structures.QueueItemResult.SUCCESS

                # Download the file
                temp_file = self._download_file(item)
                if self._halt_flag:
                    self._halt_flag.check_continue(True)

                # Decode each entry and save them
                for result in self._decoder.decode(self._stream_temp_file(temp_file), **self._decode_kwargs):
                    if self._halt_flag:
                        self._halt_flag.check_continue(True)
                    self._create_nodb_record_from_result(db, source_file, result)

            return structures.QueueItemResult.SUCCESS
        finally:
            self._cleanup()

    def _create_nodb_record_from_result(self, db: NODBControllerInstance, source_file: structures.NODBSourceFile, result: DecodeResult):
        if result.success:
            for record_idx, record in enumerate(result.records):
                if self._halt_flag:
                    self._halt_flag.check_continue(True)
                self._create_nodb_record(db, source_file, result.message_idx, record_idx, record)
        else:
            self._handle_decode_failure(db, source_file, result)

    def _create_nodb_record(self, db: NODBControllerInstance, source_file: structures.NODBSourceFile, message_idx: int, record_idx: int, record: DataRecord):
        obs_data = structures.NODBObservationData.find_by_source_info(
            db,
            source_file.source_uuid,
            source_file.received_date,
            message_idx,
            record_idx
        )
        obs = None
        if obs_data is None:
            obs_data = structures.NODBObservationData()
            obs_data.obs_uuid = str(uuid.uuid4())
            obs_data.received_date = source_file.received_date
            obs_data.message_idx = message_idx
            obs_data.record_idx = record_idx
            self._populate_observation_data(obs_data, record)
            db.insert_object(obs_data)
            # We commit this here because the observation data is BIG and
            # we don't want to re-insert it if we don't have to.
            db.commit()
        else:
            obs = structures.NODBObservation.find_by_uuid(db, obs_data.obs_uuid, obs_data.received_date)
        if obs is None:
            obs = structures.NODBObservation()
            obs.obs_uuid = obs_data.obs_uuid
            obs.received_date = obs_data.received_date
            self._populate_observation(obs, record)
            db.insert_object(obs)
            db.create_queue_item(self._verification_queue, {
                "item_uuid": obs_data.obs_uuid,
                "item_received": obs_data.received_date,
                "post_processing": self._post_verify_queues,
                "_metadata": {
                    'source_name': self._process_name,
                    'source_version': self._process_version,
                    'source_id': self._process_uuid,
                    'source_file': obs_data.source_file_uuid,
                    'message_idx': obs_data.message_idx,
                    'record_idx': obs_data.record_idx,
                    'created_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
            })
            db.commit()

    def _extract_float_in_units(self, v: AbstractValue, unit_str: str = None) -> t.Optional[float]:
        if v is None:
            return None
        values = []
        work = [v]
        while work:
            val = work.pop()
            if isinstance(val, MultiValue):
                work.extend(val)
            else:
                if val.value is None:
                    continue
                try:
                    if unit_str is None or 'Units' not in val.metadata:
                        values.append(float(val.value))
                    else:
                        values.append(self.converter.convert(float(val.value), val.metadata['Unit'].value, unit_str))
                except ValueError as ex:
                    self.log.exception(f"Error converting value to float")
        if not values:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return statistics.mean(values)

    def _extract_iso_time(self, v: AbstractValue) -> t.Optional[datetime.datetime]:
        if v is None:
            return None
        values = []
        work = [v]
        while work:
            val = work.pop()
            if isinstance(val, MultiValue):
                work.extend(val)
            else:
                if val.value is None or val.value == "":
                    continue
                try:
                    values.append(datetime.datetime.fromisoformat(val.value))
                except ValueError:
                    self.log.exception(f"Invalid date/time value {val.value}")
        if not values:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return datetime.datetime.fromtimestamp(statistics.mean([x.timestamp() for x in values]))

    def _populate_observation(self, obs: structures.NODBObservation, record: DataRecord):
        obs.surface_parameters = list(record.parameters.keys()) if record.parameters else None
        obs.profile_parameters = None
        if 'Latitude' not in record.coordinates or 'Longitude' not in record.coordinates or 'Time' not in record.coordinates:
            obs.observation_type = structures.ObservationType.OTHER
        elif 'PROFILE' in record.subrecords:
            obs.obs_time = self._extract_iso_time(record.coordinates['Time'])
            obs.location = f"POINT ({self._extract_float_in_units(record.coordinates['Longitude'])} {self._extract_float_in_units(record.coordinates['Latitude'])})"
            obs.observation_type = structures.ObservationType.PROFILE
            obs.min_depth = None
            obs.max_depth = None
            profile_parameters = set()
            for subrecord in record.iter_subrecords("PROFILE"):
                sr_depth = None
                profile_parameters.update(subrecord.parameters.keys())
                if "Depth" in subrecord.coordinates:
                    sr_depth = self._extract_float_in_units(subrecord.coordinates['Depth'], 'm')
                elif 'Pressure' in subrecord.coordinates:
                    # TODO: should we consider a depth measurement conversion from pressure?
                    pass
                if sr_depth is None:
                    continue
                elif obs.min_depth is None or sr_depth < obs.min_depth:
                    obs.min_depth = sr_depth
                elif obs.max_depth is None or sr_depth > obs.max_depth:
                    obs.max_depth = sr_depth
            obs.profile_parameters = list(profile_parameters)
        elif 'Depth' in record.coordinates:
            obs.observation_type = structures.ObservationType.AT_DEPTH
            obs.min_depth = self._extract_float_in_units(record.coordinates['DEPTH'], 'm')
            obs.max_depth = obs.min_depth
        elif 'Pressure' in record.coordinates:
            obs.observation_type = structures.ObservationType.AT_DEPTH
            # TODO: should we consider a depth measurement conversion from pressure?
        else:
            obs.observation_type = structures.ObservationType.SURFACE
            obs.min_depth = 0
            obs.max_depth = 0
        if 'CNODCStation' in record.metadata:
            obs.station_uuid = record.metadata['CNODCStation'].value
        if (obs.station_uuid is None or obs.station_uuid == '') and 'station_uuid' in self._defaults:
            obs.station_uuid = self._defaults['station_uuid']
        if 'CNODCMission' in record.metadata:
            obs.mission_name = record.metadata['CNODCMission'].value
        if (obs.mission_name is None or obs.mission_name == '') and 'mission_name' in self._defaults:
            obs.mission_name = self._defaults['mission_name']
        if 'CNODCSource' in record.metadata:
            obs.source_name = record.metadata['CNODCSource'].value
        if (obs.source_name is None or obs.source_name == '') and 'source_name' in self._defaults:
            obs.source_name = self._defaults['source_name']
        if 'CNODCInstrumentType' in record.metadata:
            obs.instrument_type = record.metadata['CNODCInstrumentType'].value
        if (obs.instrument_type is None or obs.instrument_type == '') and 'instrument_type' in self._defaults:
            obs.instrument_type = self._defaults['instrument_type']
        if 'CNODCProgram' in record.metadata:
            obs.program_name = record.metadata['CNODCProgram'].value
        if (obs.program_name is None or obs.program_name == '') and 'program_name' in self._defaults:
            obs.program_name = self._defaults['program_name']
        if 'CNODCStatus' in record.metadata:
            try:
                obs.status = structures.ObservationStatus(record.metadata['CNODCStatus'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid observation status [{record.metadata['CNODCStatus'].value}]")
        if obs.status is None and 'status' in self._defaults:
            obs.status = self._defaults['status']
        if 'CNODCLevel' in record.metadata:
            try:
                obs.processing_level = structures.ObservationStatus(record.metadata['CNODCLevel'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid processing level [{record.metadata['CNODCLevel'].value}")
        if obs.processing_level is None and 'processing_level' in self._defaults:
            obs.processing_level = self._defaults['processing_level']
        if 'CNODCEmbargoUntil' in record.metadata:
            try:
                obs.embargo_date = datetime.datetime.fromisoformat(record.metadata['CNODCEmbargoUntil'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid embargo date [{record.metadata['CNODCEmbargoUntil'].value}")
        if obs.embargo_date is None and 'embargo_date' in self._defaults:
            obs.embargo_date = self._defaults['embargo_date']

    def _populate_observation_data(self, obs_data: structures.NODBObservationData, record: DataRecord):
        obs_data.data_record = bytearray()
        for byte_ in self._encoder.encode_messages([record], **self._encoder_kwargs):
            obs_data.data_record.extend(byte_)
        if 'CNODCDuplicateId' in record.metadata and 'CNODCDuplicateDate' in record.metadata:
            try:
                obs_data.duplicate_received_date = datetime.date.fromisoformat(record.metadata['CNODCDuplicateDate'].value)
                obs_data.duplicate_uuid = record.metadata['CNODCDuplicateId'].value
            except ValueError:
                self.log.warning(f"Ignoring invalid duplicate received date [{record.metadata['CNODCDuplicateDate']}")
        if record.qc_tests:
            qc_tests = {}
            for test in record.qc_tests:
                # Assemble the most recent test of each name
                if test.test_name not in qc_tests or test.test_date > qc_tests[test.test_name][1]:
                    qc_tests[test.test_name] = (test.test_version, test.test_date, test.result)
            obs_data.qc_tests = qc_tests

    def _handle_decode_failure(self, db: NODBControllerInstance, source_file: structures.NODBSourceFile, result: DecodeResult):
        child_file = structures.NODBSourceFile.find_by_original_info(
            db,
            source_file.original_uuid,
            source_file.received_date,
            result.message_idx
        )
        if child_file is None:
            file = self._error_dir.child(f'{source_file.source_uuid}-{result.message_idx}.bin')
            file.upload(result.original)
            child_file = structures.NODBSourceFile()
            child_file.original_idx = result.message_idx
            child_file.original_uuid = source_file.original_uuid
            child_file.received_date = source_file.received_date
            child_file.file_name = source_file.file_name
            child_file.source_path = file.path()
            child_file.status = structures.SourceFileStatus.ERROR
            child_file.report_error(
                f"Decode error: {result.from_exception.__class__.__name__}: {str(result.from_exception)}",
                self._process_name,
                self._process_version,
                self._process_uuid
            )
            db.insert_object(child_file)
            db.commit()
        return child_file

    def _fetch_source_file(self, db: NODBControllerInstance, source_file_path: str, file_name: str, received_date: datetime.datetime) -> structures.NODBSourceFile:
        source_file = structures.NODBSourceFile.find_by_source_path(db, source_file_path)
        if source_file is None:
            source_file = structures.NODBSourceFile()
            source_file.source_path = source_file_path
            source_file.received_date = received_date.date()
            source_file.status = structures.SourceFileStatus.NEW
            source_file.file_name = file_name
            db.insert_object(source_file)
            db.commit()
        return source_file

    def _stream_temp_file(self, temp_file: pathlib.Path, chunk_size: int = 1048576) -> t.Iterable[bytes]:
        with open(temp_file, 'rb') as h:
            if self._halt_flag:
                self._halt_flag.check_continue(True)
            chunk = h.read(chunk_size)
            while chunk != b'':
                yield chunk
                if self._halt_flag:
                    self._halt_flag.check_continue(True)
                chunk = h.read(chunk_size)
