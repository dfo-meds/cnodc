import typing as t

from autoinject import injector

from medsutil.exceptions import CodedError
from medsutil.ocproc2 import ParentRecord
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.processing.payload_worker import PayloadWorker
from pipeman.programs.dmd.metadata import CNODCStorageLocation
from pipeman.programs.nodb.loader import NODBDecodeLoadWorker
from medsutil.storage import StorageController, FilePath, StorageTier
from pipeman.processing.payloads import SourceFilePayload, Payload, FilePayload
from pipeman.programs.glider.ego_convert import OpenGliderConverter

def add_glider_mission_platform_info(worker: NODBDecodeLoadWorker, record: ParentRecord, **kwargs):
    ...

class GliderConversionWorker(PayloadWorker):

    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="glider_ego_converter",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'glider_ego_conversion',
            'openglider_directory': None,
            'openglider_erddap_directory': None,
            'next_queue': 'workflow_continue',
            'metadata_queue': 'dmd_metadata_push',
            'gzip_erddap': True,
            'gzip_openglider': True,
            'autopublish': True,
        })
        self._target_dir: t.Optional[FilePath] = None
        self._target_erddap_dir: t.Optional[FilePath] = None
        self._converter: t.Optional[OpenGliderConverter] = None

    def on_start(self):
        self._target_dir = self.get_handle(self.get_config('openglider_directory', None), True)
        if not self._target_dir.exists():
            self._target_dir.mkdir(parents=True)
        self._target_erddap_dir = self.get_handle(self.get_config('openglider_erddap_directory', None), True)
        if not self._target_erddap_dir.exists():
            self._target_erddap_dir.mkdir(parents=True)
        self._converter = OpenGliderConverter.build(halt_flag=self._halt_flag)
        super().on_start()

    def process_payload(self, payload: Payload) -> t.Optional[QueueItemResult]:
        if isinstance(payload, SourceFilePayload):
            sf = payload.load_source_file(self.db)
            filename = sf.file_name
            filepath = sf.source_path
        elif isinstance(payload, FilePayload):
            filename = payload.filename
            filepath = payload.file_path
        else:
            raise CodedError("Invalid payload type for worker", 1000, code_space="GLIDERCONVERT")

        if filename.endswith(".gz"):
            filename = filename[:-3]

        self._log.info("Processing file %s", filepath)
        local_file = self.download_to_temp_file()

        self._log.debug(f"Converting file and building metadata")
        new_file = self.temp_dir() / "openglider.nc"
        mission_id, dmd_metadata = self._converter.convert(
            ego_file=local_file,
            og_file=new_file,
            file_name=filename,
            autopublish=self.get_config('autopublish', True),
            gzip_erddap=self.get_config('gzip_erddap', True)
        )

        dmd_metadata.detailed_storage_locations.append(
            CNODCStorageLocation.build_from_storage_object(
                t.cast(FilePath, self.storage.get_filepath(filepath, self._halt_flag)),
                "ego format; working copy"
            )
        )

        for other_path in payload.metadata.get('workflow-uploaded-files', '').split(';'):
            if other_path and other_path != filepath:
                dmd_metadata.detailed_storage_locations.append(
                    CNODCStorageLocation.build_from_storage_object(
                        t.cast(FilePath, self.storage.get_filepath(other_path, self._halt_flag)),
                        "ego format"
                    )
                )

        erddap_storage_metadata = self.storage.build_metadata(
            program_name='GLIDERS',
            dataset_name=mission_id
        )
        og_storage_metadata = erddap_storage_metadata.copy()

        gzip_file = self.temp_dir() / "openglider.nc.gz"
        gzipped = False
        erddap_target_name = filename
        og_target_name = filename
        og_file = new_file
        erddap_file = new_file
        if self.get_config('gzip_erddap', True):
            self._log.debug("Gzipped local file for upload")
            self.gzip_local_file(new_file, gzip_file)
            gzipped = True
            erddap_target_name = filename + '.gz'
            erddap_storage_metadata['Gzip'] = 'YES'
            erddap_file = gzip_file
        if self.get_config('gzip_openglider', True):
            if not gzipped:
                self._log.debug("Gzipped local file for upload")
                self.gzip_local_file(new_file, gzip_file)
            og_target_name = filename + '.gz'
            og_storage_metadata['Gzip'] = 'YES'
            og_file = gzip_file

        target_file = self._target_dir.child(og_target_name, False)
        self._log.debug("Uploading file to %s", target_file)
        target_file.upload(og_file, True, metadata=og_storage_metadata, storage_tier=StorageTier.FREQUENT)
        dmd_metadata.detailed_storage_locations.append(
            CNODCStorageLocation.build_from_storage_object(target_file, "openglider format; stored copy")
        )

        erddap_dir = self._target_erddap_dir.child(mission_id.lower(), True)
        erddap_dir.mkdir()
        target_erddap_file = erddap_dir.child(erddap_target_name, False)
        self._log.debug("Uploading file to %s", target_erddap_file)
        target_erddap_file.upload(erddap_file, True, metadata=erddap_storage_metadata, storage_tier=StorageTier.FREQUENT)
        dmd_metadata.detailed_storage_locations.append(
            CNODCStorageLocation.build_from_storage_object(target_file, "openglider format; ERDDAP copy")
        )

        dmd_metadata.set_size_from_detailed_files()

        # NOTE: let DMD handle ERDDAP reload after it is done updating ERDDAP's metadata
        metadata_queue = self.get_config('metadata_queue', default='dmd_metadata_push')
        if metadata_queue:
            self._log.debug("Pushing metadata upload request to [%s]", metadata_queue)
            self.db.create_queue_item(
                queue_name=metadata_queue,
                data=dmd_metadata.build_request_body(),
                unique_item_name=mission_id,
                tag=payload.tag,
                correlation_id=payload.correlation_id,
            )
        else:
            self._log.warning("No metadata queue configured!")




