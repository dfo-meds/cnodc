import datetime
import typing as t
from autoinject import injector

from cnodc.process import QueueItemResult, SourceWorkflowWorker
from cnodc.process.payload_worker import FileWorkflowWorker
from cnodc.storage import StorageController, BaseStorageHandle
from cnodc.util import CNODCError
from cnodc.workflow.workflow import FilePayload, SourceFilePayload
from cnodc.storage.base import StorageTier
from cnodc.programs.glider.ego_convert import OpenGliderConverter
import cnodc.dmd.dmd as dmd


class GliderConversionWorker(SourceWorkflowWorker):

    storage: StorageController = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(
            process_name="glider_ego_conversion",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'glider_conversion',
            'openglider_directory': '',
            'openglider_erddap_directory': '',
            'next_queue': 'glider_metadata_upload',
            'erddap_reload_queue': 'erddap_reload',
        })
        self._target_dir: t.Optional[BaseStorageHandle] = None
        self._target_erddap_dir: t.Optional[BaseStorageHandle] = None
        self._converter: t.Optional[OpenGliderConverter] = None

    def on_start(self):
        if self.get_config('openglider_directory', None) is None:
            raise CNODCError('OpenGlider directory not specified', 'GLIDER_CONVERT', 1000)
        self._target_dir = self.storage.get_handle(self.get_config('openglider_directory'), halt_flag=self._halt_flag)
        if not self._target_dir.exists():
            raise CNODCError('OpenGlider directory does not exist', 'GLIDER_CONVERT', 1001)
        if self.get_config('openglider_erddap_directory', None) is None:
            raise CNODCError('OpenGlider ERDDAP directory not specified', 'GLIDER_CONVERT', 1002)
        self._target_erddap_dir = self.storage.get_handle(self.get_config('openglider_erddap_directory'), halt_flag=self._halt_flag)
        if not self._target_erddap_dir.exists():
            raise CNODCError('OpenGlider ERDDAP directory does not exist', 'GLIDER_CONVERT', 1003)
        self._converter = OpenGliderConverter.build(halt_flag=self._halt_flag)

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        local_file = self.download_to_temp_file()

        new_file = self.temp_dir() / "openglider.nc"
        file_name, mission_id = self._converter.convert(local_file, new_file)

        gzip_file = self.temp_dir() / "openglider.nc.gz"
        self.gzip_local_file(new_file, gzip_file)

        storage_metadata = self.storage.build_metadata(
            program_name='GLIDERS',
            dataset_name=file_name[:-6],
            gzip=True
        )

        target_file = self._target_dir.child(file_name + ".gz", False)
        target_file.upload(gzip_file, True, metadata=storage_metadata, storage_tier=StorageTier.FREQUENT)

        target_erddap_file = self._target_erddap_dir.child(file_name + ".gz", False)
        already_exists = target_erddap_file.exists()
        target_erddap_file.upload(gzip_file, True, metadata=storage_metadata, storage_tier=StorageTier.FREQUENT)

        if already_exists:
            erddap_queue = self.get_config('erddap_reload_queue')
            if erddap_queue:
                self._db.create_queue_item(
                    queue_name=erddap_queue,
                    data={
                        'dataset_id': mission_id,
                    },
                    unique_item_key=mission_id
                )

        payload = self.file_payload_from_path(target_file.path(), datetime.datetime.now(datetime.timezone.utc))
        payload.set_metadata("glider_erddap_file_path", target_erddap_file.path())
        payload.set_metadata("glider_ego_file_path", payload.file_info.file_path)
        payload.set_metadata("glider_file_name", file_name)
        self.progress_payload(payload, prevent_default_progression=True)


class GliderMetadataUploadWorker(FileWorkflowWorker):

    metadb: dmd.DataManagerController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="glider_metadata_upload",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'next_queue': 'workflow_continue',
        })
        self._converter: t.Optional[OpenGliderConverter] = None

    def on_start(self):
        self._converter = OpenGliderConverter.build(halt_flag=self._halt_flag)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        local_file = self.download_to_temp_file()
        meta = self._converter.build_metadata(local_file, payload.get_metadata("glider_file_name", payload.file_info.filename))
        storage_locations = [
            f"Original: {payload.get_metadata("glider_ego_file_path", "")}",
            f"ERDDAP: {payload.get_metadata("glider_erddap_file_path", "")}",
            f"Public: {payload.file_info.file_path}",
        ]
        meta.set_file_storage_location("\n".join(storage_locations))
        self.metadb.upsert_dataset(meta)




