import datetime
import typing as t
import uuid

from autoinject import injector

from cnodc.nodb import NODBControllerInstance
import cnodc.nodb as nodb
from cnodc.ocproc2 import ParentRecord
from cnodc.processing.workers.queue_worker import QueueItemResult
from cnodc.processing.workers.payload_worker import FileWorkflowWorker, SourceWorkflowWorker
from cnodc.storage import StorageController, BaseStorageHandle
from cnodc.util import CNODCError
from cnodc.processing.workflow.payloads import FilePayload, SourceFilePayload
from cnodc.storage.base import StorageTier
from cnodc.programs.glider.ego_convert import OpenGliderConverter
import cnodc.programs.dmd.dmd as dmd
import cnodc.util.awaretime as awaretime


def add_glider_mission_platform_info(source_file, record: ParentRecord, db: NODBControllerInstance, memory: dict):
    if 'platform_map' not in memory:
        memory['platform_map'] = {}
    if 'mission_map' not in memory:
        memory['mission_map'] = {}
    if record.metadata.has_value('WMOID'):
        wmoid = record.metadata['WMOID'].value
        if  wmoid in memory['platform_map']:
            record.metadata['CNODCPlatform'] = memory['platform_map'][wmoid]
        else:
            platforms = [x for x in nodb.NODBPlatform.search(
                db=db,
                wmo_id=wmoid,
                in_service_time=record.coordinates['Time'].value if record.coordinates.has_value('Time') else None
            )]
            if platforms:
                record.metadata['CNODCPlatform'] = platforms[0].platform_uuid
                memory['platform_map'][wmoid] = platforms[0].platform_uuid
            else:
                platform = nodb.NODBPlatform()
                platform.platform_uuid = str(uuid.uuid4())
                platform.wmo_id = wmoid
                db.insert_object(platform)
                memory['platform_map'][wmoid] = platform.platform_uuid
                record.metadata['CNODCPlatform'] = platform.platform_uuid
    if record.metadata.has_value('CruiseID'):
        cruise_id = record.metadata['CruiseID'].value
        if cruise_id in memory['mission_map']:
            record.metadata['CNODCMission'] = memory['mission_map'][cruise_id]
        else:
            missions = [x for x in nodb.NODBMission.search(
                db=db,
                mission_id=cruise_id,
            )]
            if missions:
                record.metadata['CNODCMission'] = missions[0].mission_uuid
                memory['mission_map'][cruise_id] = missions[0].mission_uuid
            else:
                mission = nodb.NODBMission()
                mission.mission_id = cruise_id
                mission.mission_uuid = str(uuid.uuid4())
                db.insert_object(mission)
                memory['mission_map'][cruise_id] = mission.mission_uuid
                record.metadata['CNODCMission'] = mission.mission_uuid


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
            'openglider_directory': None,
            'openglider_erddap_directory': None,
            'next_queue': 'workflow_continue',
            'erddap_reload_queue': 'erddap_reload',
            'gzip_erddap': True,
            'gzip_openglider': True,
        })
        self._target_dir: t.Optional[BaseStorageHandle] = None
        self._target_erddap_dir: t.Optional[BaseStorageHandle] = None
        self._converter: t.Optional[OpenGliderConverter] = None

    def on_start(self):
        if self.get_config('openglider_directory', None) is None:
            raise CNODCError('OpenGlider directory not specified', 'GLIDER-CONVERT', 1000)
        self._target_dir = self.storage.get_handle(self.get_config('openglider_directory'), halt_flag=self._halt_flag)
        if not self._target_dir.exists():
            raise CNODCError('OpenGlider directory does not exist', 'GLIDER-CONVERT', 1001)
        if self.get_config('openglider_erddap_directory', None) is None:
            raise CNODCError('OpenGlider ERDDAP directory not specified', 'GLIDER-CONVERT', 1002)
        self._target_erddap_dir = self.storage.get_handle(self.get_config('openglider_erddap_directory'), halt_flag=self._halt_flag)
        if not self._target_erddap_dir.exists():
            raise CNODCError('OpenGlider ERDDAP directory does not exist', 'GLIDER-CONVERT', 1003)
        self._converter = OpenGliderConverter.build(halt_flag=self._halt_flag)

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        local_file = self.download_to_temp_file()
        new_file = self.temp_dir() / "openglider.nc"
        file_name, mission_id = self._converter.convert(local_file, new_file, payload.load_source_file(self._db).file_name)

        erddap_storage_metadata = self.storage.build_metadata(
            program_name='GLIDERS',
            dataset_name=file_name[:-6]
        )
        og_storage_metadata = erddap_storage_metadata.copy()

        gzip_file = self.temp_dir() / "openglider.nc.gz"
        gzipped = False
        erddap_target_name = file_name
        og_target_name = file_name
        if self.get_config('gzip_erddap', True):
            self.gzip_local_file(new_file, gzip_file)
            gzipped = True
            erddap_target_name = file_name + '.gz'
            erddap_storage_metadata['Gzip'] = 'YES'
        if self.get_config('gzip_openglider', True):
            if not gzipped:
                self.gzip_local_file(new_file, gzip_file)
            og_target_name = file_name + '.gz'
            og_storage_metadata['Gzip'] = 'YES'

        target_file = self._target_dir.child(og_target_name, False)
        target_file.upload(gzip_file, True, metadata=og_storage_metadata, storage_tier=StorageTier.FREQUENT)

        erddap_dir = self._target_erddap_dir.child(mission_id.lower(), True)
        erddap_dir.mkdir()
        target_erddap_file = erddap_dir.child(erddap_target_name, False)
        already_exists = target_erddap_file.exists()
        target_erddap_file.upload(gzip_file, True, metadata=erddap_storage_metadata, storage_tier=StorageTier.FREQUENT)

        if already_exists:
            erddap_queue = self.get_config('erddap_reload_queue')
            if erddap_queue:
                self._db.create_queue_item(
                    queue_name=erddap_queue,
                    data={'dataset_id': mission_id,},
                    unique_item_name=mission_id
                )

        payload = self.file_payload_from_path(target_file.path(), awaretime.utc_now())
        payload.set_metadata("glider_erddap_file_path", target_erddap_file.path())
        payload.set_metadata("glider_ego_file_path", payload.file_info.file_path)
        payload.set_metadata("glider_file_name", file_name)
        self.progress_payload(payload, 'glider_metadata_upload')


class GliderMetadataUploadWorker(FileWorkflowWorker):

    metadb: dmd.DataManagerController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="glider_metadata_upload",
            process_version="1.0",
            **kwargs
        )
        self._converter: t.Optional[OpenGliderConverter] = None

    def on_start(self):
        self._converter = OpenGliderConverter.build(halt_flag=self._halt_flag)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        local_file = self.download_to_temp_file()
        meta = self._converter.build_metadata(
            local_file,
            payload.get_metadata("glider_file_name", payload.file_info.filename)
        )
        storage_locations = [
            f"Original: {payload.get_metadata("glider_ego_file_path", "")}",
            f"ERDDAP: {payload.get_metadata("glider_erddap_file_path", "")}",
            f"Public: {payload.file_info.file_path}",
        ]
        meta.file_storage_location = "\n".join(storage_locations)
        self.metadb.upsert_dataset(meta)
        self._skip_autoprogress_payload = True




