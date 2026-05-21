import typing as t
import uuid

from autoinject import injector

import nodb as nodb
from medsutil.ocproc2 import ParentRecord
from pipeman.processing.queue_worker import QueueItemResult
from pipeman.processing.payload_worker import SourceWorkflowWorker
from pipeman.programs.nodb import NODBDecodeLoadWorker
from medsutil.storage import StorageController, FilePath, StorageTier
from pipeman.processing.payloads import SourceFilePayload
from pipeman.programs.glider.ego_convert import OpenGliderConverter


def add_glider_mission_platform_info(worker: NODBDecodeLoadWorker, record: ParentRecord, **kwargs):
    memory: dict[str, t.Any] = worker.memory or {}
    db = worker.db
    if 'platform_map' not in memory:
        memory['platform_map'] = {}
    if 'mission_map' not in memory:
        memory['mission_map'] = {}
    if record.metadata.has_value('WMOID'):
        wmoid = record.metadata['WMOID'].to_string()
        if  wmoid in memory['platform_map']:
            record.metadata['CNODCPlatform'] = memory['platform_map'][wmoid]
        else:
            platforms = [x for x in nodb.NODBPlatform.search(
                db=db,
                wmo_id=wmoid,
                in_service_time=record.coordinates['Time'].to_datetime() if 'Time' in record.coordinates else None
            )]
            if platforms:
                record.metadata['CNODCPlatform'] = platforms[0].platform_uuid
                memory['platform_map'][wmoid] = platforms[0].platform_uuid
            else:
                platform = nodb.NODBPlatform()
                platform.platform_type = 'glider'
                platform.platform_uuid = str(uuid.uuid4())
                platform.wmo_id = wmoid
                if record.metadata.has_value('PlatformName'):
                    platform.platform_name = record.metadata['PlatformName'].to_string()
                if record.metadata.has_value('PlatformID'):
                    platform.platform_id = record.metadata['PlatformID'].to_string()
                db.insert_object(platform)
                memory['platform_map'][wmoid] = platform.platform_uuid
                record.metadata['CNODCPlatform'] = platform.platform_uuid
    if record.metadata.has_value('CruiseID'):
        cruise_id = record.metadata['CruiseID'].to_string()
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

    def process_payload(self, payload: SourceFilePayload) -> t.Optional[QueueItemResult]:
        sf = payload.load_source_file(self.db)
        self._log.info("Processing file %s", sf.source_path)

        local_file = self.download_to_temp_file()

        self._log.debug(f"Converting file and building metadata")
        new_file = self.temp_dir() / "openglider.nc"
        file_name, mission_id, dmd_metadata = self._converter.convert(
            ego_file=local_file,
            og_file=new_file,
            file_name=sf.file_name,
            autopublish=self.get_config('autopublish', True),
            gzip_erddap=self.get_config('gzip_erddap', True)
        )

        storage_locations = [
            f'Original: {payload.load_source_file(self.db).source_path}'
        ]
        erddap_storage_metadata = self.storage.build_metadata(
            program_name='GLIDERS',
            dataset_name=file_name[:-6]
        )
        og_storage_metadata = erddap_storage_metadata.copy()

        gzip_file = self.temp_dir() / "openglider.nc.gz"
        gzipped = False
        erddap_target_name = file_name
        og_target_name = file_name
        og_file = new_file
        erddap_file = new_file
        if self.get_config('gzip_erddap', True):
            self._log.debug("Gzipped local file for upload")
            self.gzip_local_file(new_file, gzip_file)
            gzipped = True
            erddap_target_name = file_name + '.gz'
            erddap_storage_metadata['Gzip'] = 'YES'
            erddap_file = gzip_file
        if self.get_config('gzip_openglider', True):
            if not gzipped:
                self._log.debug("Gzipped local file for upload")
                self.gzip_local_file(new_file, gzip_file)
            og_target_name = file_name + '.gz'
            og_storage_metadata['Gzip'] = 'YES'
            og_file = gzip_file

        target_file = self._target_dir.child(og_target_name, False)
        self._log.debug("Uploading file to %s", target_file)
        target_file.upload(og_file, True, metadata=og_storage_metadata, storage_tier=StorageTier.FREQUENT)
        storage_locations.append(f'Public: {target_file.path()}')

        erddap_dir = self._target_erddap_dir.child(mission_id.lower(), True)
        erddap_dir.mkdir()
        target_erddap_file = erddap_dir.child(erddap_target_name, False)
        self._log.debug("Uploading file to %s", target_erddap_file)
        target_erddap_file.upload(erddap_file, True, metadata=erddap_storage_metadata, storage_tier=StorageTier.FREQUENT)
        storage_locations.append(f'ERDDAP: {target_erddap_file.path()}')

        dmd_metadata.file_storage_location = {"en": "\n".join(storage_locations)}

        # NOTE: let DMD handle ERDDAP reload after it is done updating ERDDAP's metadata
        metadata_queue = self.get_config('metadata_queue', default='dmd_metadata_push')
        if metadata_queue:
            self._log.debug("Pushing metadata upload request to [%s]", metadata_queue)
            self.db.create_queue_item(
                queue_name=metadata_queue,
                data=dmd_metadata.build_request_body(),
                unique_item_name=mission_id
            )
        else:
            self._log.warning("No metadata queue configured!")




