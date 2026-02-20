import datetime
import pathlib
import typing as t

from autoinject import injector

from cnodc.nodb import NODBControllerInstance, structures
from cnodc.storage import StorageController

from cnodc.util import CNODCError, HaltFlag, ungzip_with_halt


class FileInfo:
    """Represents information about a file submitted to the workflow process.

    Note that this is NOT a file in the database but a raw file on storage
    somewhere (e.g. an Azure Blob or an Azure File Share).
    """

    def __init__(self,
                 file_path: t.Optional[str] = None,
                 filename: t.Optional[str] = None,
                 is_gzipped: t.Optional[bool] = None,
                 last_modified_date: t.Optional[datetime.datetime] = None):
        self.file_path = file_path
        if filename is None or is_gzipped is None:
            p = pathlib.Path(file_path)
            if filename is None:
                filename = p.name
            if is_gzipped is None:
                is_gzipped = p.name.lower().endswith(".gz")
        self.filename = filename
        self.is_gzipped = is_gzipped
        self.last_modified_date = last_modified_date

    def to_map(self) -> dict:
        """Convert the object into a map."""
        map_ = {
            'file_path': self.file_path,
            'filename': self.filename,
            'is_gzipped': self.is_gzipped,
        }
        if self.last_modified_date is not None:
            map_['mod_date'] = self.last_modified_date.isoformat()
        return map_

    @staticmethod
    def from_map(map_: dict):
        """Build the object from a map."""
        if 'file_path' not in map_:
            raise CNODCError('Missing file path', 'PAYLOAD', 1005)
        return FileInfo(
            map_['file_path'],
            map_['filename'] if 'filename' in map_ else None,
            map_['is_gzipped'] if 'is_gzipped' in map_ else None,
            datetime.datetime.fromisoformat(map_['mod_date']) if 'mod_date' in map_ and map_['mod_date'] else None
        )


class WorkflowPayload:
    """Generalized class for all workflow payloads"""

    def __init__(self,
                 workflow_name: t.Optional[str] = None,
                 current_step: t.Optional[str] = None,
                 metadata: t.Optional[dict] = None,
                 current_step_done: bool = False):
        self.workflow_name = workflow_name
        self.current_step = current_step
        self.current_step_done = current_step_done
        self.metadata = metadata or {}

    def load_workflow(self,
                      db: NODBControllerInstance,
                      halt_flag: HaltFlag = None):
        """Find the workflow associated with this payload and load the controller for it."""
        from cnodc.workflow.workflow import WorkflowController
        workflow_config = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
        if workflow_config is None:
            raise CNODCError(f'Invalid workflow name: [{self.workflow_name}]', is_recoverable=True)
        return WorkflowController(
            workflow_name=self.workflow_name,
            config=workflow_config.configuration,
            halt_flag=halt_flag
        )

    def set_metadata(self, key, value):
        """Set a metadata property (or delete it if the value is None)"""
        if value is not None:
            self.metadata[key] = value
        elif key in self.metadata:
            del self.metadata[key]

    def get_metadata(self, key, default=None):
        """Retrieve a metadata property, or the default if it is not present"""
        if key in self.metadata and self.metadata[key] is not None:
            return self.metadata[key]
        return default

    def clone(self):
        """Create a deep copy of the payload."""
        return WorkflowPayload.from_map(self.to_map())

    def set_subqueue_name(self, subqueue_name: t.Optional[str]):
        """Set the name of the subqueue for the queue item."""
        self.set_metadata('manual-subqueue', subqueue_name)

    def set_unique_key(self, item_key: t.Optional[str]):
        """Set the unique key for the queue item."""
        self.set_metadata('unique-item-key', item_key)

    def set_priority(self, new_priority: t.Optional[int]):
        """Set the priority of the queue item."""
        self.set_metadata('queue-priority', new_priority)

    def set_followup_queue(self, queue_name: t.Optional[str]):
        """Set the follow-up queue after a manual review cycle"""
        self.set_metadata('followup-queue', queue_name)

    def increment_priority(self, increment: int = 1):
        """Increment the priority by the given amount."""
        if 'queue-priority' in self.metadata and isinstance(self.metadata['queue-priority'], int):
            self.metadata['queue-priority'] = self.metadata['queue-priority'] + increment
        else:
            self.metadata['queue-priority'] = increment

    def decrement_priority(self, increment: int = 1):
        """Decrement the priority by the given amount."""
        self.increment_priority(increment * -1)

    def enqueue(self,
                db: NODBControllerInstance,
                queue_name: t.Optional[str] = None,
                override_priority: t.Optional[int] = None):
        """Enqueue this payload in the given queue."""
        if queue_name is None:
            if 'followup-queue' in self.metadata and self.metadata['followup-queue']:
                queue_name = self.metadata['followup-queue']
        if queue_name is None:
            raise CNODCError("Missing queue name")
        if self.metadata is None:
            self.metadata = {}
        self.metadata['send_time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        kwargs = {
            'queue_name': queue_name,
            'data': self.to_map(),
        }
        if override_priority is not None:
            kwargs['priority'] = override_priority
        elif 'queue-priority' in self.metadata and isinstance(self.metadata['queue-priority'], int):
            kwargs['priority'] = self.metadata['queue-priority']
        if 'manual-subqueue' in self.metadata and self.metadata['manual-subqueue']:
            kwargs['subqueue_name'] = self.metadata['manual-subqueue']
        if 'unique-item-key' in self.metadata and self.metadata['unique-item-key']:
            kwargs['unique_item_name'] = self.metadata['unique-item-key']
        db.create_queue_item(**kwargs)

    def copy_details_from(self, payload, next_step: bool = False):
        """Copy key details (workflow info and metadata) from another payload into this one."""
        self.workflow_name = payload.workflow_name
        self.current_step = payload.current_step
        if next_step:
            self.current_step_done = True
        for key in payload.metadata:
            if key not in self.metadata:
                self.metadata[key] = payload.metadata[key]

    def to_map(self) -> dict:
        """Convert this object to a map."""
        return {
            'workflow': {
                'name': self.workflow_name,
                'step': self.current_step,
                'step_done': self.current_step_done,
            },
            'metadata': self.metadata
        }

    @injector.inject
    def _do_download(self,
                 file_path: str,
                 filename: str,
                 is_gzipped: bool,
                 target_dir: t.Union[str, pathlib.Path],
                 halt_flag: HaltFlag = None,
                 files: StorageController = None) -> pathlib.Path:
        """Download the file to a given directory."""
        if isinstance(target_dir, str):
            target_dir = pathlib.Path(target_dir)
        handle = files.get_handle(file_path, halt_flag=halt_flag)
        if handle is None:
            raise CNODCError('Cannot handle file path', 'PAYLOAD', 2000)
        if not handle.exists():
            raise CNODCError('File path does not exist', 'PAYLOAD', 2001)
        target_name = filename
        if is_gzipped:
            if target_name.lower().endswith('.gz'):
                gzip_target_name = target_name
                target_name = target_name[:-3]
            else:
                gzip_target_name = f"{target_name}.gz"
            gzip_path = target_dir / gzip_target_name
            target_path = target_dir / target_name
            handle.download(gzip_path)
            ungzip_with_halt(gzip_path, target_path, halt_flag=halt_flag)
            return target_path
        else:
            file_path = target_dir / target_name
            handle.download(file_path)
            return file_path

    @staticmethod
    def from_queue_item(queue_item: structures.NODBQueueItem):
        """Create a workflow payload from a queue item."""
        return WorkflowPayload.from_map(queue_item.data)

    @staticmethod
    def from_map(data: dict):
        """Create a workflow payload from a map."""
        if 'workflow' not in data:
            raise CNODCError('Missing item.data[workflow]', 'PAYLOAD', 1000)
        if 'name' not in data['workflow']:
            raise CNODCError('Missing item.data[workflow][name]', 'PAYLOAD', 1001)
        if 'step' not in data['workflow']:
            raise CNODCError('Missing item.data[workflow][step]', 'PAYLOAD', 1002)
        if 'step_done' not in data['workflow']:
            raise CNODCError('Missing item.data[workflow][step_done]', 'PAYLOAD', 1004)
        base_kwargs = {
                'workflow_name': data['workflow']['name'],
                'current_step': data['workflow']['step'],
                'current_step_done': data['workflow']['step_done'],
                'metadata': {x: data['metadata'][x] for x in data['metadata']} if 'metadata' in data else None
        }
        if 'file_info' in data:
            return FilePayload.from_map(data['file_info'], **base_kwargs)
        elif 'item_info' in data:
            return ObservationPayload.from_map(data['item_info'], **base_kwargs)
        elif 'source_info' in data:
            return SourceFilePayload.from_map(data['source_info'], **base_kwargs)
        elif 'batch_info' in data:
            return BatchPayload.from_map(data['batch_info'], **base_kwargs)
        else:
            raise CNODCError('Unknown workflow payload type', 'PAYLOAD', 1003)


class FilePayload(WorkflowPayload):
    """Workflow payload for a physical file on disk."""

    def __init__(self,
                 file_info: FileInfo,
                 **kwargs):
        super().__init__(**kwargs)
        self.file_info = file_info

    def to_map(self):
        map_ = super().to_map()
        map_['file_info'] = self.file_info.to_map()
        return map_

    def download(self,
                 target_dir: t.Union[str, pathlib.Path],
                 halt_flag: HaltFlag = None) -> pathlib.Path:
        return self._do_download(self.file_info.file_path, self.file_info.filename, self.file_info.is_gzipped, target_dir, halt_flag)

    @staticmethod
    def from_path(path: str, mod_date: t.Optional[datetime.datetime] = None, **kwargs):
        return FilePayload(
            file_info=FileInfo(path, last_modified_date=mod_date),
            **kwargs
        )

    @staticmethod
    def from_map(map_: dict, **kwargs):
        return FilePayload(
            file_info=FileInfo.from_map(map_),
            **kwargs
        )


class SourceFilePayload(WorkflowPayload):
    """Represent a source file in the database."""

    def __init__(self,
                 source_file_uuid: str,
                 received_date: datetime.date,
                 **kwargs):
        super().__init__(**kwargs)
        self.source_uuid = source_file_uuid
        self.received_date = received_date

    def load_source_file(self, db: NODBControllerInstance, **kwargs) -> structures.NODBSourceFile:
        """Load the referenced source file from the database."""
        source_file = structures.NODBSourceFile.find_by_uuid(
            db=db,
            source_uuid=self.source_uuid,
            received=self.received_date, **kwargs
        )
        if source_file is None:
            raise CNODCError('Invalid payload, no such UUID', 'PAYLOAD', 1012, is_recoverable=False)
        return source_file

    def download(self,
                 db: NODBControllerInstance,
                 target_dir: t.Union[str, pathlib.Path],
                 halt_flag: HaltFlag = None) -> pathlib.Path:
        source_info = self.load_source_file(db)
        return self._do_download(source_info.source_path, source_info.file_name, source_info.source_path.lower().endswith(".gz"), target_dir, halt_flag)

    def to_map(self):
        map_ = super().to_map()
        map_['source_info'] = {
            'source_uuid': self.source_uuid,
            'received': self.received_date.isoformat()
        }
        return map_

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'source_uuid' not in map_:
            raise CNODCError('Missing source_uuid', 'PAYLOAD', 1007)
        if 'received' not in map_:
            raise CNODCError('Missing received date', 'PAYLOAD', 1008)
        return SourceFilePayload(
            map_['source_uuid'],
            datetime.date.fromisoformat(map_['received']),
            **kwargs
        )

    @staticmethod
    def from_source_file(source_file: structures.NODBSourceFile, **kwargs):
        """Build a source file payload from a given source file."""
        return SourceFilePayload(
            source_file_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            **kwargs
        )


class BatchPayload(WorkflowPayload):
    """Represents a payload referencing an NODB batch."""

    def __init__(self,
                 batch_uuid: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.batch_uuid = batch_uuid

    def to_map(self):
        map_ = super().to_map()
        map_['batch_info'] = {
            'uuid': self.batch_uuid
        }
        return map_

    def load_batch(self, db: NODBControllerInstance, **kwargs) -> structures.NODBBatch:
        """Load the referenced batch from the database."""
        batch = structures.NODBBatch.find_by_uuid(
            db=db,
            batch_uuid=self.batch_uuid, **kwargs
        )
        if batch is None:
            raise CNODCError('Invalid batch, no such UUID', 'PAYLOAD', 1013)
        return batch

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing uuid', 'PAYLOAD', 1009)
        return BatchPayload(
            map_['uuid'],
            **kwargs
        )

    @staticmethod
    def from_batch(batch: structures.NODBBatch, **kwargs):
        """Build a payload from a batch object"""
        return BatchPayload(batch.batch_uuid, **kwargs)


class ObservationPayload(WorkflowPayload):
    """A payload referencing a specific observation in the database."""

    def __init__(self,
                 item_uuid: str,
                 item_received: datetime.date,
                 **kwargs):
        super().__init__(**kwargs)
        self.uuid = item_uuid
        self.received_date = item_received

    def to_map(self):
        map_ = super().to_map()
        map_['item_info'] = {
            'uuid': self.uuid,
            'received': self.received_date.isoformat()
        }
        return map_

    def load_observation(self, db, **kwargs) -> structures.NODBObservation:
        """Find the related observation in the database"""
        obs = structures.NODBObservation.find_by_uuid(db, self.uuid, self.received_date, **kwargs)
        if obs is None:
            raise CNODCError('No such observation', 'PAYLOAD', 1010, is_recoverable=False)
        return obs

    def load_observation_data(self, db, **kwargs) -> structures.NODBObservationData:
        """Find the related observation data in the database"""
        obs_data = structures.NODBObservationData.find_by_uuid(db, self.uuid, self.received_date, **kwargs)
        if obs_data is None:
            raise CNODCError('No such observation data', 'PAYLOAD', 1011, is_recoverable=False)
        return obs_data

    @staticmethod
    def from_map(map_: dict, **kwargs):
        if 'uuid' not in map_:
            raise CNODCError('Missing item uuid', 'PAYLOAD', 1005)
        if 'received' not in map_:
            raise CNODCError('Missing item received date', 'PAYLOAD', 1006)
        return ObservationPayload(
            map_['uuid'],
            datetime.date.fromisoformat(map_['received']),
            **kwargs
        )

    @staticmethod
    def from_observation(obs: t.Union[structures.NODBObservation, structures.NODBObservationData], **kwargs):
        """Build a payload from an observation or observation data object."""
        return ObservationPayload(
            obs.obs_uuid,
            obs.received_date,
            **kwargs
        )
