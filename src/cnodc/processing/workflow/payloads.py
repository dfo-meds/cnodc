import copy
import datetime
import pathlib
import re
import typing as t

from autoinject import injector

import cnodc.nodb as nodb
from cnodc.nodb import NODBWorkingRecord
from cnodc.storage import StorageController

from cnodc.util import CNODCError, HaltFlag, ungzip_with_halt, dynamic_object, DynamicObjectLoadError
from cnodc.util.datadict import DataDictObject, ddo_property, newdict, ddo_str, ddo_bool, ddo_datetime, ddo_date
from cnodc.util.dynamic import dynamic_name


class Payload(DataDictObject):

    metadata: dict = ddo_property('metadata', default=newdict)
    worker_config: dict = ddo_property('worker_config', default=newdict)

    def __init__(self, cls_name: str = None, **kwargs):
        super().__init__(**kwargs)

    def set_metadata(self, key, value):
        """Set a metadata property (or delete it if the value is None)"""
        if value is not None:
            self.metadata[key] = value
        elif key in self.metadata:
            del self.metadata[key]

    def set_worker_config(self, key, value):
        if value is not None:
            self.worker_config[key] = value
        elif key in self.worker_config:
            del self.worker_config[key]

    def get_metadata(self, key, default=None):
        """Retrieve a metadata property, or the default if it is not present"""
        if key in self.metadata and self.metadata[key] is not None:
            return self.metadata[key]
        return default

    def set_subqueue_name(self, subqueue_name: t.Optional[str]):
        """Set the name of the subqueue for the queue item."""
        self.set_metadata('manual-subqueue', subqueue_name)

    def set_unique_key(self, item_key: t.Optional[str]):
        """Set the unique key for the queue item."""
        self.set_metadata('unique-item-name', item_key)

    def set_priority(self, new_priority: t.Optional[int]):
        """Set the priority of the queue item."""
        self.set_metadata('queue-priority', new_priority)

    def set_followup_queue(self, queue_name: t.Optional[str]):
        """Set the follow-up queue after a manual review cycle"""
        self.set_metadata('followup-queue', queue_name)

    def increment_priority(self, increment: int = 1):
        """Increment the priority by the given amount."""
        self.set_metadata('queue-priority', self.get_metadata('queue-priority', 0) + increment)

    def decrement_priority(self, increment: int = 1):
        """Decrement the priority by the given amount."""
        self.increment_priority(increment * -1)

    def enqueue(self,
                db: nodb.NODBControllerInstance,
                queue_name: t.Optional[str] = None,
                override_priority: t.Optional[int] = None):
        """Enqueue this payload in the given queue."""
        queue_name = queue_name or self.get_metadata('followup-queue', None)
        if queue_name is None:
            raise CNODCError("Missing queue name", 'PAYLOAD', 1001)
        db.create_queue_item(
            queue_name=queue_name,
            data=self.to_map(),
            priority=override_priority or self.get_metadata('queue-priority', None),
            subqueue_name=self.get_metadata('manual-subqueue', None),
            unique_item_name=self.get_metadata('unique-item-name', None)
        )

    def to_map(self):
        map_ = copy.deepcopy(self._data)
        map_['cls_name'] = dynamic_name(self)
        return map_

    def clone(self):
        """Create a deep copy of the payload."""
        return Payload.from_map(self.to_map())

    def copy_details_from(self, payload):
        """Copy key details (workflow info and metadata) from another payload into this one."""
        for key in payload.metadata:
            if key not in self.metadata:
                self.metadata[key] = copy.deepcopy(payload.metadata[key])

    @staticmethod
    @injector.inject
    def _do_download(file_path: str,
                     filename: str,
                     is_gzipped: bool,
                     target_dir: pathlib.Path,
                     halt_flag: HaltFlag = None,
                     files: StorageController = None) -> pathlib.Path:
        """Download the file to a given directory."""
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
    def from_queue_item(queue_item: nodb.NODBQueueItem):
        """Create a workflow payload from a queue item."""
        return Payload.from_map(queue_item.data)

    @staticmethod
    def from_map(data: dict):
        cls_name = None
        try:
            cls_name = data['cls_name']
            cls = dynamic_object(cls_name)
            return cls(**data)
        except KeyError as ex:
            raise CNODCError(f"Invalid payload dictionary, missing [cls_name]", 'PAYLOAD', 1000) from ex
        except DynamicObjectLoadError as ex:
            raise CNODCError(f"Invalid payload dictionary, invalid [cls_name={cls_name}]", 'PAYLOAD', 1002) from ex
        except ValueError as ex:
            raise CNODCError(f'Invalid payload dictionary, missing mandatory entries', 'PAYLOAD', 1003) from ex
        except AttributeError as ex:
            raise CNODCError(f'Invalid payload dictionary, too many entries', 'PAYLOAD', 1004) from ex


class WorkflowPayload(Payload):
    """Generalized class for all workflow payloads"""

    workflow_name: str = ddo_str('workflow_name', default=None)
    current_step: str = ddo_str('current_step', default=None)
    current_step_done: bool = ddo_bool('current_step_done', default=False)

    def load_workflow(self,
                      db: nodb.NODBControllerInstance,
                      halt_flag: HaltFlag = None):
        """Find the workflow associated with this payload and load the controller for it."""
        from cnodc.processing.workflow.workflow import WorkflowController
        workflow_config = nodb.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
        if workflow_config is None:
            raise CNODCError(f'Invalid workflow name: [{self.workflow_name}]', is_transient=True)
        return WorkflowController(
            workflow_name=self.workflow_name,
            config=workflow_config.configuration,
            halt_flag=halt_flag
        )

    def copy_details_from(self, payload, next_step: bool = False):
        super().copy_details_from(payload)
        if isinstance(payload, WorkflowPayload):
            self.workflow_name = payload.workflow_name
            self.current_step = payload.current_step
            if next_step:
                self.current_step_done = True


class FilePayload(WorkflowPayload):
    """Workflow payload for a physical file on disk."""

    file_path: str = ddo_str('file_path')
    filename: str = ddo_str('filename', default=None)
    is_gzipped: bool = ddo_bool('is_gzipped', default=None)
    last_modified_date: datetime.datetime = ddo_datetime('last_modified_date', default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.filename is None and self.file_path is not None:
            pieces = [x.strip() for x in re.split('[/\\\\]', self.file_path) if x.strip()]
            if pieces:
                self.filename = pieces[-1]
                if '?' in self.filename:
                    self.filename = self.filename[:self.filename.find('?')]
                if '#' in self.filename:
                    self.filename = self.filename[:self.filename.find('#')]
        if self.is_gzipped is None and self.filename:
            self.is_gzipped = self.filename.lower().endswith('.gz')

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<FilePayload:{self.file_path}:{self.workflow_name}:{self.current_step}>'

    def download(self, target_dir: pathlib.Path, halt_flag: t.Optional[HaltFlag] = None) -> pathlib.Path:
        return self._do_download(self.file_path, self.filename, self.is_gzipped, target_dir, halt_flag)

    @staticmethod
    def from_path(path: str, mod_date: t.Optional[datetime.datetime] = None, **kwargs):
        return FilePayload(
            file_path=str(path),
            last_modified_date=mod_date,
            **kwargs
        )


class SourceFilePayload(WorkflowPayload):
    """Represent a source file in the database."""

    source_uuid: str = ddo_str('source_uuid')
    received_date: str = ddo_date('received_date')

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<SourceFilePayload:{self.source_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_source_file(self, db: nodb.NODBControllerInstance, **kwargs) -> nodb.NODBSourceFile:
        """Load the referenced source file from the database."""
        source_file = nodb.NODBSourceFile.find_by_uuid(
            db=db,
            source_uuid=self.source_uuid,
            received=self.received_date, **kwargs
        )
        if source_file is None:
            raise CNODCError('Invalid payload, no such UUID', 'PAYLOAD', 1012, is_transient=False)
        return source_file

    def download_from_db(self, db: nodb.NODBControllerInstance, target_dir: pathlib.Path, halt_flag: t.Optional[HaltFlag] = None) -> pathlib.Path:
        source_info = self.load_source_file(db)
        return self._do_download(source_info.source_path, source_info.file_name, source_info.source_path.lower().endswith(".gz"), target_dir, halt_flag)

    @staticmethod
    def from_source_file(source_file: nodb.NODBSourceFile, **kwargs):
        """Build a source file payload from a given source file."""
        return SourceFilePayload(
            source_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            **kwargs
        )


class BatchPayload(WorkflowPayload):
    """Represents a payload referencing an NODB batch."""

    batch_uuid = ddo_str('batch_uuid')

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<BatchPayload:{self.batch_uuid}:{self.workflow_name}:{self.current_step}>'

    def load_batch(self, db: nodb.NODBControllerInstance, **kwargs) -> nodb.NODBBatch:
        """Load the referenced batch from the database."""
        batch = nodb.NODBBatch.find_by_uuid(
            db=db,
            batch_uuid=self.batch_uuid, **kwargs
        )
        if batch is None:
            raise CNODCError('Invalid batch, no such UUID', 'PAYLOAD', 1013)
        return batch
    @staticmethod
    def from_batch(batch: nodb.NODBBatch, **kwargs):
        """Build a payload from a batch object"""
        return BatchPayload(batch_uuid=batch.batch_uuid, **kwargs)

class WorkingRecordPayload(WorkflowPayload):
    """A payload referencing a specific observation in the database."""

    working_uuid = ddo_str('working_uuid')
    received_date = ddo_date('received_date')

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<WorkingRecordPayload:{self.working_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_working_record(self, db, **kwargs) -> nodb.NODBWorkingRecord:
        """Find the related observation in the database"""
        record = nodb.NODBWorkingRecord.find_by_uuid(db, self.working_uuid, self.received_date, **kwargs)
        if record is None:
            raise CNODCError('No such observation', 'PAYLOAD', 1010, is_transient=False)
        return record

    @staticmethod
    def from_working_record(record: NODBWorkingRecord, **kwargs):
        """Build a payload from an observation or observation data object."""
        return ObservationPayload(
            working_uuid=record.working_uuid,
            received_date=record.received_date,
            **kwargs
        )

class ObservationPayload(WorkflowPayload):
    """A payload referencing a specific observation in the database."""

    obs_uuid = ddo_str('obs_uuid')
    received_date = ddo_date('received_date')

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<ObservationPayload:{self.obs_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_observation(self, db, **kwargs) -> nodb.NODBObservation:
        """Find the related observation in the database"""
        obs = nodb.NODBObservation.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)
        if obs is None:
            raise CNODCError('No such observation', 'PAYLOAD', 1010, is_transient=False)
        return obs

    def load_observation_data(self, db, **kwargs) -> nodb.NODBObservationData:
        """Find the related observation data in the database"""
        obs_data = nodb.NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)
        if obs_data is None:
            raise CNODCError('No such observation data', 'PAYLOAD', 1011, is_transient=False)
        return obs_data

    @staticmethod
    def from_observation(obs: t.Union[nodb.NODBObservation, nodb.NODBObservationData], **kwargs):
        """Build a payload from an observation or observation data object."""
        return ObservationPayload(
            obs_uuid=obs.obs_uuid,
            received_date=obs.received_date,
            **kwargs
        )
