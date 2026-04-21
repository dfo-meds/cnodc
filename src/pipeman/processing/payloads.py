import copy
import datetime
import hashlib
import pathlib
import re
import typing as t
from types import EllipsisType

from autoinject import injector

import nodb as nodb_
from nodb import NODBWorkingRecord, NODBQueueItem
from medsutil.storage import StorageController, FilePath

from pipeman.exceptions import CNODCError
from medsutil.halts import HaltFlag, ungzip_with_halt
from medsutil.awaretime import AwareDateTime
from medsutil.datadict import DataDictObject, p_str, p_bool, p_date, p_awaretime, p_dict


class Payload(DataDictObject):

    metadata: dict = p_dict()
    worker_config: dict = p_dict()

    def __init__(self,
                 deduplicate_key: t.Optional[str] = None,
                 priority: t.Optional[int] = None,
                 correlation_id: t.Optional[str] = None,
                 subqueue_name: t.Optional[str] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self._priority = priority or 0
        self._subqueue_name = subqueue_name or None
        self._deduplicate_key = deduplicate_key or None
        self._correlation_id = correlation_id or None

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

    @property
    def correlation_id(self) -> t.Optional[str]:
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, uuid: str):
        self._correlation_id = uuid or None

    @property
    def subqueue_name(self) -> t.Optional[str]:
        return self._subqueue_name

    @subqueue_name.setter
    def subqueue_name(self, subqueue_name: t.Optional[str]):
        self._subqueue_name = subqueue_name or None

    @property
    def deduplicate_key(self) -> t.Optional[str]:
        return self._deduplicate_key

    @deduplicate_key.setter
    def deduplicate_key(self, unique_name: t.Optional[str]):
        self._deduplicate_key = unique_name or None

    @property
    def priority(self) -> int:
        return self._priority

    @priority.setter
    def priority(self, priority: int):
        self._priority = priority or 0

    @property
    def followup_queue(self):
        return self.get_metadata('followup-queue')

    @followup_queue.setter
    def followup_queue(self, followup_queue: t.Optional[str]):
        self.set_metadata('followup-queue', str(followup_queue) if followup_queue else None)

    def enqueue(self,
                db: nodb_.NODBInstance,
                queue_name: t.Optional[str] = None,
                override_priority: t.Optional[int] = None):
        """Enqueue this payload in the given queue."""
        queue_name = queue_name or self.followup_queue
        if queue_name is None:
            raise CNODCError("Missing queue name", 'PAYLOAD', 1001)
        db.create_queue_item(
            queue_name=queue_name,
            data=self.export(),
            priority=override_priority or self.priority,
            subqueue_name=self.subqueue_name,
            unique_item_name=self.deduplicate_key,
            correlation_id=self.correlation_id
        )

    def to_queue_item(self, queue_name: str) -> NODBQueueItem:
        qi = NODBQueueItem()
        qi.queue_name = queue_name
        qi.data = self.export()
        qi.priority = self.priority
        qi.subqueue_name = self.subqueue_name
        qi.unique_item_name = self.deduplicate_key
        qi.correlation_id = self.correlation_id or ''
        return qi

    def clone(self):
        """Create a deep copy of the payload."""
        return Payload.from_map(self.export())

    def copy_details_from(self, payload):
        """Copy key details (workflow info and metadata and correlation_id) from another payload into this one."""
        for key in payload.metadata:
            if key not in self.metadata:
                self.metadata[key] = copy.deepcopy(payload.metadata[key])
        self.correlation_id = payload.correlation_id

    @staticmethod
    @injector.inject
    def _do_download(file_path: str,
                     filename: str,
                     is_gzipped: bool,
                     target_dir: pathlib.Path,
                     halt_flag: HaltFlag = None,
                     files: StorageController = None) -> pathlib.Path:
        """Download the file to a given directory."""
        handle = files.get_filepath(file_path, halt_flag=halt_flag)
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
            fp = target_dir / target_name
            handle.download(fp)
            return fp

    @staticmethod
    def from_queue_item(queue_item: nodb_.NODBQueueItem):
        """Create a workflow payload from a queue item."""
        payload = Payload.from_map(queue_item.data)
        payload.priority = queue_item.priority
        payload.deduplicate_key = queue_item.unique_item_name
        payload.subqueue_name = queue_item.subqueue_name
        return payload


class WorkflowPayload(Payload):
    """Generalized class for all workflow payloads"""

    workflow_name: str = p_str()
    current_step: str = p_str()
    current_step_done: bool = p_bool(default=False)

    def load_workflow(self,
                      db: nodb_.NODBInstance,
                      halt_flag: HaltFlag = None):
        """Find the workflow associated with this payload and load the controller for it."""
        from pipeman.processing.workflow import WorkflowController
        workflow_config = nodb_.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
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

    file_path: str = p_str(required=True)
    filename: str = p_str()
    is_gzipped: bool = p_bool(default=None)
    last_modified_date: datetime.datetime = p_awaretime()

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

    source_uuid: str = p_str()
    received_date: str = p_date()

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<SourceFilePayload:{self.source_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_source_file(self, db: nodb_.NODBInstance, **kwargs) -> nodb_.NODBSourceFile:
        """Load the referenced source file from the database."""
        source_file = nodb_.NODBSourceFile.find_by_uuid(
            db=db,
            source_uuid=self.source_uuid,
            received=self.received_date, **kwargs
        )
        if source_file is None:
            raise CNODCError('Invalid payload, no such UUID', 'PAYLOAD', 1012, is_transient=False)
        return source_file

    def download_from_db(self, db: nodb_.NODBInstance, target_dir: pathlib.Path, halt_flag: t.Optional[HaltFlag] = None) -> pathlib.Path:
        source_info = self.load_source_file(db)
        return self._do_download(source_info.source_path, source_info.file_name, source_info.source_path.lower().endswith(".gz"), target_dir, halt_flag)

    @staticmethod
    def from_source_file(source_file: nodb_.NODBSourceFile, **kwargs):
        """Build a source file payload from a given source file."""
        return SourceFilePayload(
            source_uuid=source_file.source_uuid,
            received_date=source_file.received_date,
            **kwargs
        )


class BatchPayload(WorkflowPayload):
    """Represents a payload referencing an NODB batch."""

    batch_uuid = p_str()

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<BatchPayload:{self.batch_uuid}:{self.workflow_name}:{self.current_step}>'

    def load_batch(self, db: nodb_.NODBInstance, **kwargs) -> nodb_.NODBBatch:
        """Load the referenced batch from the database."""
        batch = nodb_.NODBBatch.find_by_uuid(
            db=db,
            batch_uuid=self.batch_uuid, **kwargs
        )
        if batch is None:
            raise CNODCError('Invalid batch, no such UUID', 'PAYLOAD', 1013)
        return batch
    @staticmethod
    def from_batch(batch: nodb_.NODBBatch, **kwargs):
        """Build a payload from a batch object"""
        return BatchPayload(batch_uuid=batch.batch_uuid, **kwargs)

class WorkingRecordPayload(WorkflowPayload):
    """A payload referencing a specific observation in the database."""

    working_uuid = p_str()
    received_date = p_date()

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<WorkingRecordPayload:{self.working_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_working_record(self, db, **kwargs) -> nodb_.NODBWorkingRecord:
        """Find the related observation in the database"""
        record = nodb_.NODBWorkingRecord.find_by_uuid(db, working_uuid=self.working_uuid, received_date=self.received_date, **kwargs)
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

    obs_uuid = p_str()
    received_date = p_date()

    def __str__(self):  # pragma: no coverage (debugging only)
        return f'<ObservationPayload:{self.obs_uuid}:{self.received_date}:{self.workflow_name}:{self.current_step}>'

    def load_observation(self, db, **kwargs) -> nodb_.NODBObservation:
        """Find the related observation in the database"""
        obs = nodb_.NODBObservation.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)
        if obs is None:
            raise CNODCError('No such observation', 'PAYLOAD', 1010, is_transient=False)
        return obs

    def load_observation_data(self, db, **kwargs) -> nodb_.NODBObservationData:
        """Find the related observation data in the database"""
        obs_data = nodb_.NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)
        if obs_data is None:
            raise CNODCError('No such observation data', 'PAYLOAD', 1011, is_transient=False)
        return obs_data

    @staticmethod
    def from_observation(obs: t.Union[nodb_.NODBObservation, nodb_.NODBObservationData], **kwargs):
        """Build a payload from an observation or observation data object."""
        return ObservationPayload(
            obs_uuid=obs.obs_uuid,
            received_date=obs.received_date,
            **kwargs
        )


class NewFilePayload(Payload):

    file_path: str = p_str()
    filename: str = p_str()
    modified_time: AwareDateTime | None = p_awaretime()
    workflow_name: str = p_str()
    remove_when_complete: bool = p_bool(default=False)

    def download(self, target_dir: pathlib.Path, halt_flag: t.Optional[HaltFlag] = None) -> pathlib.Path:
        return Payload._do_download(
            file_path=self.file_path,
            filename=self.filename,
            is_gzipped=self.filename.endswith('.gz'),
            target_dir=target_dir,
            halt_flag=halt_flag
        )

    @staticmethod
    def from_handle(handle: FilePath, modified_time: datetime.datetime | EllipsisType | None = ..., **kwargs):
        path = handle.path()
        return NewFilePayload(
            file_path=path,
            filename=handle.name,
            modified_time=handle.modified_datetime() if modified_time is Ellipsis or modified_time is None else modified_time,
            deduplicate_key=hashlib.md5(path.encode('utf-8', 'replace')).hexdigest(),
            **kwargs
        )

    @staticmethod
    def from_path(path: pathlib.Path, **kwargs):
        path = path.absolute().resolve()
        return NewFilePayload(
            file_path=str(path),
            filename=path.name,
            modified_time=AwareDateTime.fromtimestamp(path.stat().st_mtime),
            deduplicate_key=hashlib.md5(str(path).encode('utf-8', 'replace')).hexdigest(),
            **kwargs
        )
