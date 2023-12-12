import pathlib
import datetime
import typing as t
import uuid
from urllib.parse import quote

import zrlog

from cnodc.util import CNODCError, dynamic_object, HaltFlag
from autoinject import injector
from cnodc.nodb import NODBController, NODBControllerInstance, LockType

import cnodc.nodb.structures as structures
from cnodc.storage import StorageController
from cnodc.storage.base import StorageTier

VALID_METADATA_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_:;.,\\/\"'?!(){}[]@<>=-+*#$&`|~^%"

VALID_FILENAME_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-."

RESERVED_FILENAMES = (
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
)


class WorkflowController:

    nodb: NODBController = None
    files: StorageController = None

    @injector.construct
    def __init__(self, workflow_name: str, halt_flag: HaltFlag = None):
        self.workflow_name = workflow_name
        self._halt_flag = halt_flag
        self._log = zrlog.get_logger("cnodc.workflow")
        if '/' in self.workflow_name or '\\' in self.workflow_name or '.' in self.workflow_name:
            raise CNODCError('Invalid character in workflow name', 'WORKFLOWCTRL', 1000)

    def update_workflow_config(self, config: t.Optional[dict], is_active: t.Optional[bool] = None):
        if config is None and is_active is None:
            raise CNODCError("No workflow changes provided", "WORKFLOWCTRL", 1001)
        with self.nodb as db:
            workflow = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name, lock_type=LockType.FOR_NO_KEY_UPDATE)
            if workflow is None:
                workflow = structures.NODBUploadWorkflow()
                workflow.workflow_name = self.workflow_name
                workflow.is_active = True
                workflow.configuration = {}
            if config is not None:
                workflow.configuration = config
            if is_active is not None:
                workflow.is_active = is_active
            workflow.check_config()
            db.upsert_object(workflow)
            db.commit()

    def _current_permissions(self) -> set:
        return {'_admin', }

    def _load_workflow(self, db: NODBControllerInstance) -> structures.NODBUploadWorkflow:
        workflow: structures.NODBUploadWorkflow = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
        if workflow is None:
            raise CNODCError(f"Workflow [{self.workflow_name}] not found", "WORKFLOWCTRL", 1002)
        if not workflow.is_active:
            raise CNODCError(f"Workflow [{self.workflow_name}] is not active", "WORKFLOWCTRL", 1003)
        current_perms = self._current_permissions()
        if '_admin' not in self._current_permissions():
            required_perms = workflow.permissions()
            if required_perms:
                if not any(p in current_perms for p in required_perms):
                    raise CNODCError(f"Access to [{self.workflow_name}] denied", "WORKFLOWCTRL", 1004)
        return workflow

    def get_working_file(self, gzip_working_file: bool = True) -> pathlib.Path:
        raise NotImplementedError()

    def get_queue_metadata(self) -> dict:
        raise NotImplementedError()

    def file_best_modified_time(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    def _complete_request(self, db: NODBControllerInstance, workflow: structures.NODBUploadWorkflow, headers: dict):
        primary_handle = None
        secondary_handle = None
        try:
            gzip_active = bool(workflow.get_config("gzip", True))
            headers['gzip'] = gzip_active
            local_source_file = self.get_working_file(gzip_active)
            validation_target = workflow.get_config("validation", None)
            if validation_target is not None:
                if not dynamic_object(validation_target)(local_source_file, headers):
                    raise CNODCError("Validation failed", "WORKFLOWCTRL", 1005)
            filename = self._get_filename(headers, gzip_active)
            allow_overwrite = headers['allow-overwrite'] == '1' if 'allow-overwrite' in headers else False
            workflow_allow_overwrite = workflow.get_config('allow_overwrite', None)
            if workflow_allow_overwrite == 'always':
                allow_overwrite = True
            elif workflow_allow_overwrite == 'never':
                allow_overwrite = False
            primary_metadata = self._get_metadata(workflow.get_config('upload_metadata', default={}), headers)
            primary_upload_uri = workflow.get_config('upload', None)
            primary_upload_tier_name = workflow.get_config('upload_tier', None)
            primary_upload_tier = StorageTier(primary_upload_tier_name) if primary_upload_tier_name else StorageTier.FREQUENT
            secondary_metadata = self._get_metadata(workflow.get_config('archive_metadata', default={}), headers)
            secondary_upload_tier_name = workflow.get_config('archive_tier', None)
            secondary_upload_tier = StorageTier(secondary_upload_tier_name) if secondary_upload_tier_name else StorageTier.ARCHIVAL
            if gzip_active:
                primary_metadata['Gzip'] = 'Y'
                secondary_metadata['Gzip'] = 'Y'
            if primary_upload_uri is not None:
                with open(local_source_file, "rb") as h:
                    upload_dir = self.files.get_handle(primary_upload_uri, halt_flag=self._halt_flag)
                    primary_handle = upload_dir.child(filename)
                    primary_handle.upload(
                        h,
                        allow_overwrite=allow_overwrite,
                        storage_tier=primary_upload_tier,
                        metadata=primary_metadata
                    )
            secondary_upload_uri = workflow.get_config('archive', None)
            if secondary_upload_uri is not None:
                with open(local_source_file, "rb") as h:
                    archive_dir = self.files.get_handle(secondary_upload_uri, halt_flag=self._halt_flag)
                    secondary_handle = archive_dir.child(filename)
                    secondary_handle.upload(
                        h,
                        allow_overwrite=allow_overwrite,
                        storage_tier=secondary_upload_tier,
                        metadata=secondary_metadata
                    )
            queue_name = workflow.get_config('queue', None)
            if queue_name is not None:
                db.create_queue_item(
                    queue_name,
                    {
                        'upload_file': primary_handle.path() if primary_handle else None,
                        'archive_file': secondary_handle.path() if secondary_handle else None,
                        'gzip': gzip_active,
                        'filename': filename,
                        'last_modified': self.file_best_modified_time(),
                        'headers': headers,
                        '_metadata': self.get_queue_metadata()
                    },
                    priority=workflow.get_config('queue_priority', None),
                    unique_item_key=headers['unique-key'] if 'unique-key' in headers else None
                )
            self.on_complete(db)
            db.commit()
        except Exception as ex:
            if primary_handle:
                primary_handle.remove()
            if secondary_handle:
                secondary_handle.remove()
            raise ex

    def on_complete(self, db: NODBControllerInstance):
        pass

    def _get_metadata(self, metadata, headers):
        return {x: self._substitute_headers(metadata[x], headers) for x in metadata}

    def _substitute_headers(self, v, headers):
        for h in headers:
            v = v.replace("${" + h + "}", headers[h])
        v = v.replace('${now}', datetime.datetime.now(datetime.timezone.utc)).isoformat()
        return quote(v, safe=VALID_METADATA_CHARACTERS)

    def _get_filename(self, headers: dict, is_gzipped: bool = False):
        filename = self._sanitize_filename(headers['filename']) if 'filename' in headers else None
        if not filename:
            filename = self._default_filename(headers)
        if is_gzipped:
            filename += ".gz"
        return filename

    def _default_filename(self, headers) -> str:
        return str(uuid.uuid4())

    def _sanitize_filename(self, filename: str):
        filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])
        filename = filename.rstrip(".")
        if len(filename) > 255:
            return None
        check = filename if "." not in filename else filename[:filename.find(".")]
        if check in RESERVED_FILENAMES:
            return None
        return filename

    def properties(self) -> dict:
        with self.nodb as db:
            workflow = self._load_workflow(db)
            return {
                'max_size': workflow.get_config('max_size', None)
            }
