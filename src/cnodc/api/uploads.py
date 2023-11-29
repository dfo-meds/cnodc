import shutil
import gzip
import flask
import uuid
import pathlib
import secrets
import hashlib
import datetime
import enum
import typing as t
from urllib.parse import quote
from cnodc.util import CNODCError, dynamic_object
from autoinject import injector
from cnodc.nodb import NODBController, NODBControllerInstance, LockType
import yaml

import cnodc.nodb.structures as structures
from .auth import LoginController
from ..storage import StorageController, DirFileHandle
from ..storage.base import StorageTier

NO_SAVE_HEADERS = [
    'x-cnodc-token',
    'x-cnodc-upload-md5',
    'x-cnodc-more-data'
]

VALID_FILENAME_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-."

VALID_METADATA_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_:;.,\\/\"'?!(){}[]@<>=-+*#$&`|~^%"

RESERVED_FILENAMES = (
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
)


class RequestDataIterator:

    def __init__(self, request_dir: pathlib.Path):
        self._request_dir = request_dir

    def __iter__(self, chunk_size: int = 8196):
        idx = 0
        bin_file = self._request_dir / f"part.{idx}.bin"
        while bin_file.exists():
            with open(bin_file, "rb") as h:
                chunk = h.read(chunk_size)
                while chunk != b'':
                    yield chunk
                    chunk = h.read(chunk_size)
            idx += 1
            bin_file = self._request_dir / f"part.{idx}.bin"


class UploadResult(enum.Enum):

    CONTINUE = 1
    COMPLETE = 2


class UploadController:

    nodb: NODBController = None
    login: LoginController = None
    files: StorageController = None

    @injector.construct
    def __init__(self, workflow_name: str, request_id: t.Optional[str] = None, token: t.Optional[str] = None):
        self.workflow_name = workflow_name
        self.request_id = request_id
        self.token = token
        self._request_dir = None
        self._assembled_file = None
        if '/' in self.workflow_name or '\\' in self.workflow_name or '.' in self.workflow_name:
            raise CNODCError('Invalid character in workflow name', 'UPLOADCTRL', 1000)

    def update_workflow_config(self, config: t.Optional[dict], is_active: t.Optional[bool] = None):
        if config is None and is_active is None:
            raise CNODCError("No workflow changes provided", "UPLOADCTRL", 1012)
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

    def _ensure_request_id(self):
        if self.request_id is None:
            self.request_id = str(uuid.uuid4())

    def _request_directory(self) -> pathlib.Path:
        if self._request_dir is None:
            if not flask.current_app.config.get('UPLOAD_FOLDER'):
                raise CNODCError("No upload folder configured", "UPLOADCTRL", 1009)
            upload_dir = pathlib.Path(flask.current_app.config['UPLOAD_FOLDER']).resolve()
            try:
                self._ensure_request_id()
                self._request_dir = upload_dir / self.workflow_name / self.request_id
                self._request_dir.mkdir(0o660, parents=True, exist_ok=True)
                if not self._request_dir.is_dir():
                    self._request_dir = None
                    raise CNODCError("Could not make a request directory, one of the paths is a file", "UPLOADCTRL", 1001)
            except OSError as ex:
                self._request_dir = None
                raise CNODCError("Could not make a request directory", "UPLOADCTRL", 1002) from ex
        return self._request_dir

    def _check_token(self, allow_new: bool = False) -> bool:
        if self.token is None:
            if not allow_new:
                raise CNODCError('No continuation token found in request', 'UPLOADCTRL', 1003)
            return True
        token_file = self._request_directory() / ".token"
        if not token_file.exists():
            raise CNODCError('No continuation token found on the server', 'UPLOADCTRL', 1004)
        with open(token_file, "rb") as h:
            token_hash = h.read()
            if not secrets.compare_digest(token_hash, UploadController.hash_token(self.token)):
                raise CNODCError('Invalid continuation token', 'UPLOADCTRL', 1005)
        return True

    def _save_token(self):
        with open(self._request_directory() / ".token", "wb") as h:
            h.write(UploadController.hash_token(self.token))

    def _load_workflow(self, db: NODBControllerInstance) -> structures.NODBUploadWorkflow:
        workflow: structures.NODBUploadWorkflow = structures.NODBUploadWorkflow.find_by_name(db, self.workflow_name)
        if workflow is None:
            raise CNODCError(f"Workflow [{self.workflow_name}] not found", "UPLOADCTRL", 1006)
        if not workflow.is_active:
            raise CNODCError(f"Workflow [{self.workflow_name}] is not active", "UPLOADCTRL", 1011)
        current_perms = self.login.current_permissions()
        required_perms = workflow.permissions()
        if required_perms:
            if not any(p in current_perms for p in required_perms):
                raise CNODCError(f"Access to [{self.workflow_name}] denied", "UPLOADCTRL", 1007)
        return workflow

    def _check_data_integrity(self, data: bytes, headers: dict[str, str]):
        if 'x-cnodc-upload-md5' in headers:
            md5_sent = headers['x-cnodc-upload-md5'].lower()
            md5_actual = hashlib.md5(data).hexdigest().lower()
            if md5_sent != md5_actual:
                raise CNODCError(f'MD5 mismatch [{md5_sent}] vs [{md5_actual}]', 'UPLOADCTRL', 1008)

    def _save_metadata(self, headers: dict[str, str]) -> dict:
        header_file = self._request_directory() / ".headers.yaml"
        relevant_headers = {
            'workflow_name': self.workflow_name,
            'request_id': self.request_id
        }
        relevant_headers.update(self._load_metadata())
        for header_name in headers:
            if header_name.startswith('x-cnodc-') and header_name not in NO_SAVE_HEADERS:
                relevant_headers[header_name[8:]] = headers[header_name]
        with open(header_file, "w") as h:
            h.write(yaml.safe_dump(relevant_headers))
        return relevant_headers

    def _load_metadata(self) -> dict:
        header_file = self._request_directory() / ".headers.yaml"
        if header_file.exists():
            with open(header_file, "r") as h:
                return yaml.safe_load(h.read()) or {}
        return {}

    def _save_data(self, data: bytes, max_size: int = None):
        idx = 0
        request_dir = self._request_directory()
        bin_file = request_dir / f"part.{idx}.bin"
        total_size = 0
        while bin_file.exists():
            if max_size is not None:
                total_size += bin_file.stat().st_size
            idx += 1
            bin_file = request_dir / f"part.{idx}.bin"
        if max_size is not None and (total_size + len(data)) > max_size:
            raise CNODCError(f"Maximum size of {max_size} exceeded", "UPLOADCTRL", 1013)
        with open(bin_file, "wb") as h:
            h.write(data)
        with open(request_dir / ".timestamp", "w") as h:
            h.write(datetime.datetime.now(datetime.timezone.utc).isoformat())

    def assemble_file(self, gzip_result: bool = True) -> pathlib.Path:
        if self._assembled_file is None:
            request_dir = self._request_directory()
            files = []
            idx = 0
            bin_file = request_dir / f"part.{idx}.bin"
            while bin_file.exists():
                files.append(bin_file)
                idx += 1
                bin_file = request_dir / f"part.{idx}.bin"
            if not files:
                raise CNODCError(f"No files found in request directory", "UPLOADCTRL", 1014)
            if len(files) == 1:
                self._assembled_file = files[0]
            else:
                self._assembled_file = self._request_directory() / "assembled.bin"
                open_fn = gzip.open if gzip_result else open
                with open_fn(self._assembled_file, "wb") as dest:
                    for file in files:
                        with open(file, "rb") as src:
                            shutil.copyfileobj(src, dest)
        return self._assembled_file

    def _complete_request(self, db: NODBControllerInstance, workflow: structures.NODBUploadWorkflow, headers: dict):
        primary_handle = None
        secondary_handle = None
        try:
            gzip_active = bool(workflow.get_config("gzip", True))
            headers['gzip'] = gzip_active
            local_source_file = self.assemble_file(gzip_active)
            validation_target = workflow.get_config("validation", None)
            if validation_target is not None:
                if not dynamic_object(validation_target)(local_source_file, headers):
                    raise CNODCError("Validation failed", "UPLOADCTRL", 1010)
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
            secondary_metadata = self._get_metadata(workflow.get_config('upload_metadata', default={}), headers)
            secondary_upload_tier_name = workflow.get_config('archive_tier', None)
            secondary_upload_tier = StorageTier(secondary_upload_tier_name) if secondary_upload_tier_name else StorageTier.ARCHIVAL
            if gzip_active:
                primary_metadata['Gzip'] = 'Y'
                secondary_metadata['Gzip'] = 'Y'
            if primary_upload_uri is not None:
                with open(local_source_file, "rb") as h:
                    upload_dir = self.files.get_handle(primary_upload_uri)
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
                    archive_dir = self.files.get_handle(secondary_upload_uri)
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
                        'headers': headers,
                        '_metadata': {
                            'source': 'web_upload',
                            'correlation_id': self.request_id,
                            'workflow_name': self.workflow_name,
                            'user': self.login.current_user().username
                        }
                    },
                    priority=workflow.get_config('queue_priority', None),
                    unique_item_key=headers['unique-key'] if 'unique-key' in headers else None
                )
            self._cleanup_request()
        except Exception as ex:
            if primary_handle:
                primary_handle.remove()
            if secondary_handle:
                secondary_handle.remove()
            raise ex

    def _get_metadata(self, metadata, headers):
        return {x: self._substitute_headers(metadata[x], headers) for x in metadata}

    def _substitute_headers(self, v, headers):
        for h in headers:
            v = v.replace("${" + h + "}", headers[h])
        v = v.replace('${now}', datetime.datetime.now(datetime.timezone.utc)).isoformat()
        return quote(v, safe=VALID_METADATA_CHARACTERS)

    def _get_filename(self, headers: dict, is_gzipped: bool = False):
        filename = self._sanitize_filename(headers['filename']) if 'filename' in headers else None
        if filename is None:
            filename = headers['request_id']
        if is_gzipped:
            filename += ".gz"
        return filename

    def _sanitize_filename(self, filename: str):
        filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])
        filename = filename.rstrip(".")
        if len(filename) > 255:
            return None
        check = filename if "." not in filename else filename[:filename.find(".")]
        if check in RESERVED_FILENAMES:
            return None
        return filename

    def _cleanup_request(self):
        request_dir = self._request_directory()
        for f in request_dir.iterdir():
            f.unlink(True)
        request_dir.rmdir()

    def cancel_request(self):
        self._check_token()
        self._cleanup_request()

    def properties(self) -> dict:
        with self.nodb as db:
            workflow = self._load_workflow(db)
            return {
                'max_size': workflow.get_config('max_size', None)
            }

    def upload_request(self, data: bytes, headers: dict[str, str]) -> UploadResult:
        self._check_token(self.request_id is None)
        self._check_data_integrity(data, headers)
        with self.nodb as db:
            workflow = self._load_workflow(db)
            working_headers = self._save_metadata(headers)
            self._save_data(data, workflow.get_config('max_size', None))
            if 'x-cnodc-more-data' in headers and headers['x-cnodc-more-data'] == '1':
                self.token = secrets.token_urlsafe(64)
                self._save_token()
                return UploadResult.CONTINUE
            else:
                self._complete_request(db, workflow, working_headers)
                return UploadResult.COMPLETE

    @staticmethod
    def hash_token(token: str) -> bytes:
        return hashlib.pbkdf2_hmac('sha256', token.encode('utf-8'), salt=flask.current_app.config['SECRET_KEY'], iterations=985123)
