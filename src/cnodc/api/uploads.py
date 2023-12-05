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
from cnodc.util import CNODCError
from autoinject import injector
import yaml

from .auth import LoginController
from cnodc.process.workflows import WorkflowController


NO_SAVE_HEADERS = [
    'x-cnodc-token',
    'x-cnodc-upload-md5',
    'x-cnodc-more-data'
]


class UploadResult(enum.Enum):

    CONTINUE = 1
    COMPLETE = 2


class UploadController(WorkflowController):

    login: LoginController = None

    @injector.construct
    def __init__(self, workflow_name: str, request_id: t.Optional[str] = None, token: t.Optional[str] = None):
        super().__init__(workflow_name)
        self.request_id = request_id
        self.token = token
        self._request_dir = None
        self._assembled_file = None

    def _ensure_request_id(self):
        if self.request_id is None:
            self.request_id = str(uuid.uuid4())

    def _request_directory(self) -> pathlib.Path:
        if self._request_dir is None:
            if not flask.current_app.config.get('UPLOAD_FOLDER'):
                raise CNODCError("No upload folder configured", "UPLOADCTRL", 1000)
            upload_dir = pathlib.Path(flask.current_app.config['UPLOAD_FOLDER']).resolve()
            try:
                self._ensure_request_id()
                self._request_dir = upload_dir / 'requests' / self.workflow_name / self.request_id
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

    def _check_data_integrity(self, data: bytes, headers: dict[str, str]):
        if 'x-cnodc-upload-md5' in headers:
            md5_sent = headers['x-cnodc-upload-md5'].lower()
            md5_actual = hashlib.md5(data).hexdigest().lower()
            if md5_sent != md5_actual:
                raise CNODCError(f'MD5 mismatch [{md5_sent}] vs [{md5_actual}]', 'UPLOADCTRL', 1006)

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
            raise CNODCError(f"Maximum size of {max_size} exceeded", "UPLOADCTRL", 1007)
        with open(bin_file, "wb") as h:
            h.write(data)
        with open(request_dir / ".timestamp", "w") as h:
            h.write(datetime.datetime.now(datetime.timezone.utc).isoformat())

    def get_working_file(self, gzip_working_file: bool = True) -> pathlib.Path:
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
                raise CNODCError(f"No files found in request directory", "UPLOADCTRL", 1008)
            if len(files) == 1:
                self._assembled_file = files[0]
            else:
                self._assembled_file = self._request_directory() / "assembled.bin"
                open_fn = gzip.open if gzip_working_file else open
                with open_fn(self._assembled_file, "wb") as dest:
                    for file in files:
                        with open(file, "rb") as src:
                            shutil.copyfileobj(src, dest)
        return self._assembled_file

    def _current_permissions(self) -> set:
        return self.login.current_permissions()

    def get_queue_metadata(self) -> dict:
        return {
            'source': 'web_upload',
            'correlation_id': self.request_id,
            'workflow_name': self.workflow_name,
            'user': self.login.current_user().username,
            'upload_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    def _cleanup_request(self):
        request_dir = self._request_directory()
        for f in request_dir.iterdir():
            f.unlink(True)
        request_dir.rmdir()

    def cancel_request(self):
        self._check_token()
        self._cleanup_request()

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
                self._cleanup_request()
                return UploadResult.COMPLETE

    @staticmethod
    def hash_token(token: str) -> bytes:
        return hashlib.pbkdf2_hmac('sha256', token.encode('utf-8'), salt=flask.current_app.config['SECRET_KEY'], iterations=985123)
