import enum
import hashlib
import pathlib
import secrets
import shutil
import uuid
from base64 import b64decode, b64encode

import flask
import typing as t
from autoinject import injector

from gcflask.user import current_user
from medsutil import json
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.secure import check_password, generate_salt, generate_secure_key, hash_password
from nodb.interface import NODBInstance, NODB
from nodb.workflow import NODBUploadWorkflow, WorkflowConfiguration
from pipeman.processing.workflow import WorkflowController


class IntakeAPIError(CodedError): CODE_SPACE = "INTAKE"


@injector.injectable
class IntakeManager:

    nodb: NODB = None

    @injector.construct
    def __init__(self):
        ...

    def _load_workflow(self, workflow_name: str, db: NODBInstance) -> NODBUploadWorkflow:
        workflow = NODBUploadWorkflow.find_by_name(db, workflow_name)
        if not workflow:
            raise IntakeAPIError(f"No workflow found for {workflow_name}", 1000)
        if not self._check_access(workflow):
            raise IntakeAPIError(f"No access to workflow {workflow_name}", 1001)
        return workflow

    def _check_access(self, workflow: NODBUploadWorkflow) -> bool:
        return workflow.check_access(current_user().permissions)

    def _properties(self, workflow: NODBUploadWorkflow):
        info = {
            'max_chunk_size': flask.current_app.config["MAX_CONTENT_LENGTH"],
            'actions': {
                'submit': {
                    'endpoint': flask.url_for('intake.submit_file', workflow_name=workflow.workflow_name, _external=True),
                },
                'info': {
                    'endpoint': flask.url_for('intake.workflow_info', workflow_name=workflow.workflow_name, _external=True),
                }
            },
        }
        return info

    def list_workflows(self) -> dict:
        with self.nodb as db:
            workflows = {}
            for workflow in NODBUploadWorkflow.find_all(db):
                if not self._check_access(workflow):
                    continue
                workflows[workflow.workflow_name] = self._properties(workflow)
            return {
                'success': True,
                'message': 'Success',
                'data': workflows
            }

    def workflow_info(self, workflow_name: str) -> dict:
        with self.nodb as db:
            workflow = self._load_workflow(workflow_name, db)
            return {
                'success': True,
                'message': 'Success',
                'data': self._properties(workflow)
            }

    def submit_file(self, workflow_name: str, data: bytes, metadata: dict[str, str], request_id: str | None = None) -> dict:
        with self.nodb as db:
            workflow = self._load_workflow(workflow_name, db)
            handler = RequestHandler(workflow, request_id)
            result = handler.submit(data, metadata)
            if result is RequestResult.CONTINUE:
                try:
                    wfc = WorkflowController(workflow.workflow_name or '', t.cast(WorkflowConfiguration, workflow.configuration))
                    wfc.handle_incoming_file(
                        local_path=handler.get_working_file(),
                        metadata=handler.load_metadata(),
                        db=db
                    )
                    return {
                        'success': True,
                        'message': 'File has been submitted',
                    }
                finally:
                    handler.cleanup()
            else:
                return {
                    'success': True,
                    'message': 'File has been saved, waiting for more data',
                    'data': {
                        'actions': {
                            'submit-followup': {
                                'endpoint': flask.url_for(
                                    'intake.submit_followup_file',
                                    _external=True,
                                    workflow_name=workflow_name,
                                    request_id=handler.request_id,
                                ),
                                'headers': {
                                    'x-cnodc-token': handler.create_token()
                                }
                            }
                        }
                    }
                }


    def cancel_upload(self, workflow_name: str, request_id: str, metadata: dict[str, str]) -> dict:
        with self.nodb as db:
            workflow = self._load_workflow(workflow_name, db)
            handler = RequestHandler(workflow, request_id)
            handler.cancel(metadata)
            return {
                "success": True,
                "message": "Upload cancelled"
            }


class RequestResult(enum.Enum):
    CONTINUE = 1
    COMPLETE = 2

class RequestHandler:

    NO_SAVE_HEADERS = {
        'x-cnodc-token',
        'x-cnodc-checksum',
        'x-cnodc-more-data',
    }

    def __init__(self, workflow: NODBUploadWorkflow, request_id: str | None = None):
        self._workflow: NODBUploadWorkflow = workflow
        self._request_id: str | None = request_id
        self._request_directory: pathlib.Path | None = None

    @property
    def request_id(self) -> str:
        if self._request_id is None:
            request_id = str(uuid.uuid4())
            while self._request_directory_path(request_id).exists():
                request_id = str(uuid.uuid4())
            self._request_id = request_id
        return t.cast(str, self._request_id)

    @property
    def request_directory(self) -> pathlib.Path:
        if self._request_directory is None:
            try:
                self._request_directory = self._request_directory_path(self.request_id)
                self._request_directory.mkdir(0o660, parents=True, exist_ok=True)
                if not self._request_directory.is_dir():
                    raise IntakeAPIError("Could not make a request directory", 2001)
            except OSError as ex:
                raise IntakeAPIError("Could not make a request directory", 2002) from ex
        return t.cast(pathlib.Path, self._request_directory)

    @property
    def metadata_file(self):
        return self.request_directory / '.metadata.json'

    @property
    def salt_file(self):
        return self.request_directory / ".salt"

    @property
    def hash_file(self):
        return self.request_directory / ".hash"

    @property
    def time_file(self):
        return self.request_directory / ".time"

    def _request_directory_path(self, request_id: str) -> pathlib.Path:
        uploads = flask.current_app.config.get("UPLOAD_FOLDER", None)
        if not uploads:
            raise IntakeAPIError(f"No upload folder configured", 2000)
        upload_dir = pathlib.Path(uploads)
        return upload_dir / 'requests' / str(self._workflow.workflow_name) / request_id

    def _verify_token(self, token_str: str):
        if self._request_id is not None:
            token = b64decode(token_str)
            salt_file = self.salt_file
            if not (salt_file.exists() and salt_file.is_file()):
                raise IntakeAPIError("No salt file", 2100)
            hash_file = self.hash_file
            if not (hash_file.exists() and hash_file.is_file()):
                raise IntakeAPIError("No token file", 2101)
            with open(salt_file, "rb") as h:
                token_salt = h.read()
            with open(hash_file, "rb") as h:
                token_hash = h.read()
            if not check_password(token, token_salt, token_hash):
                raise IntakeAPIError("Invalid token", 2102)

    def create_token(self) -> str:
        salt = generate_salt()
        with open(self.request_directory / ".salt", "wb") as h:
            h.write(salt)
        secure_key = generate_secure_key()
        with open(self.request_directory / ".token", "wb") as h:
            h.write(hash_password(secure_key, salt))
        return b64encode(secure_key).decode('ascii')

    def _check_data_integrity(self, data: bytes, md5_expected: str):
        md5_actual = hashlib.md5(data, usedforsecurity=False).hexdigest()
        if md5_expected.lower() != md5_actual.lower():
            raise IntakeAPIError("Invalid MD5 checksum", 2200)

    def _update_metadata(self, headers: dict[str, str]) -> dict[str, str]:
        metadata: dict[str, str] = {
            'workflow-name': str(self._workflow.workflow_name),
            'request-id': self.request_id,
            'source': 'web',
            'correlation_id': self.request_id,
            'user': current_user().get_username(),
            'default-filename': self.request_id,
        }
        metadata.update(self.load_metadata())
        for name, value in headers.items():
            if name.startswith("x-cnodc-") and name not in self.NO_SAVE_HEADERS:
                metadata[name[8:]] = value.strip()
        with open(self.metadata_file, 'w') as h:
            h.write(json.dumps(metadata))
        return metadata

    def load_metadata(self) -> dict[str, str]:
        md_file = self.metadata_file
        if md_file.exists():
            with open(md_file, "r") as h:
                return t.cast(dict[str, str], json.load_dict(h.read()))
        return {}

    def get_working_file(self) -> pathlib.Path:
        files: list[pathlib.Path] = []
        idx = 0
        part_file = self._part_file_path(idx)
        while part_file.exists():
            files.append(part_file)
            idx += 1
            part_file = self._part_file_path(idx)

        if len(files) == 0:
            raise IntakeAPIError("No working files", 2400)
        elif len(files) == 1:
            return files[0]
        else:
            assembled = self.request_directory / "assembled.bin"
            with open(self.request_directory / "assembled.bin", "wb") as h_final:
                for part_file in files:
                    with open(part_file, "rb") as h_input:
                        shutil.copyfileobj(h_input, h_final, 1024 * 1024)
            return assembled

    def _part_file_path(self, idx: int) -> pathlib.Path:
        return self.request_directory / f'part.{idx}.bin'

    def _save_data(self, data: bytes):
        if data:
            max_size = self._workflow.configuration.max_file_size
            idx = 0
            part_file = self._part_file_path(idx)
            total_size = 0
            while part_file.exists():
                if max_size is not None:
                    total_size += part_file.stat().st_size
                idx += 1
                part_file = self._part_file_path(idx)
            if max_size is not None and (total_size + len(data)) > max_size:
                self.cleanup()
                raise IntakeAPIError(f"Maximum size of {max_size} exceeded", 2300)
            with open(part_file, "wb") as h:
                h.write(data)
            with open(self.time_file, "w") as h:
                h.write(AwareDateTime.utcnow().isoformat())

    def cleanup(self):
        ...

    def submit(self, data: bytes, metadata: dict[str, str]) -> RequestResult:
        if self._request_id is not None:
            self._verify_token(metadata.get('x-cnodc-token', '').strip())
        md5_expected = metadata.get('x-cnodc-checksum', '').strip()
        if md5_expected:
            self._check_data_integrity(data, md5_expected)
        self._save_data(data)
        self._update_metadata(metadata)
        more_data = str(metadata.get('x-cnodc-more-data', '0')).strip()
        if more_data == '1':
            return RequestResult.CONTINUE
        else:
            return RequestResult.COMPLETE

    def cancel(self, metadata: dict[str, str]) -> bool:
        self._verify_token(metadata.get('x-cnodc-token', ''))
        self.cleanup()
        return True
