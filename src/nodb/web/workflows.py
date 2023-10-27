import datetime
import os
import secrets
import shutil
import uuid
import typing as t
from autoinject import injector
import hashlib

import flask
import pathlib

import yaml
from nodb.web.auth import current_permissions

from cnodc.exc import CNODCError
from cnodc.files import FileController
from cnodc.files.base import StorageTier
from cnodc.nodb import NODBController
from cnodc.util import dynamic_object
from urllib.parse import quote
import inspect

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


def update_workflow_config(workflow_name: str,
                           metadata: dict[str, str] = None,
                           validation_callable: str = None,
                           allow_overwrite: str = None,
                           upload_dir: str = None,
                           upload_tier: str = None,
                           archive_dir: str = None,
                           archive_tier: str = None,
                           queue_name: str = None
                           ) -> dict:
    if allow_overwrite is not None or allow_overwrite in ('always', 'never', 'user'):
        return {'error': f'invalid value for [allow_overwrite]: {allow_overwrite}', 'code': 'SUBMIT-1011'}
    if validation_callable is not None:
        x = dynamic_object(validation_callable)
        if not callable(x):
            return {'error': f'non-callable value for [validation]: {validation_callable}', 'code': 'SUBMIT-1012'}
        sig = inspect.signature(x)
        if not len(sig.parameters) >= 2:
            return {'error': f'wrong number of parameters for [validation]: {validation_callable}', 'code': 'SUBMIT-1013'}


@injector.inject
def check_access(workflow_name, nodb: NODBController = None):
    with nodb as db:
        config = db.get_workflow_config(workflow_name)
        if config is None:
            raise CNODCError(f"invalid workflow [{workflow_name}]", "SUBMIT", 1006)
        if 'permission' in config and not config['permission'] in current_permissions():
            raise CNODCError(f"no access to [{workflow_name}]", "SUBMIT", 1014)
    return True


def cancel_file_upload(workflow_name: str, request_id: str, token: str) -> dict:
    if token is None:
        return {'error': 'missing token for cancelling upload', 'code': 'SUBMIT-1004'}
    request_dir = _get_upload_directory(workflow_name, request_id)
    if not _check_token(request_dir, token):
        return {'error': 'invalid token for cancelling upload', 'code': 'SUBMIT-1005'}
    _cleanup_request(request_dir)
    return {'success': True}


def handle_file_upload(workflow_name: str, headers: dict[str, str], data: bytes, request_id: str = None) -> dict:
    response = {}
    is_first_request = False
    if request_id is None:
        check_access(workflow_name)
        request_id = str(uuid.uuid4())
        is_first_request = True
    request_dir = _get_upload_directory(workflow_name, request_id)
    if not is_first_request:
        if 'x-cnodc-token' not in headers:
            return {'error': 'missing token for continuing upload', 'code': 'SUBMIT-1007'}
        if not _check_token(request_dir, headers['x-cnodc-token']):
            return {'error': 'invalid token for continuing upload', 'code': 'SUBMIT-1008'}
    if 'x-cnodc-upload-md5' in headers:
        md5_sent = headers['x-cnodc-upload-md5'].lower()
        md5_calc = hashlib.md5(flask.request.data).hexdigest().lower()
        if md5_sent != md5_calc:
            return {'error': f'md5 mismatch [{md5_sent}] vs [{md5_calc}]', 'code': 'SUBMIT-1009'}
    _save_file_metadata(request_dir, workflow_name, request_id, headers)
    _save_file_part(request_dir, data)
    if 'x-cnodc-more-data' in headers and headers['x-cnodc-more-data'] == '1':
        next_token = secrets.token_urlsafe(64)
        _save_token(request_dir, next_token)
        response["more_data_endpoint"] = flask.url_for('nodb.submit_file_to_request', workflow_name=workflow_name, request_id=request_id, _external=True)
        response["cancel_endpoint"] = flask.url_for('nodb.cancel_file_submission', workflow_name=workflow_name, request_id=request_id, _external=True)
        response['x-cnodc-token'] = next_token
        response["success"] = True
    else:
        _do_file_upload(request_dir, workflow_name)
        response["success"] = True
    return response


def _get_upload_directory(workflow_name: str, request_id: str) -> pathlib.Path:
    upload_dir = pathlib.Path(flask.current_app.config['UPLOAD_DIRECTORY']).resolve()
    try:
        workflow_dir = upload_dir / workflow_name / request_id
        workflow_dir.mkdir(0o660, parents=True, exist_ok=True)
        if not workflow_dir.is_dir():
            raise CNODCError("Could not make a request directory, one of the paths is a file", "SUBMIT", 1000)
        return workflow_dir
    except OSError:
        raise CNODCError("Could not make a request directory", "SUBMIT", 1001)


def _save_token(request_dir: pathlib.Path, token: str):
    with open(request_dir / ".token", "wb") as h:
        h.write(_hash_token(token))


def _hash_token(token: str) -> bytes:
    return hashlib.pbkdf2_hmac('sha256', token, salt=flask.current_app.config['SECRET_KEY'], iterations=985123)


def _check_token(request_dir: pathlib.Path, token: str):
    with open(request_dir / ".token", "rb") as h:
        token_hash = h.read()
        return secrets.compare_digest(token_hash, _hash_token(token))


def _save_file_metadata(request_dir: pathlib.Path, workflow_name: str, request_id: str, headers: dict[str, str]):
    header_file = request_dir / ".headers.yaml"
    relevant_headers = {'workflow_name': workflow_name, 'request_id': request_id}
    if header_file.exists():
        with open(header_file, "r") as h:
            relevant_headers = yaml.safe_load(h.read()) or {}
    for hname in headers:
        if hname.startswith("x-cnodc-") and hname not in NO_SAVE_HEADERS:
            relevant_headers[hname[8:]] = flask.request.headers.get(hname)
    with open(header_file, "w") as h:
        h.write(yaml.safe_dump(relevant_headers))


def _save_file_part(request_dir: pathlib.Path, data: bytes):
    idx = 0
    next_bin_file = request_dir / f"part{idx}.bin"
    while next_bin_file.exists():
        idx += 1
        next_bin_file = request_dir / f"part{idx}.bin"
    with open(next_bin_file, "wb") as h:
        h.write(data)
    with open(request_dir / ".timestamp", "w") as h:
        h.write(datetime.datetime.utcnow().isoformat())


def _do_file_upload(request_dir: pathlib.Path, workflow_name: str):
    part_files = []
    idx = 0
    next_bin_file = request_dir / f"part{idx}.bin"
    while next_bin_file.exists():
        part_files.append(next_bin_file)
        idx += 1
        next_bin_file = request_dir / f"part{idx}.bin"
    if not part_files:
        raise CNODCError("No files detected", "SUBMIT", 1002)
    full_file = _assemble_files(request_dir, part_files)
    with open(request_dir / ".headers.yaml", "r") as h:
        headers = yaml.safe_load(h.read()) or {}
        _process_request(full_file, headers, workflow_name)
    _cleanup_request(request_dir)


@injector.inject
def _process_request(local_file: pathlib.Path, headers: dict[str, t.Any], workflow_name: str, nodb: NODBController = None, fc: FileController = None):
    with nodb as db:
        upload_handle = None
        archive_handle = None
        try:
            config = db.get_workflow_config(workflow_name)
            if config is None:
                raise CNODCError("No workflow found", "SUBMIT", 1003)
            filename = _sanitize_filename(headers['filename']) if 'filename' in headers else None
            if filename is None:
                filename = headers['request_id']
            if 'validation' in config:
                if not dynamic_object(config['validation'])(local_file, headers):
                    raise CNODCError("Validation failed", "SUBMIT", 1010)
            target_tier = StorageTier(config['upload_tier']) if 'upload_tier' in config else StorageTier.FREQUENT
            archival_tier = StorageTier(config['archive_tier']) if 'archive_tier' in config else StorageTier.ARCHIVAL
            metadata = _substitute_metadata(config['metadata'], headers) if 'metadata' in config else {}
            allow_overwrite = headers['allow-overwrite'] == '1' if 'allow-overwrite' in headers else False
            if 'allow_overwrite' in config:
                if config['allow_overwrite'] == 'always':
                    allow_overwrite = True
                elif config['allow_overwrite'] == 'never':
                    allow_overwrite = False
            if 'upload' in config:
                upload_dir = fc.get_handle(config['upload'])
                upload_handle = upload_dir.child(filename)
                upload_handle.upload(
                    local_file,
                    allow_overwrite,
                    storage_tier=target_tier,
                    metadata=metadata
                )
            if 'archive' in config:
                archive_dir = fc.get_handle(config['archive'])
                archive_handle = archive_dir.child(filename)
                archive_handle.upload(
                    local_file,
                    allow_overwrite,
                    storage_tier=archival_tier,
                    metadata=metadata
                )
            if 'queue' in config:
                db.create_queue_item(
                    config['queue'],
                    {
                        'upload_file': upload_handle.path() if upload_handle else None,
                        'archive_file': archive_handle.path() if archive_handle else None,
                        'filename': filename,
                        'headers': headers,
                        'workflow_name': workflow_name,
                    },
                    priority=config['queue_priority'] if 'queue_priority' in config else None,
                    unique_item_key=headers['workflow_name'],
                )
                db.commit()
        except Exception as ex:
            if upload_handle:
                upload_handle.remove()
            if archive_handle:
                archive_handle.remove()
            raise ex


def _sanitize_filename(filename: str) -> t.Optional[str]:

    # Guaranteed widely compatible characters
    clean_filename = ''.join([x for x in filename if x in VALID_FILENAME_CHARACTERS])

    # Windows trailing dot check
    while clean_filename.endswith("."):
        clean_filename = clean_filename[:-1]

    if len(filename) > 255:
        return None

    # Windows reserved filename check (if found, just use default)
    check = clean_filename if "." not in clean_filename else clean_filename[:clean_filename.find(".")]
    if check in RESERVED_FILENAMES:
        return None

    # All dot check
    if all(x == '.' for x in clean_filename):
        return None

    return clean_filename


def _substitute_metadata(metadata, headers):
    return {
        x: _substitute_headers(metadata[x], headers) for x in metadata
    }


def _substitute_headers(metadata_value, headers):
    for h in headers:
        metadata_value = metadata_value.replace("${" + h + "}", headers[h])
    metadata_value = metadata_value.replace('%{now}', datetime.datetime.utcnow().isoformat())
    return quote(metadata_value, safe=VALID_METADATA_CHARACTERS)


def _cleanup_request(request_dir: pathlib.Path):
    for file in os.scandir(request_dir):
        pathlib.Path(file.path).unlink(True)
    request_dir.rmdir()


def _cleanup_files(files: t.Iterable[pathlib.Path]):
    for file in files:
        file.unlink(missing_ok=True)


def _assemble_files(request_dir: pathlib.Path, part_files: list[pathlib.Path]) -> pathlib.Path:
    if len(part_files) == 1:
        return part_files[0]
    assembled_file = request_dir / "assembled.bin"
    with open(assembled_file, "wb") as fdest:
        for part_file in part_files:
            with open(part_file, "rb") as fsrc:
                shutil.copyfileobj(fsrc, fdest)
    return assembled_file
