import hashlib
import typing as t
import pathlib
import requests
import datetime
from cnodc.exc import CNODCError
from cnodc.util import Readable, HaltFlag


class NODBUploader:

    def __init__(self, nodb_base_url: str):
        self._base_url = nodb_base_url.rstrip('/')
        self._token = None
        self._expiry = None
        self._renew_at = 60 * 5

    def login(self, username, password):
        with requests.session() as sess:
            result = self._json_request(sess, 'post', 'login', {
                'username': username,
                'password': password
            })
            self._token = result['token']
            self._expiry = datetime.datetime.fromisoformat(result['expiry'])

    def _check_renewal(self, sess):
        if self._expiry is None or (datetime.datetime.utcnow() - self._expiry).total_seconds() <= self._renew_at:
            result = self._json_request(sess, 'get', 'renew')
            self._token = result['token']
            self._expiry = datetime.datetime.fromisoformat(result['expiry'])

    def _json_request(self, sess: requests.Session, method: str, endpoint: str, data: dict = None, headers: dict = None):
        url = f"{self._base_url}/{endpoint}"
        if headers is None:
            headers = {}
        if self._token is not None:
            headers['Authorization'] = f"Bearer {self._token}"
        response = sess.request(method, url, data, headers)
        if response.status_code != 200:
            raise CNODCError(f"HTTP error [{response.status_code}] requesting [{url}]", "UPLOAD", 1000)
        results = response.json()
        if 'error' in results:
            raise CNODCError(results['error'], 'UPLOAD', 1001)
        return results

    def upload_file(self,
                    workflow_name: str,
                    file: t.Union[str, pathlib.Path, Readable],
                    filename: t.Optional[str] = None,
                    allow_overwrite: t.Optional[bool] = None,
                    halt_flag: HaltFlag = None):
        with requests.session() as sess:
            self._check_renewal(sess)
            result = self._json_request(sess, 'get', f'submit/{workflow_name}')
            chunk_size = int(result['max_chunk_size'])
            send_url = f"{self._base_url}/submit/{workflow_name}"
            cancel_url = None
            headers = {
                'X-CNODC-More-Data': '1',
            }
            if allow_overwrite is True:
                headers['X-CNODC-Allow-Overwrite'] = '1'
            elif allow_overwrite is False:
                headers['X-CNODC-Allow-Overwrite'] = '0'
            if filename is not None:
                headers['X-CNODC-Filename'] = filename
            try:
                if halt_flag: halt_flag.check_continue(True)
                old_chunk = None
                for chunk in self._read_in_chunks(file, chunk_size):
                    if old_chunk is not None:
                        if halt_flag: halt_flag.check_continue(True)
                        results = self._send_chunk(sess, workflow_name, send_url, old_chunk, headers)
                        headers['X-CNODC-Token'] = results['x-cnodc-token']
                        send_url = results['more_data_endpoint']
                        cancel_url = results['cancel_endpoint']
                    old_chunk = chunk
                if halt_flag: halt_flag.check_continue(True)
                if old_chunk is not None:
                    del headers['X-CNODC-More-Data']
                    self._send_chunk(sess, workflow_name, send_url, old_chunk, headers)
                else:
                    raise CNODCError(f"File is empty", "UPLOAD", 1004)
            except Exception as ex:
                if cancel_url is not None:
                    sess.post(cancel_url, headers=headers)
                raise ex

    def _send_chunk(self, sess, workflow_name, send_url, chunk, headers):
        headers['X-CNODC-Upload-MD5'] = hashlib.md5(chunk).hexdigest()
        self._check_renewal(sess)
        headers['Authorization'] = f'Bearer {self._token}'
        response = sess.post(send_url, data=chunk, headers=headers)
        if response.status_code != 200:
            raise CNODCError(f"HTTP error [{response.status_code}] sending data to [{workflow_name}]", "UPLOAD", 1002)
        results = response.json()
        if 'error' in results:
            raise CNODCError(results['error'], 'UPLOAD', 1003)
        return results

    def _read_in_chunks(self, file: t.Union[str, pathlib.Path, Readable], chunk_size: int) -> t.Iterable[bytes]:
        if isinstance(file, (str, pathlib.Path)):
            with open(file, "rb") as h:
                yield from self._read_readable_in_chunks(h, chunk_size)
        else:
            yield from self._read_readable_in_chunks(file, chunk_size)

    def _read_readable_in_chunks(self, file: Readable, chunk_size: int) -> t.Iterable[bytes]:
        chunk = file.read(chunk_size)
        while chunk != b'' and chunk is not None:
            yield chunk
            chunk = file.read(chunk_size)
