import datetime
import hashlib
import pathlib
from urllib.parse import quote_plus
import requests
import typing as t


class RequestError(Exception):

    def __init__(self, message, code = None):
        super().__init__(message)
        self.code = code


class CNODCClient:

    def __init__(self, base: str, renew_time: int = 300):
        self._base = base.rstrip("/")
        self._token = None
        self._expiry = None
        self._renew_time = renew_time
        self._chunk_sizes = {}

    def login(self, username: str, password: str):
        login_resp = self._make_request("POST", "login", json={
            "username": username,
            "password": password
        })
        self._token = login_resp['token']
        self._expiry = datetime.datetime.fromisoformat(login_resp['expiry'])

    def check_renew(self):
        if self._token is not None and (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._renew_time)) > self._expiry:
            self.renew()

    def renew(self):
        renew_resp = self._make_request("POST", "renew", json={})
        self._token = renew_resp['token']
        self._expiry = datetime.datetime.fromisoformat(renew_resp['expiry'])

    def upload_file(self, workflow_name: str, file_path: t.Union[str, pathlib.Path], filename: str = None, headers: dict = None):
        if '/' in workflow_name or '.' in workflow_name or '\\' in workflow_name:
            raise RequestError(f"Invalid workflow name")
        if self._token is None:
            raise RequestError(f"Login required")
        workflow_name = quote_plus(workflow_name)
        if workflow_name not in self._chunk_sizes:
            self._chunk_sizes[workflow_name] = self._get_workflow_chunk_size(workflow_name)
        with open(file_path, "rb") as h:
            chunk_current = h.read(self._chunk_sizes[workflow_name])
            chunk_next = h.read(self._chunk_sizes[workflow_name])
            send_link = f"submit/{workflow_name}"
            headers = headers or {}
            if filename is not None:
                headers['X-CNODC-Filename'] = filename
            send_md5 = not self._base.startswith("https://")
            while chunk_current != b'':
                self.check_renew()
                headers['X-CNODC-More-Data'] = '1' if chunk_next != b'' else '0'
                if send_md5:
                    headers['X-CNODC-Upload-MD5'] = hashlib.md5(chunk_current).hexdigest().lower()
                elif 'X-CNODC-Upload-MD5' in headers:
                    del headers['X-CNODC-Upload-MD5']
                print(headers)
                resp = self._make_request("POST", send_link, data=chunk_current, headers=headers)
                if chunk_next != b'':
                    if 'headers' not in resp or 'next_uri' not in resp:
                        raise RequestError("Upstream did not provide a next link")
                    headers.update(resp['headers'])
                    send_link = resp['next_uri']
                    chunk_current = chunk_next
                    chunk_next = h.read(self._chunk_sizes[workflow_name])
                else:
                    break

    def _get_workflow_chunk_size(self, workflow_name: str) -> int:
        self.check_renew()
        resp = self._make_request("GET", f"submit/{workflow_name}")
        return int(resp['max_chunk_size'])

    def _make_request(self, method: str, endpoint: str, headers: dict = None, **kwargs):
        headers = headers or {}
        if self._token is not None:
            headers['Authorization'] = f"Bearer {self._token}"
        full_url = endpoint if "://" in endpoint else f"{self._base}/{endpoint}"
        resp = requests.request(method, full_url, headers=headers, **kwargs)
        resp.raise_for_status()
        data = resp.json()
        if 'error' in data and data['error']:
            raise RequestError(data['error'], data['code'] if 'code' in data else None)
        return resp.json()

