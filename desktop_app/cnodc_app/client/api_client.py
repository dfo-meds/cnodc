import datetime
import json
import typing as t
from autoinject import injector
from cnodc_app.util import TranslatableException
import zirconium as zr
import requests


class RemoteAPIError(TranslatableException):

    def __init__(self, message: str, code: str = None):
        super().__init__('remote_api_error', message=message, code=code or '')


@injector.injectable
class _CNODCAPIClient:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._token = None
        self._expiry = None
        self._app_url = self.config.as_str(('cnodc_api', 'app_url'), default='http://localhost:5000').rstrip('/ ')

    def make_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        full_url = f"{self._app_url}/{endpoint}"
        headers = {}
        if self._token is not None:
            headers['Authorization'] = f'bearer {self._token}'
        response = requests.request(method, full_url, json=kwargs, headers=headers)
        response.raise_for_status()
        json_body = response.json()
        if 'error' in json_body:
            raise RemoteAPIError(json_body['error'], json_body['code'] if 'code' in json_body else None)
        return json_body

    def login(self, username: str, password: str) -> str:
        response = self.make_request('login', 'POST', username=username, password=password)
        self._token = response['token']
        self._expiry = datetime.datetime.fromisoformat(response['expiry'])
        return response['username']

    def refresh(self) -> bool:
        if self._token is not None:
            # TODO: check expiry time to determine if renewal is necessary
            response = self.make_request('renew', 'POST')
            self._token = response['token']
            self._expiry = datetime.datetime.fromisoformat(response['expiry'])
            return True


@injector.inject
def login(username: str, password: str, client: _CNODCAPIClient = None) -> str:
    return client.login(username, password)


@injector.inject
def refresh(client: _CNODCAPIClient = None) -> bool:
    return client.refresh()
