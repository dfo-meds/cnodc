import datetime
import functools
import json
import typing as t
from autoinject import injector

from cnodc_app.client.local_db import LocalDatabase, CursorWrapper
from cnodc_app.util import TranslatableException, clean_for_json
import zirconium as zr
import requests


class RemoteAPIError(TranslatableException):

    def __init__(self, message: str, code: str = None):
        super().__init__('remote_api_error', message=message, code=code or '')


@injector.injectable
class _CNODCAPIClient:

    config: zr.ApplicationConfig = None
    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self):
        self._token = None
        self._expiry = None
        self._app_url = self.config.as_str(('cnodc_api', 'app_url'), default='http://localhost:5000').rstrip('/ ')
        self._access_list = None
        self._check_time = 300  # Renew when five minutes left on session

    def make_raw_json_request(self, endpoint: str, method: str, **kwargs: str) -> requests.Response:
        full_url = f"{self._app_url}/{endpoint}"
        headers = {}
        if self._token is not None:
            headers['Authorization'] = f'bearer {self._token}'
        if kwargs:
            response = requests.request(method, full_url, json=clean_for_json(kwargs), headers=headers)
        else:
            response = requests.request(method, full_url, json={}, headers=headers)
        response.raise_for_status()
        return response

    def make_json_request(self, *args, **kwargs) -> dict:
        response = self.make_raw_json_request(*args, **kwargs)
        json_body = response.json()
        if 'error' in json_body:
            raise RemoteAPIError(json_body['error'], json_body['code'] if 'code' in json_body else None)
        return json_body

    def login(self, username: str, password: str) -> tuple[str, list[str]]:
        response = self.make_json_request(endpoint='login', method='POST', username=username, password=password)
        self._token = response['token']
        self._expiry = datetime.datetime.fromisoformat(response['expiry'])
        self._access_list = response['access']
        return response['username'], self._access_list

    def refresh(self) -> bool:
        if self._token is not None:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            time_left = (self._expiry - now).total_seconds()
            if time_left < 0:
                self._token = None
                self._expiry = None
                self._access_list = None
                return False
            elif time_left < self._check_time:
                response = self.make_json_request('renew', 'POST')
                self._token = response['token']
                self._expiry = datetime.datetime.fromisoformat(response['expiry'])
                return True
            else:
                return True
        else:
            return False

    def _check_access(self, access_key_name: str):
        if self._access_list is None or access_key_name not in self._access_list:
            raise RemoteAPIError('access denied')

    def reload_stations(self) -> bool:
        self._check_access('queue:station-failure')
        response = self.make_raw_json_request('stations', 'GET')
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.truncate_table('stations')
            for station_def in self._iter_json_dicts(response):
                cur.insert('stations', station_def)
            cur.commit()
        return True

    def create_station(self, station_def: dict) -> bool:
        self._check_access('queue:station-failure')
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.insert('stations', station_def)
            self.make_json_request(
                endpoint='stations/new',
                method='POST',
                station=station_def
            )
            cur.commit()
            return True

    def _iter_json_dicts(self, response: requests.Response):
        buffer = ''
        check_idx = 1
        depth = 1
        for chunk in response.iter_content(10240, True):
            buffer += chunk
            if not buffer[0] == '{':
                raise ValueError('invalid stream')
            while True:
                next_end = buffer.find('}', check_idx)
                next_start = buffer.find('{', check_idx)
                if next_end == -1 and next_start == -1:
                    break
                elif next_end == -1 or next_start < next_end:
                    depth += 1
                    check_idx = next_start + 1
                elif depth > 1:
                    depth -= 1
                    check_idx = next_end + 1
                else:
                    yield json.loads(buffer[0:next_end+1])
                    buffer = buffer[next_end+1:]
                    check_idx = 1
                    depth = 1


@injector.inject
def login(username: str, password: str, client: _CNODCAPIClient = None) -> tuple[str, list[str]]:
    return client.login(username, password)


@injector.inject
def refresh(client: _CNODCAPIClient = None) -> bool:
    return client.refresh()


@injector.inject
def reload_stations(client: _CNODCAPIClient = None) -> bool:
    return client.reload_stations()


@injector.inject
def create_station(station_def: dict, client: _CNODCAPIClient = None) -> bool:
    return client.create_station(station_def)


