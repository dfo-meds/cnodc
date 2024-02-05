import datetime
import functools
import json
import typing as t
from autoinject import injector

from cnodc.codecs import OCProc2BinCodec
from cnodc_app.client.local_db import LocalDatabase, CursorWrapper
from cnodc_app.gui.messenger import CrossThreadMessenger
from cnodc_app.util import TranslatableException, clean_for_json, vlq_decode
import zirconium as zr
import requests
import cnodc.ocproc2.structures as ocproc2

ALLOW_TEST_USER = True
ALL_PERMISSIONS = [
    'queue:station-failure'
]


class RemoteAPIError(TranslatableException):

    def __init__(self, message: str, code: str = None):
        super().__init__('remote_api_error', message=message, code=code or '')



@injector.injectable
class _CNODCAPIClient:

    config: zr.ApplicationConfig = None
    local_db: LocalDatabase = None
    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self):
        self._token = None
        self._expiry = None
        self._app_url = self.config.as_str(('cnodc_api', 'app_url'), default='http://localhost:5000').rstrip('/ ')
        self._access_list = None
        self._check_time = 300  # Renew when five minutes left on session
        self._test_mode = False
        self._current_queue_item = None

    def make_raw_json_request(self, endpoint: str, method: str, **kwargs: str) -> requests.Response:
        self.messenger.send_translatable('foobar')
        full_url = f"{self._app_url}/{endpoint}" if not endpoint.startswith('http') else endpoint
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
        if ALLOW_TEST_USER and username == 'test' and password == 'test':
            self._test_mode = True
            return 'test', ALL_PERMISSIONS
        response = self.make_json_request(endpoint='login', method='POST', username=username, password=password)
        self._token = response['token']
        self._expiry = datetime.datetime.fromisoformat(response['expiry'])
        self._access_list = response['access']
        return response['username'], list(x for x in self._access_list)

    def refresh(self) -> bool:
        if self._test_mode:
            return True
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
        if self._test_mode:
            return True
        if self._access_list is None or access_key_name not in self._access_list:
            raise RemoteAPIError('access denied')

    def reload_stations(self) -> bool:
        self._check_access('queue:station-failure')
        if self._test_mode:
            return True
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
            if not self._test_mode:
                self.make_json_request(
                    endpoint='stations/new',
                    method='POST',
                    station=station_def
                )
            cur.commit()
            return True

    def load_next_station_failure(self) -> bool:
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            if not self._test_mode:
                response = self.make_json_request('/next/station-failure')
                if response['item_uuid'] is None:
                    return False
                else:
                    self._current_queue_item = response
                    self._load_working_records(cur)
            else:
                cur.truncate_table('records')
            cur.commit()
        return True

    def _make_item_request(self, action_name: str):
        return self.make_json_request(
            self._current_queue_item['actions'][action_name],
            app_id=self._current_queue_item['app_id']
        )

    def release_lock(self) -> bool:
        if self._current_queue_item is None:
            return False
        if not self._test_mode:
            self._make_item_request('release')
        self._current_queue_item = None
        return True

    def mark_item_failed(self) -> bool:
        if self._current_queue_item is None:
            return False
        if not self._test_mode:
            self._make_item_request('fail')
        self._current_queue_item = None
        return True

    def complete_item(self) -> bool:
        if self._current_queue_item is None:
            return False
        if not self._test_mode:
            self._make_item_request('complete')
        self._current_queue_item = None
        return True

    def renew_lock(self) -> bool:
        if self._current_queue_item is None:
            return False
        if self._test_mode:
            return True
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        time_left = (datetime.datetime.fromisoformat(self._current_queue_item['lock_expiry']) - now).total_seconds()
        if time_left < 0:
            self._current_queue_item = None
            return False
        elif time_left < self._check_time:
            resp = self._make_item_request('renew')
            self._current_queue_item.update(resp)
            return True

    def _load_working_records(self, cur: CursorWrapper):
        if 'actions' not in self._current_queue_item or 'download_working' not in self._current_queue_item['actions']:
            raise ValueError('Missing response information')
        response = self.make_raw_json_request(self._current_queue_item['actions']['download_working'], 'GET')
        for working_uuid, record in self._iter_working_records(response):
            cur.insert('records', {
                'record_uuid': working_uuid,
                'record_content': json.dumps(record.to_mapping())
            })

    def _iter_working_records(self, response: requests.Response) -> t.Iterable[tuple[str, ocproc2.DataRecord]]:
        buffer = bytearray()
        record_estimate = None
        codec = OCProc2BinCodec()
        for chunk in response.iter_content(10240, False):
            buffer.extend(chunk)
            l_buffer = len(buffer)
            if record_estimate is None:
                idx = 0
                while buffer[idx] >= 128:
                    idx += 1
                    if idx == l_buffer:
                        break
                if idx == l_buffer:
                    continue
                record_estimate, _ = vlq_decode(buffer[0:idx+1])
                buffer = buffer[idx+1:]
                l_buffer = len(buffer)
            if record_estimate is not None:
                idx = 0
                while buffer[idx] >= 128:
                    idx += 1
                    if idx == l_buffer:
                        break
                if idx == l_buffer:
                    continue
                record_id_start = idx + 1
                id_length, _ = vlq_decode(buffer[0:record_id_start])
                record_id_end = idx + id_length + 1
                if record_id_end >= l_buffer:
                    continue
                idx2 = record_id_end
                while buffer[idx2] >= 128:
                    idx2 += 1
                    if idx2 == l_buffer:
                        break
                if idx2 == l_buffer:
                    continue
                record_content_start = idx2 + 1
                content_length, _ = vlq_decode(buffer[record_id_end:record_content_start])
                record_content_end = idx2 + content_length
                if record_content_end >= l_buffer:
                    continue
                yield buffer[record_id_start:record_id_end].decode('ascii'), [r for r in codec.decode_messages(buffer[record_content_start:record_content_end])]
                buffer = buffer[record_content_end:]

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
def renew_lock(client: _CNODCAPIClient = None) -> bool:
    return client.renew_lock()


@injector.inject
def complete_item(client: _CNODCAPIClient = None) -> bool:
    return client.complete_item()


@injector.inject
def release_item(client: _CNODCAPIClient = None) -> bool:
    return client.release_lock()


@injector.inject
def fail_item(client: _CNODCAPIClient = None) -> bool:
    return client.mark_item_failed()


@injector.inject
def reload_stations(client: _CNODCAPIClient = None) -> bool:
    return client.reload_stations()


@injector.inject
def create_station(station_def: dict, client: _CNODCAPIClient = None) -> bool:
    return client.create_station(station_def)


@injector.inject
def next_station_failure(client: _CNODCAPIClient = None) -> bool:
    return client.load_next_station_failure()

