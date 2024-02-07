import datetime
import json
import typing as t
from autoinject import injector

from cnodc.codecs import OCProc2BinCodec
from cnodc.codecs.base import ByteSequenceReader
from cnodc.desktop.client.local_db import LocalDatabase, CursorWrapper
from cnodc.desktop.client.test_client import TestClient
from cnodc.desktop.gui.messenger import CrossThreadMessenger
from cnodc.desktop.util import TranslatableException
from cnodc.util import clean_for_json, vlq_decode
import zirconium as zr
import requests
import cnodc.ocproc2.structures as ocproc2


class RemoteAPIError(TranslatableException):

    def __init__(self, message: str, code: str = None):
        super().__init__('remote_api_error', message=message, code=code or '')


class _WebAPIClient:

    config: zr.ApplicationConfig = None
    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self):
        self._token = None
        self._app_url = self.config.as_str(('cnodc_api', 'app_url'), default='http://localhost:5000').rstrip('/ ')

    def _make_raw_request(self, endpoint: str, method: str, **kwargs: str) -> requests.Response:
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
        response = self._make_raw_request(*args, **kwargs)
        json_body = response.json()
        if 'error' in json_body:
            raise RemoteAPIError(json_body['error'], json_body['code'] if 'code' in json_body else None)
        return json_body

    def make_working_records_request(self, *args, **kwargs) -> t.Iterable[tuple[str, str, ocproc2.DataRecord]]:
        response = self._make_raw_request(*args, **kwargs)
        codec = OCProc2BinCodec()
        stream = ByteSequenceReader(response.iter_content(10240, False))
        record_estimate = stream.consume_vlq_int()
        while not stream.at_eof():
            record_id = stream.consume(stream.consume_vlq_int()).decode('ascii')
            record_hash = stream.consume(stream.consume_vlq_int()).decode('ascii')
            record_content = stream.consume(stream.consume_vlq_int())
            yield record_id, record_hash, codec.decode_messages([record_content])

    def make_json_dict_list_request(self, *args, **kwargs) -> t.Iterable[dict]:
        response = self._make_raw_request(*args, **kwargs)
        buffer = ''
        check_idx = 1
        depth = 1
        for chunk in response.iter_content(10240, True):
            buffer += chunk
            if buffer == '':
                break
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

    def set_token(self, token):
        self._token = token

    def is_logged_in(self) -> bool:
        return self._token is not None


@injector.injectable
class CNODCServerAPI:

    local_db: LocalDatabase = None
    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self, test_mode: bool = True):
        self._expiry = None
        self._access_list = None
        self._check_time = 300  # Renew when five minutes left on session
        self._test_mode = False
        self._current_queue_item = None
        self._username = None
        self._client = TestClient() if test_mode else _WebAPIClient()

    def login(self, username: str, password: str) -> tuple[str, list[str]]:
        response = self._client.make_json_request(
            endpoint='login',
            method='POST',
            username=username,
            password=password
        )
        self._client.set_token(response['token'])
        self._expiry = datetime.datetime.fromisoformat(response['expiry'])
        self._access_list = response['access']
        self._username = response['username']
        return self._username, list(x for x in self._access_list)

    def logout(self) -> bool:
        if self._client.is_logged_in():
            self._client.make_json_request(
                endpoint='logout',
                method='POST'
            )
        return True

    def refresh(self) -> bool:
        if self._client.is_logged_in():
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            time_left = (self._expiry - now).total_seconds()
            if time_left < 0:
                self._client.set_token(None)
                self._expiry = None
                self._access_list = None
                return False
            elif time_left < self._check_time:
                response = self._client.make_json_request('renew', 'POST')
                self._client.set_token(response['token'])
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
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.truncate_table('stations')
            for station_def in self._client.make_json_dict_list_request('stations', 'GET'):
                cur.insert('stations', station_def)
            cur.commit()
        return True

    def create_station(self, station_def: dict) -> bool:
        self._check_access('queue:station-failure')
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.insert('stations', station_def)
            self._client.make_json_request(
                endpoint='stations/new',
                method='POST',
                station=station_def
            )
            cur.commit()
            return True

    def load_next_station_failure(self) -> bool:
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            response = self._client.make_json_request('next/station-failure', 'POST')
            if response['item_uuid'] is None:
                return False
            else:
                self._current_queue_item = response
                self._load_working_records(cur)
            cur.commit()
        return True

    def _make_item_request(self, action_name: str, **kwargs):
        return self._client.make_json_request(
            self._current_queue_item['actions'][action_name],
            'POST',
            app_id=self._current_queue_item['app_id'],
            **kwargs
        )

    def release_lock(self) -> bool:
        if self._current_queue_item is None:
            return False
        self._make_item_request('release')
        self._current_queue_item = None
        return True

    def mark_item_failed(self) -> bool:
        if self._current_queue_item is None:
            return False
        self._make_item_request('fail')
        self._current_queue_item = None
        return True

    def complete_item(self) -> bool:
        if self._current_queue_item is None:
            return False
        self._make_item_request('complete')
        self._current_queue_item = None
        return True

    def save_work(self) -> bool:
        if self._current_queue_item is None:
            return False
        with self.local_db.cursor() as cur:
            actions = {}
            cur.execute('SELECT a.record_uuid, a.action_text, r.record_hash FROM actions a JOIN records r ON r.record_uuid = a.record_uuid')
            for record_id, action_text, record_hash in cur.fetchall():
                if record_id not in actions:
                    actions[record_id] = {
                        'hash': record_hash,
                        'actions': []
                    }
                actions[record_id]['actions'].append(
                    json.loads(action_text)
                )
            self._make_item_request('apply_working', operations=actions)
            cur.truncate_table('actions')
            cur.commit()
            return True

    def renew_lock(self) -> bool:
        if self._current_queue_item is None:
            return False
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
        cur.truncate_table('records')
        for working_uuid, record_hash, record in self._client.make_working_records_request(
                endpoint=self._current_queue_item['actions']['download_working'],
                method='GET',
                app_id=self._current_queue_item['app_id']
        ):
            cur.insert('records', {
                'record_uuid': working_uuid,
                'display': self._build_display(record, working_uuid),
                'record_hash': record_hash,
                'record_content': json.dumps(record.to_mapping()),
                'has_errors': 1 if record.qc_tests[-1].result == ocproc2.QCResult.MANUAL_REVIEW else 0
            })
        cur.commit()

    def _build_display(self, record: ocproc2.DataRecord, working_id: str):
        s = []
        if record.coordinates.has_value('Time'):
            s.append(f'T:{record.coordinates.best_value("Time")}')
        if record.coordinates.has_value('Latitude') and record.coordinates.has_value('Longitude'):
            s.append(f'X:{record.coordinates.best_value("Longitude")}')
            s.append(f'Y:{record.coordinates.best_value("Latitude")}')
        if record.coordinates.has_value('Depth'):
            s.append(f'Z:{record.coordinates.best_value("Depth")}')
        elif record.coordinates.has_value('Pressure'):
            s.append(f'P:{record.coordinates.best_value("Pressure")}')
        if not s:
            s.append(f"I:{working_id}")
        return '  '.join(s)



@injector.inject
def login(username: str, password: str, client: CNODCServerAPI = None) -> tuple[str, list[str]]:
    return client.login(username, password)


@injector.inject
def refresh(client: CNODCServerAPI = None) -> bool:
    return client.refresh()


@injector.inject
def renew_lock(client: CNODCServerAPI = None) -> bool:
    return client.renew_lock()


@injector.inject
def complete_item(client: CNODCServerAPI = None) -> bool:
    return client.complete_item()


@injector.inject
def release_item(client: CNODCServerAPI = None) -> bool:
    return client.release_lock()


@injector.inject
def fail_item(client: CNODCServerAPI = None) -> bool:
    return client.mark_item_failed()


@injector.inject
def reload_stations(client: CNODCServerAPI = None) -> bool:
    return client.reload_stations()


@injector.inject
def create_station(station_def: dict, client: CNODCServerAPI = None) -> bool:
    return client.create_station(station_def)


@injector.inject
def next_station_failure(client: CNODCServerAPI = None) -> bool:
    return client.load_next_station_failure()


@injector.inject
def save_work(client: CNODCServerAPI = None) -> bool:
    return client.save_work()


@injector.inject
def logout(client: CNODCServerAPI = None) -> bool:
    return client.logout()

