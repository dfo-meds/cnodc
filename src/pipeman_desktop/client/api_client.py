import datetime
import json
import typing as t

import zrlog
from autoinject import injector
from requests import JSONDecodeError, HTTPError, RequestException

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2.codecs import OCProc2BinCodec
from medsutil.byteseq import ByteSequenceReader
from medsutil.web import request
from pipeman_desktop.client.local_db import LocalDatabase, CursorWrapper
from pipeman_desktop.gui.messenger import CrossThreadMessenger
from pipeman_desktop.util import TranslatableException
import zirconium as zr
import requests
import medsutil.ocproc2 as ocproc2


class RemoteAPIError(TranslatableException):

    def __init__(self, message: str, code: str = None):
        super().__init__('remote_api_error', message=message, code=code or '')


def with_remote_api_error_handling(cb: t.Callable) -> t.Callable:
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except JSONDecodeError as ex:
            raise RemoteAPIError("Invalid JSON", None) from ex
        except HTTPError as ex:
            raise RemoteAPIError(f"{ex.errno}: {ex}", None) from ex
    return _inner



@injector.injectable
class WebAPIClient:

    config: zr.ApplicationConfig = None
    messenger: CrossThreadMessenger = None

    @injector.construct
    def __init__(self):
        import socket
        self.token = None
        self._app_id = socket.gethostname()
        self._app_url = self.config.as_str(('medweb_api', 'app_url'), default='http://localhost:5000').rstrip('/ ')
        self._log = zrlog.get_logger('pipeman.desktop.web_client')
        self._session = requests.Session()

    @property
    def is_logged_in(self) -> bool:
        return self.token is not None

    def _make_raw_request(self, endpoint: str, method: str, **kwargs: str) -> requests.Response:
        full_url = f"{self._app_url.rstrip('/')}/{endpoint.lstrip('/')}" if not endpoint.startswith('http') else endpoint
        self._log.trace(f"Web request: {method} {full_url}")
        headers = {}
        if self.token is not None:
            headers['Authorization'] = f'Bearer {self.token}'
        return request(method, full_url, session=self._session, json=kwargs, headers=headers, check_for_response_error=False)

    @with_remote_api_error_handling
    def make_json_request(self, *args, **kwargs) -> dict:
        kwargs["app_id"] = self._app_id
        response = self._make_raw_request(*args, **kwargs)
        if not response.headers.get('Content-Type', '').startswith('application/json'):
            response.raise_for_status()
        json_body = response.json()
        if 'error' in json_body:
            raise RemoteAPIError(json_body['error'], json_body['code'] if 'code' in json_body else None)
        return json_body

    # deprecated?
    def make_working_records_request(self, *args, **kwargs) -> t.Iterable[tuple[str, str, ocproc2.ParentRecord, list[dict]]]:
        response = self._make_raw_request(*args, **kwargs)
        response.raise_for_status()
        codec = OCProc2BinCodec()
        stream = ByteSequenceReader(response.iter_content(10240, False))
        while not stream.at_eof():
            record_id = stream.consume(stream.consume_vlq_int()).decode('ascii')
            record_hash = stream.consume(stream.consume_vlq_int()).decode('ascii')
            record_content = stream.consume(stream.consume_vlq_int())
            action_content = stream.consume(stream.consume_vlq_int())
            actions = []
            if action_content != b'':
                actions = json.loads(action_content.decode('utf-8'))
            yield record_id, record_hash, next(codec.decode_messages([record_content])), actions

    # deprecated?
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



@injector.injectable
class CNODCServerAPI:

    local_db: LocalDatabase = None
    messenger: CrossThreadMessenger = None
    web_client: WebAPIClient = None

    @injector.construct
    def __init__(self):
        self._expiry: AwareDateTime | None = None
        self._service_list: dict[str, dict] | None = None
        self._check_time: int = 300  # Renew when five minutes left on session
        self._current_queue_item = None
        self._username: str | None = None
        self._display_name: str | None = None
        self._log = zrlog.get_logger('cnodc.desktop.api')

    @property
    def username(self) -> str | None:
        return self._username

    @property
    def display_name(self) -> str | None:
        return self._display_name or self._username

    @property
    def services(self) -> list[str]:
        return list(self._service_list.keys()) if self._service_list else []

    def make_service_json_request(self,
                                  service_identifier: str,
                                  method: str,
                                  **kwargs):
        endpoint, extra_kwargs = self.service_info(service_identifier)
        return self.web_client.make_json_request(
            endpoint=endpoint,
            method=method,
            **kwargs,
            **extra_kwargs
        )

    def service_info(self, service_identifier: str) -> tuple[str, dict[str, t.Any]]:
        if self._service_list is not None and service_identifier in self._service_list:
            return (
                self._service_list[service_identifier]["url"],
                self._service_list[service_identifier]["kwargs"]
            )
        else:
            raise RemoteAPIError(f"No access to the service {service_identifier}")

    def has_access(self, service_identifier: str):
        _ = self.service_info(service_identifier)

    def login(self, username: str, password: str) -> bool:
        response = self.web_client.make_json_request(
            endpoint='api/create-access-token',
            method='POST',
            username=username,
            password=password
        )
        self.web_client.token = response['token']
        self._expiry = AwareDateTime.fromisoformat(response['expiry'])
        self._service_list = response['access']
        self._username = response['username']
        self._display_name = response['display']
        self._log.info(f'User {self._username} logged in')
        return True

    def logout(self) -> bool:
        if self.web_client.is_logged_in:
            self.make_service_json_request(
                service_identifier='user.logout',
                method='POST',
                token=self.web_client.token
            )
            self._clear_user_info()
            self._log.info(f'User logged out')
        return True

    def refresh(self) -> int:
        if self.web_client.is_logged_in and self._expiry is not None:
            now = AwareDateTime.now()
            time_left = int((self._expiry - now).total_seconds())
            if time_left < 0:
                self._clear_user_info()
                self._log.info('User session expired')
                return -1
            elif time_left < self._check_time:
                self._log.debug('Renewing session')
                response = self.make_service_json_request(
                    service_identifier='user.renew',
                    method='POST',
                    token=self.web_client.token
                )
                self.web_client.token = response['token']
                self._expiry = expiry = AwareDateTime.fromisoformat(response['expiry'])
                now = AwareDateTime.now()
                return int((expiry - now).total_seconds()) - self._check_time
            else:
                return time_left - self._check_time
        else:
            return -1

    def _clear_user_info(self):
        self._username = None
        self._display_name = None
        self.web_client.token = None
        self._service_list = None
        self._expiry = None
"""
    def reload_stations(self) -> bool:
        self._check_access('queue:station-failure')
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.truncate_table('stations')
            for station_def in self._client.make_json_dict_list_request(
                    endpoint=self._api_endpoint('other:list_stations'),
                    method='GET'
            ):
                cur.insert('stations', station_def)
            cur.commit()
        return True

    def create_station(self, station_def: dict) -> bool:
        self._check_access('queue:station-failure')
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            cur.insert('stations', station_def)
            self._client.make_json_request(
                endpoint=self._api_endpoint('other:create_station'),
                method='POST',
                station=station_def
            )
            cur.commit()
            return True

    def _api_endpoint(self, item_name: str):
        item_names = item_name.split(':')
        d = self._service_list
        for x in item_names:
            d = d[x]
        return d['url'] if isinstance(d, dict) else d

    def load_next_queue_item(self, service_name: str) -> t.Optional[tuple[list[str], list[str]]]:
        with self.local_db.cursor() as cur:
            cur.begin_transaction()
            response = self._client.make_json_request(self._api_endpoint(service_name), 'POST')
            if response['item_uuid'] is None:
                return None
            else:
                self._current_queue_item = response
                self._load_working_records(cur, self._current_queue_item['batch_size'])
            cur.commit()
        return list(self._current_queue_item['actions'].keys()), self._current_queue_item['current_tests']

    def _make_item_request(self, action_name: str, **kwargs):
        if action_name not in self._current_queue_item['actions']:
            raise RemoteAPIError('Insufficient permissions for the operation')
        return self._client.make_json_request(
            self._current_queue_item['actions'][action_name],
            'POST',
            app_id=self._current_queue_item['app_id'],
            **kwargs
        )

    def escalate_item(self) -> bool:
        if self._current_queue_item is None:
            return False
        self._make_item_request('escalate')
        self._current_queue_item = None
        return True

    def descalate_item(self) -> bool:
        if self._current_queue_item is None:
            return False
        self._make_item_request('descalate')
        self._current_queue_item = None
        return True

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
            cur.execute('SELECT a.record_uuid, a.action_text, r.record_hash FROM actions a JOIN records r ON r.record_uuid = a.record_uuid AND is_saved = 0')
            for record_id, action_text, record_hash in cur.fetchall():
                if record_id not in actions:
                    actions[record_id] = {
                        'hash': record_hash,
                        'actions': []
                    }
                actions[record_id]['actions'].append(
                    json.loads(action_text)
                )
            response = self._make_item_request('apply_working', operations=actions)
            successful_saves = [wrid for wrid in response if response[wrid][0]]
            cur.execute("UPDATE actions SET is_saved = 1 WHERE record_uuid IN (" + (','.join('?' for _ in successful_saves)) + ")", successful_saves)
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

    def _load_working_records(self, cur: CursorWrapper, rough_count: int):
        if 'actions' not in self._current_queue_item or 'download_working' not in self._current_queue_item['actions']:
            raise ValueError('Missing response information')
        cur.truncate_table('records')
        cur.truncate_table('actions')
        for working_uuid, record_hash, record, actions in self._client.make_working_records_request(
                endpoint=self._current_queue_item['actions']['download_working'],
                method='GET',
                app_id=self._current_queue_item['app_id']
        ):
            lat = None
            lon = None
            ts = None
            lat_qc = None
            lon_qc = None
            ts_qc = None
            station_id = None
            if record.metadata.has_value('CNODCStation'):
                station_id = record.metadata.best('CNODCStation')
            elif record.metadata.has_value('CNODCStationString'):
                station_id = record.metadata.best('CNODCStationString')
            if record.coordinates.has_value('Latitude') and record.coordinates.has_value('Longitude'):
                try:
                    lat = record.coordinates['Latitude'].to_float()
                    lon = record.coordinates['Longitude'].to_float()
                    lat_qc = int(record.coordinates['Latitude'].metadata.best('WorkingQuality', 0))
                    lon_qc = int(record.coordinates['Longitude'].metadata.best('WorkingQuality', 0))
                except (ValueError, TypeError):
                    pass
            if record.coordinates.has_value('Time'):
                try:
                    ts = record.coordinates['Time'].to_datetime().isoformat()
                    ts_qc = int(record.coordinates['Time'].metadata.best('WorkingQuality', 0))
                except (ValueError, TypeError):
                    pass
            cur.insert('records', {
                'record_uuid': working_uuid,
                'display': self._build_display(record, working_uuid),
                'record_hash': record_hash,
                'station_id': station_id,
                'lat': lat,
                'lon': lon,
                'lat_qc': lat_qc,
                'lon_qc': lon_qc,
                'datetime': ts,
                'datetime_qc': ts_qc,
                'record_content': json.dumps(record.to_mapping()),
                'has_errors': 1 if record.qc_tests[-1].result == ocproc2.QCResult.MANUAL_REVIEW else 0
            })
            for action in actions:
                cur.insert('actions', {
                    'record_uuid': working_uuid,
                    'action_text': json.dumps(action)
                })
        cur.commit()

    def _build_display(self, record: ocproc2.ParentRecord, working_id: str):
        s = []
        if record.coordinates.has_value('Time'):
            s.append(f'T:{record.coordinates.best("Time")}')
        if record.coordinates.has_value('Latitude') and record.coordinates.has_value('Longitude'):
            s.append(f'X:{record.coordinates.best("Longitude")}')
            s.append(f'Y:{record.coordinates.best("Latitude")}')
        if record.coordinates.has_value('Depth'):
            s.append(f'Z:{record.coordinates.best("Depth")}')
        elif record.coordinates.has_value('Pressure'):
            s.append(f'P:{record.coordinates.best("Pressure")}')
        if not s:
            s.append(f"I:{working_id}")
        return '  '.join(s)
"""

@injector.inject
def login(username: str, password: str, client: CNODCServerAPI = None) -> tuple[str | None, list[str]]:
    client.login(username, password)
    return client.display_name, client.services


@injector.inject
def refresh(client: CNODCServerAPI = None) -> int:
    return client.refresh()


@injector.inject
def logout(client: CNODCServerAPI = None) -> bool:
    return client.logout()


"""



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
def next_queue_item(service_name: str, client: CNODCServerAPI = None) -> t.Optional[list[str]]:
    return client.load_next_queue_item(service_name)


@injector.inject
def escalate_item(client: CNODCServerAPI = None) -> bool:
    return client.escalate_item()


@injector.inject
def descalate_item(client: CNODCServerAPI = None) -> bool:
    return client.descalate_item()


@injector.inject
def save_work(client: CNODCServerAPI = None) -> bool:
    return client.save_work()


@injector.inject
def change_password(password: str, client: CNODCServerAPI = None) -> bool:
    return client.change_password(password)

"""
