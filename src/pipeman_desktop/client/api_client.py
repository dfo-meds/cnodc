import json
import typing as t

import zrlog
from autoinject import injector
from requests import JSONDecodeError, HTTPError

from gcapp.i18n.base import TranslatableError
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.ocproc2 import QCResult
from medsutil.ocproc2.codecs import OCProc2BinCodec
from medsutil.byteseq import ByteSequenceReader
from medsutil.web import request
from nodb.observations import PlatformStatus
from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.messenger import CrossThreadMessenger
import zirconium as zr
import requests
import medsutil.ocproc2 as ocproc2


class RemoteAPIError(CodedError):

    def __init__(self, message: str, remote_code: str | None = None, local_code: int | None = None):
        super().__init__(f"{remote_code or ''}: {message}", local_code, code_space="REMOTE")


class LocalAPIError(TranslatableError): CODE_SPACE="API-LOCAL"


def with_remote_api_error_handling(cb: t.Callable) -> t.Callable:
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except JSONDecodeError as ex:
            raise LocalAPIError("error.invalid_json", 1000) from ex
        except HTTPError as ex:
            raise RemoteAPIError(f"{ex.errno}: {ex}", "HTTP", 1000) from ex
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
                                  _service_list: dict[str, dict[str, t.Any]] | None = None,
                                  **kwargs):
        endpoint, extra_kwargs = self.service_info(
            service_list=_service_list if _service_list is not None else self._service_list,
            service_identifier=service_identifier
        )
        return self.web_client.make_json_request(
            endpoint=endpoint,
            method=method,
            **kwargs,
            **extra_kwargs
        )

    @staticmethod
    def service_info(service_list: dict[str, dict[str, t.Any]] | None, service_identifier: str) -> tuple[str, dict[str, t.Any]]:
        if service_list is not None and service_identifier in service_list:
            return (
                service_list[service_identifier]["endpoint"],
                service_list[service_identifier].get("kwargs", None) or {}
            )
        else:
            err = LocalAPIError("error.no_access", 1100)
            err.add_note(f"service: {service_identifier}")
            raise err

    def has_access(self, service_identifier: str):
        _ = self.service_info(self._service_list, service_identifier)

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

    def fetch_queue_ready_count(self) -> list[tuple[str, str | None, int, int]]:
        response = self.make_service_json_request(
            service_identifier="desktop.queue_items_ready",
            method="GET"
        )
        return response["ready"]

    def open_batch(self, batch_service_name: str) -> list[str] | None:
        response = self.make_service_json_request(
            service_identifier=f"batch_qc.{batch_service_name}.open",
            method="POST",
        )
        if "queue_uuid" in response and response["queue_uuid"]:
            self._current_queue_item = response
            self._load_batch()
            return self._service_list[f"batch_qc.{batch_service_name}.open"].get("metadata", {}).get("allowed_qc_results", [])

        else:
            self._current_queue_item = None
            return None

    def _load_batch(self):
        platform_load_list = set()
        with self.local_db.cursor() as cur:
            response = self.make_batch_json_request(
                action_name="stream",
                method="GET"
            )
            cur.truncate_table('records')
            cur.truncate_table('actions')
            for working_info in response["data"]:
                cur.insert('records', {
                    'record_uuid': working_info["working_uuid"],
                    'downloaded': 0,
                    'platform_id': working_info["platform_uuid"],
                    'actions': json.dumps(working_info["actions"]),
                })
                platform_load_list.add(working_info["platform_uuid"])
            cur.commit()
        with self.local_db.cursor() as cur:
            cur.execute("SELECT record_uuid, actions FROM records WHERE downloaded = 0")
            while row := cur.fetchone():
                actions = json.loads(row[1])
                response = self.make_service_json_request(
                    "fetch",
                    "GET",
                    _service_list=actions
                )
                record = ocproc2.ParentRecord.build_from_mapping(response["data"])
                if record.metadata.has_value("CNODCPlatformCandidates"):
                    platform_load_list.update(record.metadata["CNODCPlatformCandidates"].value)
                local_info, proposed_actions = self._build_local_record(record, row[0])
                is_saved = False
                if response["proposed_actions"] is not None:
                    proposed_actions = response["proposed_actions"]
                    is_saved = True
                with self.local_db.cursor() as cur2:
                    cur2.update("records", {
                        "record_content": json.dumps(record.to_mapping()),
                        **local_info
                    }, {
                        "record_uuid": row[0]
                    })
                    for action in proposed_actions:
                        cur2.insert("actions", {
                            "record_uuid": row[0],
                            "action_text": json.dumps(action.export()),
                            "is_saved": 1 if is_saved else 0,
                        })
        self.load_platforms(platform_load_list)

    def _build_local_record(self, record: ocproc2.ParentRecord, working_uuid: str) -> tuple[dict, list]:
        lat = record.coordinates.ideal("Latitude")
        lon = record.coordinates.ideal("Longitude")
        time = record.coordinates.ideal("Time")
        info = {
            "lat": lat.to_string() if lat else None,
            "lat_qc": lat.quality if lat else None,
            "lon": lon.to_string() if lon else None,
            "lon_qc": lon.quality if lon else None,
            "datetime": time.to_string() if time else None,
            "datetime_qc": time.quality if time else None,
            "has_errors": 0,
            "display": self._build_display(record, working_uuid),
        }
        default_actions = []
        for qcr in record.qc_tests.iterate_with_load():
            if qcr.result is QCResult.MANUAL_REVIEW:
                info["has_errors"] = 1
                default_actions.extend(qcr.proposed_actions)
        return info, default_actions

    def _build_display(self, record: ocproc2.ParentRecord, working_uuid: str):
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
            s.append(f"I:{working_uuid}")
        return '  '.join(s)

    def make_batch_json_request(self, action_name, method: str, **kwargs) -> dict:
        if not self._current_queue_item:
            err = LocalAPIError("error.no_open_batch", 1200)
            err.add_note(f"action: {action_name}")
            raise err
        return self.make_service_json_request(
            action_name,
            method,
            _service_list=self._current_queue_item.get("actions", None),
            **kwargs
        )

    def renew_batch(self) -> bool:
        response = self.make_batch_json_request(
            action_name="renew",
            method="POST"
        )
        return "success" in response and response["success"]

    def close_batch(self, close_operation: str):
        response = self.make_batch_json_request(
            action_name="close",
            method="POST",
            result=close_operation
        )
        self._current_queue_item = None
        return "success" in response and response["success"]

    def search_platforms(self,
                         wmo_id: str | None,
                         wigos_id: str | None,
                         platform_id: str | None,
                         platform_name: str | None,
                         time_frame: AwareDateTime | None) -> list[str]:
        response = self.make_service_json_request(
            service_identifier="desktop.search_platforms",
            method="POST",
            wmo_id=wmo_id or None,
            wigos_id=wigos_id or None,
            platform_id=platform_id or None,
            platform_name=platform_name or None,
            time_frame=time_frame.isoformat() if time_frame else None,
        )
        return self.load_platforms(
            x["platform_uuid"] for x in response["data"]
        )

    def create_platform(self,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: AwareDateTime | None,
                        end_date: AwareDateTime | None,
                        status: PlatformStatus,
                        embargo_data_days: int | None,
                        map_to_uuid: str | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | None) -> str | None:
        response = self.make_service_json_request(
            service_identifier="desktop.create_platform",
            method="POST",
            wmo_id=wmo_id or None,
            wigos_id=wigos_id or None,
            platform_name=platform_name or None,
            platform_id=platform_id or None,
            platform_type=platform_type or None,
            embargo_data_days=embargo_data_days or None,
            map_to_uuid=map_to_uuid or None,
            skip_speed_check=skip_speed_check,
            skip_land_check=skip_land_check,
            dedupe_time_window=dedupe_time_window,
            dedupe_distance_window=dedupe_distance_window,
            top_speed=top_speed or None,
            status=status.value,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
        if response["success"]:
            load_result = self.load_platforms([response["data"]["platform_uuid"]])
            if load_result:
                return load_result[0]
        return None

    def update_platform(self,
                        platform_uuid: str,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: AwareDateTime | None,
                        end_date: AwareDateTime | None,
                        status: PlatformStatus,
                        embargo_data_days: int | None,
                        map_to_uuid: str | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | None) -> bool:
        response = self.make_service_json_request(
            service_identifier="update",
            method="POST",
            _service_list=self._platform_services(platform_uuid),
            wmo_id=wmo_id or None,
            wigos_id=wigos_id or None,
            platform_name=platform_name or None,
            platform_id=platform_id or None,
            platform_type=platform_type or None,
            embargo_data_days=embargo_data_days or None,
            map_to_uuid=map_to_uuid or None,
            skip_speed_check=skip_speed_check,
            skip_land_check=skip_land_check,
            dedupe_time_window=dedupe_time_window,
            dedupe_distance_window=dedupe_distance_window,
            top_speed=top_speed or None,
            status=status.value,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
        if response["success"]:
            load_result = self.load_platforms([platform_uuid])
            return len(load_result) > 0
        return False

    def _platform_services(self, platform_uuid: str) -> dict:
        with self.local_db.cursor() as cur:
            cur.execute("SELECT actions FROM platforms WHERE platform_uuid = ?", (platform_uuid,))
            result = cur.fetchone()
            if not result:
                return {}
            return json.loads(result[0])

    def reload_platforms(self) -> list[str]:
        with self.local_db.cursor() as cur:
            cur.execute("SELECT platform_uuid FROM platforms")
            platforms = []
            for res in cur.fetchall():
                platforms.append(res[0])
        return self.load_platforms(platforms)

    def load_platforms(self, platform_uuids: t.Iterable[str]) -> list[str]:
        success = []
        for pid in platform_uuids:
            if self._load_platform(pid):
                success.append(pid)
        return success

    def _load_platform(self, platform_uuid: str) -> bool:
        response = self.make_service_json_request(
            service_identifier="desktop.find_platform",
            platform_uuid=platform_uuid,
            method="GET"
        )
        with self.local_db.cursor() as cur:
            cur.execute("DELETE FROM platforms WHERE platform_uuid = ?", (platform_uuid,))
            if response["success"]:
                cur.execute("INSERT INTO platforms (platform_uuid, wmo_id, wigos_id, platform_name, platform_id, platform_type, service_start_date, service_end_date, metadata, map_to_uuid, status, embargo_data_days, actions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    platform_uuid,
                    response["data"].get("wmo_id", None),
                    response["data"].get("wigos_id", None),
                    response["data"].get("platform_name", None),
                    response["data"].get("platform_id", None),
                    response["data"].get("platform_type", None),
                    response["data"].get("service_start_date", None),
                    response["data"].get("service_end_date", None),
                    json.dumps(response["data"].get("metadata", {})),
                    response["data"].get("map_to_uuid", None),
                    response["data"].get("status", None),
                    response["data"].get("embargo_data_days", None),
                    json.dumps(response["data"].get("actions", {})),
                ))
                cur.commit()
                return True
            else:
                cur.commit()
                return False

    def save_changes(self) -> bool:
        with self.local_db.cursor() as cur:
            cur.execute("SELECT DISTINCT record_uuid FROM actions WHERE is_saved = 0")
            for row in cur.fetchall():
                self._save_changes(row[0])
            return True

    def _save_changes(self, record_uuid: str):
        with self.local_db.cursor() as cur:
            cur.execute("SELECT actions FROM record WHERE record_uuid = ?", (record_uuid,))
            row = cur.fetchone()
            if row is None:
                raise LocalAPIError("error.no_record_entry", 1300)
            record_actions = row[0]
            action_list = []
            cur.execute("SELECT action_text FROM action WHERE record_uuid = ?", (record_uuid,))
            for row in cur.fetchall():
                action_list.append(row[0])
            response = self.make_service_json_request(
                service_identifier="save",
                method="POST",
                _service_list=record_actions,
                actions=action_list
            )
            if response["success"]:
                cur.execute("UPDATE actions SET is_saved = 1 WHERE record_uuid = ?", (record_uuid,))


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


@injector.inject
def fetch_queue_ready_count(client: CNODCServerAPI = None) -> list[tuple[str, str | None, int, int]]:
    return client.fetch_queue_ready_count()


@injector.inject
def save_changes(client: CNODCServerAPI = None) -> bool:
    return client.save_changes()


@injector.inject
def open_batch(batch_service_name: str, client: CNODCServerAPI = None) -> list[str] | None:
    return client.open_batch(batch_service_name)


@injector.inject
def close_batch(close_operation: str, client: CNODCServerAPI = None) -> bool:
    return client.close_batch(close_operation)


@injector.inject
def reload_platforms(client: CNODCServerAPI = None) -> list[str]:
    return client.reload_platforms()


@injector.inject
def load_platforms(platform_uuids: t.Iterable[str], client: CNODCServerAPI = None) -> list[str]:
    return client.load_platforms(platform_uuids)


@injector.inject
def create_platform(client: CNODCServerAPI = None, **kwargs) -> str | None:
    return client.create_platform(**kwargs)


@injector.inject
def update_platform(platform_uuid: str, client: CNODCServerAPI = None, **kwargs) -> bool:
    return client.update_platform(platform_uuid, **kwargs)


@injector.inject
def search_platforms(client: CNODCServerAPI = None, **kwargs) -> list[str]:
    return client.search_platforms(**kwargs)
