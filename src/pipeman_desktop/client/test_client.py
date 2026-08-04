import copy
import datetime
import typing as t
import uuid

from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_object
from nodb.observations import NODBWorkingRecord, PlatformStatus
from pipeman.programs.dmd.metadata import Platform
# this line is currently necessary to ensure it is properly override
# I should put in a fix for autoinject to ensure overrides always override
from pipeman_desktop.client.api_client import WebAPIClient

class MockNODB:

    PROGRAM_NAME = "pipeman_desktop.client.test_programs.program_1"

    def __init__(self):
        self._platforms: dict[str, dict[str, t.Any]] = {}
        self._records: dict[str, tuple[NODBWorkingRecord, list[dict] | None]] = {}
        self._queue_items: dict[str, dict] = {}
        self._queue_records: dict[str, list[str]] = {}
        self._batch_qc_queues: list[tuple[str, int, str | None]] = []
        setup = dynamic_object(f"{self.PROGRAM_NAME}.setup")
        setup(self)

    def batch_qc_endpoints(self) -> dict:
        return {
            f"batch_qc.{queue_name}{str(esc) if esc != 0 else ''}{str(subqueue_name) if subqueue_name else ''}.open": {
                "endpoint": "api/open",
                "kwargs": {
                    "queue_name": queue_name,
                    "escalation_level": esc,
                    "subqueue_name": subqueue_name
                }
            }
            for queue_name, esc, subqueue_name in self._batch_qc_queues
        }

    def add_batch_qc_endpoint(self, queue_name: str, escalation_level: int = 0, subqueue_name: str | None = None):
        self._batch_qc_queues.append((queue_name, escalation_level, subqueue_name))


    def add_queue_item(self,
                       records: t.Iterable[tuple[NODBWorkingRecord, list[dict] | None]],
                       queue_uuid: str,
                       queue_name: str,
                       subqueue_name: str | None = None,
                       escalation_level: int = 0):
        self._queue_items[queue_uuid] = {
            "queue_name": queue_name,
            "subqueue_name": subqueue_name,
            "escalation_level": escalation_level,
            "queue_uuid": queue_uuid,
            "success": True,
            "message": "Success",
            "actions": {
                "renew": {
                    "endpoint": f"api/renew/{queue_uuid}",
                },
                "close": {
                    "endpoint": f"api/close/{queue_uuid}",
                },
                "stream": {
                    "endpoint": f"api/stream/{queue_uuid}",
                },
            }
        }
        self._queue_records[queue_uuid] = []
        for record, actions in records:
            wuuid = str(record.working_uuid)
            self._queue_records[queue_uuid].append(wuuid)
            self._records[wuuid] = (record, actions)


    def get_queue_item(self,
                       queue_name: str,
                       subqueue_name: str | None,
                       escalation_level: int = 0) -> dict:
        for item_uuid, item in self._queue_items.items():
            if item["queue_name"] != queue_name:
                continue
            if subqueue_name is not None and subqueue_name != item["subqueue_name"]:
                continue
            if escalation_level != item["escalation_level"]:
                continue
            return {
                **item,
                "locked_until": (AwareDateTime.utcnow() + datetime.timedelta(hours=2))
            }
        return {
            "success": False,
            "message": "No queue items available",
            "actions": {},
            "queue_uuid": None,
            "locked_until": None,
            "data": None,
            "subqueue_name": subqueue_name,
            "queue_name": queue_name,
            "escalation_level": escalation_level
        }

    def renew_queue_item(self, queue_uuid: str):
        return {"success": True, "message": "Success"}

    def close_batch(self, queue_uuid: str, result: str):
        del self._queue_items[queue_uuid]
        return {
            "success": True,
            "message": "Success"
        }

    def stream_batch(self, queue_uuid: str):
        return {
            "success": True,
            "message": "Success",
            "data": [
                {
                    "working_uuid": self._records[ruuid][0].working_uuid,
                    "received_date": self._records[ruuid][0].received_date,
                    "source_file_uuid": self._records[ruuid][0].source_file_uuid,
                    "message_idx": self._records[ruuid][0].message_idx,
                    "record_idx": self._records[ruuid][0].record_idx,
                    "platform_uuid": self._records[ruuid][0].platform_uuid,
                    "data_mode": self._records[ruuid][0].data_mode,
                    "quality_checks": self._records[ruuid][0].quality_checks,
                    "actions": {
                        "fetch": {
                            "endpoint": f"api/fetch/{ruuid}",
                        },
                        "save": {
                            "endpoint": f"api/save/{ruuid}",
                        }
                    }
                }
                for ruuid in self._queue_records[queue_uuid]
            ]
        }

    def fetch_record(self, record_uuid: str) -> dict:
        return {
            "success": True,
            "message": "Success",
            "data": self._records[record_uuid][0].record.to_mapping(),
            "proposed_actions": copy.deepcopy(self._records[record_uuid][1])
        }

    def save_record(self, record_uuid: str, actions: list[dict]) -> dict:
        self._records[record_uuid] = (self._records[record_uuid][0], copy.deepcopy(actions))
        return {"success": True, "message": "Record updated"}

    def fetch_platform(self, platform_uuid) -> dict:
        if platform_uuid in self._platforms:
            return {
                "success": True,
                "message": "Success",
                "data": {
                    "actions": self._platform_actions(platform_uuid),
                    **self._platforms[platform_uuid]
                }
            }
        else:
            return {
                "success": False,
                "message": "No such platform",
                "data": None
            }

    def search_platforms(self,
                         wmo_id: str | None,
                         wigos_id: str | None,
                         platform_id: str | None,
                         platform_name: str | None,
                         time_frame: str | None) -> dict:
        results = []
        for pid, platform in self._platforms.items():
            check = False
            if wmo_id and platform["wmo_id"] is not None and platform["wmo_id"] == wmo_id:
                check = True
            elif wigos_id and platform["wigos_id"] is not None and platform["wigos_id"] == wigos_id:
                check = True
            elif platform_id and platform["platform_id"] is not None and platform["platform_id"] == platform_id:
                check = True
            elif platform_name and platform["platform_name"] is not None and platform["platform_name"] == platform_name:
                check = True
            if not check:
                continue
            if time_frame is not None and (platform["service_start_date"] or platform["service_end_date"]):
                tf = AwareDateTime.fromisoformat(time_frame)
                sd = AwareDateTime.fromisoformat(platform["service_start_date"]) if platform["service_start_date"] else None
                if sd is not None and tf < sd:
                    continue
                ed = AwareDateTime.fromisoformat(platform["service_end_date"]) if platform["service_end_date"] else None
                if ed is not None and tf > ed:
                    continue
            results.append({
                "platform_uuid": pid,
                "actions": self._platform_actions(pid)
            })
        return {
            "success": True,
            "message": "Success",
            "data": results,
        }

    def create_platform(self,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: str | None,
                        end_date: str | None,
                        status: str,
                        embargo_data_days: int | None,
                        map_to_uuid: str | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | None) -> dict:
        pid = str(uuid.uuid4())
        while pid in self._platforms:
            pid = str(uuid.uuid4())
        self._platforms[pid] = {
            "platform_uuid": pid,
            "platform_name": platform_name,
            "platform_id": platform_id,
            "wmo_id": wmo_id,
            "wigos_id": wigos_id,
            "platform_type": platform_type,
            "service_start_date": AwareDateTime.fromisoformat(start_date).isoformat() if start_date else None,
            "service_end_date": AwareDateTime.fromisoformat(end_date).isoformat() if end_date else None,
            "status": PlatformStatus(status).value,
            "embargo_data_days": embargo_data_days,
            "map_to_uuid": map_to_uuid,
            "metadata": {
                "skip_speed_check": skip_speed_check,
                "skip_on_land_check": skip_land_check,
                "dedupe_time_window": dedupe_time_window,
                "dedupe_distance_window": dedupe_distance_window,
                "top_speed": top_speed
            }
        }
        return {
            "success": True,
            "message": "Success",
            "data": {
                "platform_uuid": pid,
                "actions": self._platform_actions(pid)
            }
        }

    def _platform_actions(self, pid: str) -> dict:
        return {
            "view": {"endpoint": f"api/platforms/{pid}"},
            "update": {"endpoint": f"api/platforms/{pid}"}
        }

    def update_platform(self,
                        platform_uuid: str,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: str | None,
                        end_date: str | None,
                        status: str,
                        embargo_data_days: int | None,
                        map_to_uuid: str | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | None) -> dict:
        if platform_uuid not in self._platforms:
            return {
                "success": False,
                "message": "No such platform",
            }
        else:
            self._platforms[platform_uuid] = {
                "platform_uuid": platform_uuid,
                "platform_name": platform_name,
                "platform_id": platform_id,
                "wmo_id": wmo_id,
                "wigos_id": wigos_id,
                "platform_type": platform_type,
                "service_start_date": AwareDateTime.fromisoformat(start_date).isoformat() if start_date else None,
                "service_end_date": AwareDateTime.fromisoformat(end_date).isoformat() if end_date else None,
                "status": PlatformStatus(status).value,
                "embargo_data_days": embargo_data_days,
                "map_to_uuid": map_to_uuid,
                "metadata": {
                    "skip_speed_check": skip_speed_check,
                    "skip_on_land_check": skip_land_check,
                    "dedupe_time_window": dedupe_time_window,
                    "dedupe_distance_window": dedupe_distance_window,
                    "top_speed": top_speed
                }
            }
            return {
                "success": True,
                "message": "Success",
            }


class TestClient:

    def __init__(self):
        self.token = None
        self.mock_nodb = MockNODB()

    @property
    def is_logged_in(self):
        return self.token is not None

    def make_json_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        kwargs["app_id"] = "12345"
        if endpoint == 'api/create-access-token' and method == 'POST':
            return self._login(**kwargs)
        elif endpoint == 'api/remove-access-token' and method == 'POST':
            return self._logout(**kwargs)
        elif endpoint == 'api/renew-access-token' and method == 'POST':
            return self._renew(**kwargs)
        elif endpoint == "api/open" and method == "POST":
            return self._open_batch(**kwargs)
        elif endpoint.startswith("api/renew") and method == "POST":
            return self._renew_batch(endpoint.split("/", maxsplit=2)[2], **kwargs)
        elif endpoint.startswith("api/close") and method == "POST":
            return self._close_batch(endpoint.split("/", maxsplit=2)[2], **kwargs)
        elif endpoint.startswith("api/stream") and method == "GET":
            return self._stream_batch(endpoint.split("/", maxsplit=2)[2], **kwargs)
        elif endpoint.startswith("api/fetch/") and method == "GET":
            return self._fetch_record(endpoint.split('/', maxsplit=2)[2], **kwargs)
        elif endpoint == "api/fetch" and method == "GET":
            return self._fetch_record(**kwargs)
        elif endpoint.startswith("api/save") and method == "POST":
            return self._save_record(endpoint.split('/', maxsplit=2)[2], **kwargs)
        elif endpoint == "api/platforms" and method == "GET":
            return self._fetch_platform(**kwargs)
        elif endpoint == "api/platforms/search" and method == "GET":
            return self._search_platforms(**kwargs)
        elif endpoint == "api/platforms/create" and method == "POST":
            return self._create_platform(**kwargs)
        elif endpoint.startswith("api/platforms/") and method == "GET":
            return self._fetch_platform(endpoint.split('/', maxsplit=2)[2], **kwargs)
        elif endpoint.startswith("api/platforms/") and method == "POST":
            return self._update_platform(endpoint.split('/', maxsplit=2)[2], **kwargs)
        raise Exception('invalid test request')

    def _fetch_platform(self, platform_uuid: str, app_id: str) -> dict:
        return self.mock_nodb.fetch_platform(platform_uuid)

    def _search_platforms(self, app_id: str, **kwargs) -> dict:
        return self.mock_nodb.search_platforms(**kwargs)

    def _create_platform(self, app_id: str, **kwargs) -> dict:
        return self.mock_nodb.create_platform(**kwargs)

    def _update_platform(self, app_id: str, platform_uuid: str, **kwargs) -> dict:
        return self.mock_nodb.update_platform(platform_uuid, **kwargs)

    def _fetch_record(self,
                      working_record_uuid: str,
                      app_id: str):
        return self.mock_nodb.fetch_record(working_record_uuid)

    def _save_record(self,
                    record_uuid: str,
                    app_id: str,
                    actions: list[dict]):
        return self.mock_nodb.save_record(record_uuid, actions)

    def _open_batch(self,
                    queue_name: str,
                    app_id: str,
                    subqueue_name: str | None = None,
                    escalation_level: int = 0) -> dict:
        return self.mock_nodb.get_queue_item(queue_name, subqueue_name, escalation_level)

    def _renew_batch(self,
                     queue_uuid: str,
                     app_id: str) -> dict:
        return self.mock_nodb.renew_queue_item(queue_uuid)

    def _close_batch(self,
                     queue_uuid: str,
                     app_id: str,
                     result: str) -> dict:
        return self.mock_nodb.close_batch(queue_uuid, result)

    def _stream_batch(self,
                      queue_uuid: str,
                      app_id: str) -> dict:
        return self.mock_nodb.stream_batch(queue_uuid)

    def _logout(self, app_id: str, token: str) -> dict:
        return {'success': True}

    def _login(self, username: str, password: str, app_id: str) -> dict:
        return {
            'success': True,
            'token': 'abc',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            'access': {
                'user.renew': {
                    'endpoint': 'api/renew-access-token',
                } ,
                'user.logout': {
                    'endpoint': 'api/remove-access-token',
                },
                "desktop.find_working_record": {
                    "endpoint": "api/fetch"
                },
                "desktop.search_platforms": {
                    "endpoint": "api/platforms/search"
                },
                "desktop.create_platform": {
                    "endpoint": "api/platforms/create"
                },
                "desktop.find_platform": {
                    "endpoint": "api/platforms"
                },
                **self.mock_nodb.batch_qc_endpoints(),
            },
            'username': username,
            'display': username,
        }

    def _renew(self, app_id: str, token: str = None):
        return {
            'success': True,
            'token': 'abc',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
        }
