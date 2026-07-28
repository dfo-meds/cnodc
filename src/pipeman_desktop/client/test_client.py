import copy
import datetime
import typing as t
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_object
from nodb.observations import NODBWorkingRecord
# this line is currently necessary to ensure it is properly override
# I should put in a fix for autoinject to ensure overrides always override
from pipeman_desktop.client.api_client import WebAPIClient

class MockNODB:

    PROGRAM_NAME = "pipeman_desktop.client.test_programs.program_1"

    def __init__(self):
        self._records: dict[str, tuple[NODBWorkingRecord, list[dict] | None]] = {}
        self._queue_items: dict[str, dict] = {}
        self._queue_records: dict[str, list[str]] = {}
        setup = dynamic_object(f"{self.PROGRAM_NAME}.setup")
        setup(self)

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


class TestClient:

    def __init__(self):
        self.token = None
        self.mock_nodb = MockNODB()

    @property
    def is_logged_in(self):
        return self.token is not None

    def make_json_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        if endpoint == 'api/create-access-token' and method == 'POST':
            return self._login(**kwargs)
        elif endpoint == 'api/remove-access-token' and method == 'POST':
            return self._logout()
        elif endpoint == 'api/renew-access-token' and method == 'POST':
            return self._renew()
        elif endpoint == "api/open" and method == "POST":
            return self._open_batch(**kwargs, app_id="12345")
        elif endpoint.startswith("api/renew") and method == "POST":
            return self._renew_batch(endpoint.split("/", maxsplit=2)[2], **kwargs, app_id="12345")
        elif endpoint.startswith("api/close") and method == "POST":
            return self._close_batch(endpoint.split("/", maxsplit=2)[2], **kwargs, app_id="12345")
        elif endpoint.startswith("api/stream") and method == "GET":
            return self._stream_batch(endpoint.split("/", maxsplit=2)[2], **kwargs, app_id="12345")
        elif endpoint.startswith("api/fetch") and method == "GET":
            return self._fetch_record(endpoint.split('/', maxsplit=2)[2], **kwargs, app_id="12345")
        elif endpoint.startswith("api/save") and method == "POST":
            return self._save_record(endpoint.split('/', maxsplit=2)[2], **kwargs, app_id="12345")
        raise Exception('invalid test request')

    def _fetch_record(self,
                      record_uuid: str,
                      app_id: str):
        return self.mock_nodb.fetch_record(record_uuid)

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

    def _logout(self) -> dict:
        return {'success': True}

    def _login(self, username: str, password: str) -> dict:
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
                'batch_qc.integrity_check.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'integrity_check',
                    },
                },
                'batch_qc.platform_check.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'platform_check',
                    },
                },
                'batch_qc.gtspp_qca.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'gtspp_qca',
                    },
                },
                'batch_qc.gtspp_qcb.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'gtspp_qcb',
                    },
                },
                'batch_qc.relationships.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'relationships',
                    },
                },
                'batch_qc.integrity_check_esc.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'escalation_level': 1,
                        'queue_name': 'integrity_check'
                    }
                },
                'batch_qc.platform_check_esc.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'platform_check',
                        'escalation_level': 1,
                    }
                },
                'batch_qc.gtspp_qca_esc.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'gtspp_qca',
                        'escalation_level': 1,
                    }
                },
                'batch_qc.gtspp_qcb_esc.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'gtspp_qcb',
                        'escalation_level': 1,
                    }
                },
                'batch_qc.relationships_esc.open': {
                    'endpoint': 'api/open',
                    'kwargs': {
                        'queue_name': 'relationships',
                        'escalation_level': 1,
                    }
                },
            },
            'username': username,
            'display': username,
        }

    def _renew(self):
        return {
            'success': True,
            'token': 'abc',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
        }

    def _list_stations(self) -> t.Iterable[dict]:
        return []

    def _create_station(self, station: dict) -> dict:
        return {'success': True}
