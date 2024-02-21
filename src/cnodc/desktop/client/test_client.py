import datetime
import json
import pathlib
import typing as t
import random
from requests import HTTPError

import cnodc.ocproc2 as ocproc2
from cnodc.codecs import OCProc2YamlCodec
from cnodc.ocproc2.operations import QCOperator


class TestClient:

    def __init__(self):
        self._token = None

    def make_json_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        if endpoint == 'login' and method == 'POST':
            return self._login(**kwargs)
        elif endpoint == 'logout' and method == 'POST':
            return self._logout()
        elif endpoint == 'change-password':
            return self._change_password(**kwargs)
        elif endpoint == 'renew' and method == 'POST':
            return self._renew()
        elif endpoint == 'stations/new' and method == 'POST':
            return self._create_station(**kwargs)
        elif endpoint.startswith('next/') and method == 'POST':
            return self._next_queue_item(endpoint[5:])
        elif endpoint.startswith('release/') and method == 'POST':
            return self._release_item(**kwargs)
        elif endpoint.startswith('fail/') and method == 'POST':
            return self._fail_item(**kwargs)
        elif endpoint.startswith('complete/') and method == 'POST':
            return self._complete_item(**kwargs)
        elif endpoint.startswith('renew/') and method == 'POST':
            return self._renew_item(**kwargs)
        elif endpoint.startswith('apply/') and method == 'POST':
            return self._apply_to_item(**kwargs)
        elif endpoint.startswith('escalate/') and method == 'POST':
            return self._escalate_item(**kwargs)
        elif endpoint.startswith('descalate/') and method == 'POST':
            return self._descalate_item(**kwargs)
        raise Exception('invalid test request')

    def _change_password(self, password):
        if len(password) < 15:
            raise Exception('Password too short')
        return {'success': True}

    def make_working_records_request(self, endpoint: str, method: str, **kwargs: str) -> t.Iterable[tuple[str, str, ocproc2.ParentRecord, list[dict]]]:
        if endpoint.startswith('download/') and method == 'GET':
            return self._download_station_failure(endpoint[9:], **kwargs)
        raise Exception('invalid test request')

    def make_json_dict_list_request(self, endpoint: str, method: str, **kwargs: str) -> t.Iterable[dict]:
        if endpoint == 'stations' and method == 'GET':
            return self._list_stations()
        raise Exception('invalid test request')

    def set_token(self, token):
        self._token = token

    def is_logged_in(self):
        return self._token is not None

    def _logout(self) -> dict:
        return {'success': True}

    def _login(self, username, password) -> dict:
        queue_path = pathlib.Path(__file__).absolute().parent / 'ocproc2_examples'
        queues = []
        for file in queue_path.glob('*.yaml'):
            queues.append(file.name[:-5])
        return {
            'token': 'abc',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            'access': {
                'other': {
                    'change_password': 'change-password',
                    'renew': 'renew',
                    'logout': 'logout',
                    'access': 'access',
                    'create_station': 'stations/new',
                    'list_stations': 'stations',
                },
                'service_queues': {
                    q: {
                        'url': f'next/{q}',
                        'name': {
                            'en': q,
                            'fr': q
                        }
                    }
                    for q in queues
                },
                'workflows': {
                    'test': {
                        'url': 'upload/test',
                        'name': {
                            'en': 'Test Workflow',
                            'fr': 'Workflow test'
                        }
                    }
                }
            },
            'username': username
        }

    def _renew(self):
        return self._login('', '')

    def _list_stations(self) -> t.Iterable[dict]:
        return []

    def _create_station(self, station: dict) -> dict:
        return {'success': True}

    def _next_queue_item(self, error_file_name: str) -> dict:
        tests = []
        if error_file_name.startswith('station_'):
            tests.append('nodb_station_check')
        return {
            'item_uuid': error_file_name,
            'app_id': '67890',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            # TODO
            'current_tests': tests,
            'batch_size': 1,
            'actions': {
                'renew': f'renew/{error_file_name}',
                'release': f'release/{error_file_name}',
                'fail': f'fail/{error_file_name}',
                'complete': f'complete/{error_file_name}',
                'escalate': f'escalate/{error_file_name}',
                'descalate': f'descalate/{error_file_name}',
                'download_working': f'download/{error_file_name}',
                'apply_working': f'apply/{error_file_name}',
                'clear_actions': f'clear/{error_file_name}',
            }
        }

    def _download_station_failure(self, filename: str, app_id: str) -> t.Iterable[tuple[str, str, ocproc2.ParentRecord, list[dict]]]:
        if app_id != '67890':
            raise Exception('invalid app id')
        file_path = pathlib.Path(__file__).absolute().parent / 'ocproc2_examples' / f'{filename}.yaml'
        codec = OCProc2YamlCodec()
        for idx, record in enumerate(codec.load_all(file_path)):
            yield str(idx), record.generate_hash(), record, []

    def _get_lat(self, x: int):
        return 45 - (0.03 * x) + (random.randint(-100, 100) / 100) - (0 if x % 2 else 10)

    def _get_long(self, x: int):
        return -45 - (0.03 * x) + (random.randint(-100, 100) / 100)

    def _get_time(self, x: int):
        dt = datetime.datetime.now(datetime.timezone.utc)
        dt += datetime.timedelta(hours=x, minutes=random.randint(0, 10))
        return dt.isoformat()

    def _temp_wq(self, depth: float):
        if depth < 100:
            return 1
        elif depth < 150:
            return 2
        elif depth < 200:
            return 3
        elif depth < 250:
            return 4
        elif depth < 300:
            return 5
        elif depth < 350:
            return 13
        elif depth < 400:
            return 14
        else:
            return 0

    def _temp(self, depth: float):
        if depth > 300:
            return 279.15 - (depth / 100) + (random.randint(0, 100) / 200)
        elif depth < 50:
            return 305.15 - (depth / 100) + (random.randint(0, 100) / 200)
        else:
            return 310 - (0.114 * depth) + (random.randint(0, 100) / 200)

    def _sal(self, depth: float):
        if depth < 100:
            return 36.000 - (depth / 1000) + (random.randint(0, 100) / 500)
        elif depth > 500:
            return 35 - random.randint(-50, 50) / 1000
        else:
            return 36.25 - (0.0025 * depth) + (random.randint(0, 100) / 500)

    def _curspd(self, depth: float):
        if depth < 200 or depth > 400:
            return 0.02 + (random.randint(0, 100) / 1000)
        else:
            return 0.15 + (random.randint(0, 100) / 100)

    def _curdir(self, depth: float):
        if depth < 200 or depth > 400:
            return 45 + random.randint(-10, 10)
        else:
            return 97 + random.randint(-10, 10)

    def _release_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _escalate_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _descalate_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _fail_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _complete_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _renew_item(self, app_id: str) -> dict:
        if app_id != '67890':
            raise Exception('invalid app id')
        return {'success': True}

    def _apply_to_item(self, app_id: str, operations: dict) -> dict:
        results = {}
        if app_id != '67890':
            raise Exception('invalid app id')
        if not isinstance(operations, dict):
            raise Exception('invalid operations')
        for x in operations:
            if 'hash' not in operations[x]:
                raise Exception('invalid operation format')
            if not isinstance(operations[x]['hash'], str):
                raise Exception('invalid operation format')
            if 'actions' not in operations[x]:
                raise Exception('invalid operation format')
            if not isinstance(operations[x]['actions'], list):
                raise Exception('invalid operation format')
            for y in operations[x]['actions']:
                if not isinstance(y, dict):
                    raise Exception('invalid operation format')
                QCOperator.from_map(y)
                results[x] = [True, operations[x]['hash']]
        return results
