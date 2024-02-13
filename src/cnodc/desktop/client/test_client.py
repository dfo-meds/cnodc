import datetime
import json
import typing as t
import random
from requests import HTTPError

import cnodc.ocproc2.structures as ocproc2
from cnodc.ocproc2.operations import QCOperator


class TestClient:

    def __init__(self):
        self._token = None

    def make_json_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        if endpoint == 'login' and method == 'POST':
            return self._login(**kwargs)
        elif endpoint == 'logout' and method == 'POST':
            return self._logout()
        elif endpoint == 'renew' and method == 'POST':
            return self._renew()
        elif endpoint == 'stations/new' and method == 'POST':
            return self._create_station(**kwargs)
        elif endpoint == 'next/station-failure' and method == 'POST':
            return self._next_station_failure()
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
            return self._apply_to_item(**kwargs)
        elif endpoint.startswith('descalate/') and method == 'POST':
            return self._apply_to_item(**kwargs)
        raise Exception('invalid test request')

    def make_working_records_request(self, endpoint: str, method: str, **kwargs: str) -> t.Iterable[tuple[str, str, ocproc2.DataRecord, list[dict]]]:
        if endpoint == 'download/12345' and method == 'GET':
            return self._download_station_failure(**kwargs)
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
        return {
            'token': 'abc',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            'access': [
                'queue:station-failure'
            ],
            'username': username
        }

    def _renew(self):
        return self._login('', '')

    def _list_stations(self) -> t.Iterable[dict]:
        return []

    def _create_station(self, station: dict) -> dict:
        return {'success': True}

    def _next_station_failure(self) -> dict:
        return {
            'item_uuid': '12345',
            'app_id': '67890',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            'current_tests': ['nodb_station_check'],
            'batch_size': 9,
            'actions': {
                'renew': 'renew/12345',
                'release': 'release/12345',
                #'fail': 'fail/12345',
                'complete': 'complete/12345',
                'escalate': 'escalate/12345',
                'descalate': 'descalate/12345',
                'download_working': 'download/12345',
                'apply_working': 'apply/12345',
                'clear_actions': 'clear/12345',
            }
        }

    def _download_station_failure(self, app_id: str) -> t.Iterable[tuple[str, str, ocproc2.DataRecord, list[dict]]]:
        if app_id != '67890':
            raise Exception('invalid app id')
        for i in range(1, 10):
            r = ocproc2.DataRecord()

            r.coordinates['Latitude'] = ocproc2.Value(round(self._get_lat(i), 3), Uncertainty=0.0005, Units='degrees', WorkingQuality=0)
            r.coordinates['Longitude'] = ocproc2.Value(round(self._get_long(i), 3), Uncertainty=0.0005, Units='degrees', WorkingQuality=0)
            r.coordinates['Time'] = ocproc2.Value(self._get_time(i), WorkingQuality=0)
            for j in range(0, 20):
                sr = ocproc2.DataRecord()
                depth = (j * 50) + (random.randint(0, 10) / 100)
                sr.coordinates['Depth'] = ocproc2.Value(depth, Uncertainty=0.5, Units="m", WorkingQuality=(0 if i < 8 else (3 if i == 8 else 4)))
                sr.parameters['Temperature'] = ocproc2.Value(self._temp(depth), Uncertainty=0.005, Units='K', WorkingQuality=self._temp_wq(depth))
                sr.parameters['PracticalSalinity'] = ocproc2.Value(self._sal(depth), Uncertainty=0.0005, Units='0.001', WorkingQuality=0)
                sr.parameters['CurrentSpeed'] = ocproc2.Value(self._curspd(depth), Units='m s-1', WorkingQuality=0, Uncertainty=0.5)
                sr.parameters['CurrentDirection'] = ocproc2.Value(self._curdir(depth), Units='degrees', WorkingQuality=0, Uncertainty=1)
                r.subrecords.append_record_set('PROFILE', 0, sr)
            if i % 2 == 0:
                r.metadata['WMOID'] = '12345'
                r.metadata['CNODCStationString'] = ocproc2.Value('WMOID=12345', WorkingQuality=0)
                r.record_qc_test_result(
                    'nodb_station_check',
                    '1.0',
                    ocproc2.QCResult.MANUAL_REVIEW,
                    messages=[
                        ocproc2.QCMessage('station_no_record', ''),
                        ocproc2.QCMessage('temp_invalid', 'subrecords/PROFILE/0/4/parameters/Temperature')
                    ],
                    test_tags=['GTSPP_1.1']
                )
            else:
                r.metadata['WMOID'] = '23456'
                r.metadata['CNODCStation'] = ocproc2.Value('12345', WorkingQuality=1)
                r.record_qc_test_result(
                    'nodb_station_check',
                    '1.0',
                    ocproc2.QCResult.PASS,
                    messages=[],
                    test_tags=['GTSPP_1.1']
                )
            r.add_history_entry('Test record, not real', 'desktop_test', '1.0', 'abc', ocproc2.MessageType.INFO)
            yield f'000{i}', r.generate_hash(), r, []

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
