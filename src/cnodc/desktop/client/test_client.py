import datetime
import typing as t

from requests import HTTPError

import cnodc.ocproc2.structures as ocproc2


class TestClient:

    def __init__(self):
        self._token = None

    def make_json_request(self, endpoint: str, method: str, **kwargs: str) -> dict:
        if endpoint == 'login' and method == 'POST':
            return self._login(**kwargs)
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
        raise Exception('invalid test request')

    def make_working_records_request(self, endpoint: str, method: str, **kwargs: str) -> t.Iterable[tuple[str, str, ocproc2.DataRecord]]:
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

    def _create_station(self, station_def: dict) -> dict:
        return {'success': True}

    def _next_station_failure(self) -> dict:
        return {
            'item_uuid': '12345',
            'app_id': '67890',
            'expiry': (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat(),
            'actions': {
                'release': 'release/12345',
                'fail': 'fail/12345',
                'complete': 'complete/12345',
                'renew': 'renew/12345',
                'download_working': 'download/12345',
            }
        }

    def _download_station_failure(self, app_id: str) -> t.Iterable[tuple[str, str, ocproc2.DataRecord]]:
        if app_id != '67890':
            raise Exception('invalid app id')
        r = ocproc2.DataRecord()
        r.metadata['WMOID'] = '12345'
        r.coordinates['Latitude'] = ocproc2.Value(45.321, Uncertainty=0.0005, Units='degrees')
        r.coordinates['Longitude'] = ocproc2.Value(-47.123, Uncertainty=0.0005, Units='degrees')
        r.coordinates['Time'] = ocproc2.Value('2023-02-06T09:58:00+00:00')
        r.metadata['CNODCStationString'] = 'WMOID=12345'
        for i in range(0, 10):
            sr = ocproc2.DataRecord()
            sr.coordinates['Depth'] = ocproc2.Value((10 * i) + 1, Uncertainty=0.5, Units="m")
            sr.parameters['Temperature'] = ocproc2.Value(275, Uncertainty=0.5, Units='K')
            r.subrecords.append_record_set('PROFILE', 0, sr)
        r.record_qc_test_result(
            'nodb_station_check',
            '1.0',
            ocproc2.QCResult.MANUAL_REVIEW,
            messages=[
                ocproc2.QCMessage('station_no_record', '')
            ],
            test_tags=['GTSPP_1.1']
        )
        r.add_history_entry('Test record, not real', 'desktop_test', '1.0', 'abc', ocproc2.MessageType.INFO)
        yield '0001', r.generate_hash(), r

    def _release_item(self, app_id: str) -> dict:
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
