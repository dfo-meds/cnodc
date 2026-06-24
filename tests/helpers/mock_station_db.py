import typing as t

from medsutil.awaretime import AwareDateTime
from nodb.observations import NODBPlatform, NODBWorkingRecord, NODBObservationData


class TestStationDatabase:
    def __init__(self, db=None):
        self._platforms: dict[str, NODBPlatform] = {
            'assigned_platform': NODBPlatform(platform_uuid="assigned_platform"),
            'wmo_12345': NODBPlatform(platform_uuid="wmo_12345", wmo_id="12345"),
            'wigos_123456': NODBPlatform(platform_uuid="wigos_123456", wigos_id="123456"),
            'wmo_12345_redirect': NODBPlatform(platform_uuid="wmo_12345_redirect", wmo_id="1200345", map_to_uuid="wmo_12345"),
            'wmo_12346_redirect': NODBPlatform(platform_uuid="wmo_12345_redirect", wmo_id="1200346", map_to_uuid="wmo_12346"),
            'id_ship': NODBPlatform(platform_uuid="id_ship", platform_id="SHIP"),
            'name_shippymcship': NODBPlatform(platform_uuid="name_shippymcship", platform_name="ShippyMcShip"),
            'wmo_23456_2020': NODBPlatform(platform_uuid='wmo_23456_2020', wmo_id='23456', service_start_date='2020-01-01T00:00:00+00:00', service_end_date='2020-12-31T23:59:59+00:00'),
            'wmo_23456_2024': NODBPlatform(platform_uuid='wmo_23456_2024', wmo_id='23456', service_start_date='2024-01-01T00:00:00+00:00', service_end_date='2024-12-31T23:59:59+00:00'),
            'top_speed_knots': NODBPlatform(platform_uuid='top_speed_knots', metadata={'top_speed': "20 kts"}),
            'top_speed_mps': NODBPlatform(platform_uuid='top_speed_mps', metadata={'top_speed': "20 m s-1"}),
            'top_speed_kph': NODBPlatform(platform_uuid='top_speed_kph', metadata={'top_speed': "20 km h-1"}),
            'top_speed_mph': NODBPlatform(platform_uuid='top_speed_mph', metadata={'top_speed': "105600 ft hour-1"}),
            'top_speed_integer': NODBPlatform(platform_uuid='top_speed_mph', metadata={'top_speed': 20}),
            'top_speed_float': NODBPlatform(platform_uuid='top_speed_mph', metadata={'top_speed': 20.0}),
            'top_speed_dict_knots': NODBPlatform(platform_uuid='top_speed_dict', metadata={'top_speed': {'_value': 20, '_metadata': {'Units': 'kts'}}}),
            'top_speed_none': NODBPlatform(platform_uuid='top_speed_dict', metadata={'top_speed': None}),
            'top_speed_missing': NODBPlatform(platform_uuid='top_speed_dict', metadata={}),
            'top_speed_skip': NODBPlatform(platform_uuid='top_speed_dict', metadata={'top_speed': 20, 'skip_speed_check': True}),

        }
        self._working_records: dict[str, list[NODBWorkingRecord]] = {

        }
        self._observations: dict[str, list[NODBObservationData]] = {

        }

    def find_by_uuid(self, platform_uuid: str) -> NODBPlatform | None:
        if platform_uuid in self._platforms:
            return self._platforms[platform_uuid]
        return None

    def search(self,
               *,
               platform_id: str | None = None,
               platform_name: str | None = None,
               wigos_id: str | None = None,
               wmo_id: str | None = None,
               in_service_time: AwareDateTime | None = None) -> t.Iterable[NODBPlatform]:
        for pid, platform in self._platforms.items():
            if platform_id is not None and platform.platform_id != platform_id:
                continue
            if wigos_id is not None and platform.wigos_id != wigos_id:
                continue
            if wmo_id is not None and platform.wmo_id != wmo_id:
                continue
            if platform_name is not None and platform.platform_name != platform_name:
                continue
            if in_service_time is not None:
                if platform.service_start_date is not None and in_service_time < platform.service_start_date:
                    continue
                if platform.service_end_date is not None and in_service_time > platform.service_end_date:
                    continue
            yield platform

    def recent_working_records(self,
                               platform_id: str,
                               start_time: AwareDateTime,
                               end_time: AwareDateTime) -> t.Iterable[NODBWorkingRecord]:
        ...

    def recent_observations(self,
                            platform_id: str,
                            start_time: AwareDateTime,
                            end_time: AwareDateTime) -> t.Iterable[NODBObservationData]:
        ...
