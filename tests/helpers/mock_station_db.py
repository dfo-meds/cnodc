import datetime
import typing as t

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, SingleElement
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
            'assigned_platform': [x for x in self._good_speed_check_working_records("assigned_platform")]
        }
        self._observations: dict[str, list[NODBObservationData]] = {
            'assigned_platform': [x for x in self._good_speed_check_observations("assigned_platform")]
        }

    def _good_speed_check_working_records(self, pid: str) -> t.Iterable[NODBWorkingRecord]:
        for idx, record in enumerate(self._good_speed_check_records(pid, ilat=5.375, ilon=5.475, it=AwareDateTime(2015, 1, 2, 3, 14, 5, tzinfo="Etc/UTC"))):
            obs_data = NODBWorkingRecord(working_uuid=f"wr_{idx}", received_date="2015-01-05")
            obs_data.record = record
            yield obs_data

    def _good_speed_check_observations(self, pid: str) -> t.Iterable[NODBObservationData]:
        for idx, record in enumerate(self._good_speed_check_records(pid, ilat=5.3, ilon=5.4, it=AwareDateTime(2015, 1, 2, 3, 4, 5, tzinfo="Etc/UTC"))):
            obs_data = NODBObservationData(obs_uuid=f"obs_{idx}", received_date="2015-01-05")
            obs_data.record = record
            yield obs_data

    def _good_speed_check_records(self,
                                  pid: str,
                                  dlat: float = 0.015,
                                  dlon: float = 0.015,
                                  dt: float = 120.0,
                                  ilat: float = 5.3,
                                  ilon: float = 5.4,
                                  it: AwareDateTime = AwareDateTime(2015, 1, 2, 3, 4, 5, tzinfo="Etc/UTC"),
                                  records=5) -> t.Iterable[ParentRecord]:
        for x in range(0, records):
            record = ParentRecord()
            record.metadata["CNODCPlatform"] = SingleElement(pid, Quality=1)
            record.coordinates["ObservationNumber"] = x + 1
            record.coordinates["Latitude"] = SingleElement(ilat + (dlat * x), Units="degree_north")
            record.coordinates["Longitude"] = SingleElement(ilon + (dlon * x), Units="degree_east")
            record.coordinates["Time"] = (it + datetime.timedelta(seconds=(dt * x))).isoformat()
            yield record

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
        if platform_id in self._working_records:
            for wr in self._working_records[platform_id]:
                record = wr.record
                if not record.coordinates.has_value("Time"):
                    continue
                record_time = record.coordinates.ideal("Time").to_datetime()
                if record_time > end_time:
                    continue
                if record_time < start_time:
                    continue
                yield wr

    def recent_observations(self,
                            platform_id: str,
                            start_time: AwareDateTime,
                            end_time: AwareDateTime) -> t.Iterable[NODBObservationData]:
        if platform_id in self._observations:
            for wr in self._observations[platform_id]:
                record = wr.record
                if not record.coordinates.has_value("Time"):
                    continue
                record_time = record.coordinates.ideal("Time").to_datetime()
                if record_time > end_time:
                    continue
                if record_time < start_time:
                    continue
                yield wr
