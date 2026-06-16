import datetime
import decimal
import typing as t
import medsutil.math as amath
from medsutil import geodesy
from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import AbstractElement
from medsutil.ocproc2.util import RequiredQuality, Quality
from nodb.observations import NODBPlatform
from pipeman.programs.qc.base import DeepDiveChecker
from medsutil.ocproc2.refs import ParentRecordRef
from autoinject import injector
from nodb.interface import NODB

@injector.construct
class GTSPPSpeedTest(DeepDiveChecker):

    nodb: NODB

    DEFAULT_TOP_SPEED = 40

    def __init__(self, past_time_days: int | float = 5):
        super().__init__(
            'gtspp_speed',
            '1.0',
            test_tags=['GTSPP_1.5'],
            working_sort='obs_time_asc',
        )
        self._past_time_days = past_time_days

    def parent_record_check(self, ref: ParentRecordRef):
        self.require_quality(ref.record.metadata["CNOCDPlatform"])
        pid = ref.record.metadata['CNODCPlatform'].to_string()
        lat_ref = ref.setdefault_coordinate_ref("Latitude")
        lon_ref = ref.setdefault_coordinate_ref("Longitude")
        time_ref = ref.setdefault_coordinate_ref("Time")
        ref_time = None
        for x in time_ref.single_element_refs():
            if x.element.is_empty() or not x.element.is_iso_datetime():
                continue
            else:
                check_time = x.element.to_datetime()
                if check_time < ref_time:
                    ref_time = check_time
        top_speed = ...
        for previous_position in self._get_previous_positions(pid, ref_time):
            previous_lats = previous_position.parameter_ref("Latitude")
            previous_lons = previous_position.parameter_ref("Longitude")
            previous_times = previous_position.parameter_ref("Time")
            if previous_lats is None or previous_lons is None or previous_times is None:
                continue
            for lat, lon in self.extract_keyed_parameters(lat_ref, lon_ref):
                if lat is None or lon is None:
                    continue
                previous_lat = previous_lats.keyed_parameter(lat.sensor_rank)
                previous_lon = previous_lons.keyed_parameter(lon.sensor_rank)
                if previous_lat is None or previous_lon is None:
                    continue
                for time, in self.extract_keyed_parameters(time_ref):
                    if time is None:
                        continue
                    previous_time = previous_times.keyed_parameter(time.sensor_rank)
                    if previous_time is None:
                        continue
                    with self.review_all("valid_speed", [lat, lon, time], fail_flag=3, pass_flag=1) as ctx:
                        ctx.check_review_already_complete(RequiredQuality.GOOD_VALUE)
                        self.require_quality(lat.element, RequiredQuality.HAS_UNITS)
                        self.require_quality(lon.element, RequiredQuality.HAS_UNITS)
                        self.require_quality(previous_time.element)
                        self.require_quality(previous_lat.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)
                        self.require_quality(previous_lon.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)

                        if top_speed is ...:
                            top_speed = self._get_top_speed(pid)

                        self._run_speed_test((
                            lon.element.to_numeric("degrees_east"),
                            lat.element.to_numeric("degrees_north"),
                            time.element.to_datetime()
                        ), (
                            previous_lon.element.to_numeric("degrees_east"),
                            previous_lat.element.to_numeric("degrees_north"),
                            previous_time.element.to_datetime()
                        ), top_speed)

    def _run_speed_test(self,
                        xyt2: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        xyt1: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        top_speed: amath.AnyNumber | None):
        if top_speed is None:
            return
        distance = geodesy.haversine((xyt2[1], xyt2[0]), (xyt1[1], xyt1[0]))
        time = (xyt2[2] - xyt1[2]).total_seconds()
        self.assert_less_or_close(amath.div(distance, time), top_speed)

    def _get_previous_positions(self,
                                pid: str,
                                ref_time: AwareDateTime | None) -> t.Iterable[ParentRecordRef]:
        if ref_time is not None:
            start_time = ref_time - datetime.timedelta(days=self._past_time_days)
            end_time = ref_time - datetime.timedelta(seconds=1)
            with self.nodb as db:
                for working_record in NODBPlatform.stream_recent_working_records(db, pid, start_time, end_time):
                    rec = working_record.record
                    if rec is None:
                        continue
                    if "Latitude" not in rec.coordinates or "Longitude" not in rec.coordinates or "Time" not in rec.coordinates:
                        continue
                    yield ParentRecordRef("", None, rec)
                for observation in NODBPlatform.stream_recent_observations(db, pid, start_time, end_time):
                    rec = observation.record
                    if rec is None:
                        continue
                    if "Latitude" not in rec.coordinates or "Longitude" not in rec.coordinates or "Time" not in rec.coordinates:
                        continue
                    yield ParentRecordRef("", None, rec)

    def _get_top_speed(self, platform_uuid: str) -> amath.AnyNumber | None:
        if "top_speeds" not in self.batch_memory:
            self.batch_memory["top_speeds"] = top_speeds = {}
        else:
            top_speeds = self.batch_memory["top_speeds"]
        if platform_uuid not in top_speeds:
            top_speeds[platform_uuid] = self._real_get_top_speed(platform_uuid)
        return top_speeds[platform_uuid]

    def _real_get_top_speed(self, platform_uuid: str) -> amath.AnyNumber | None:
        with self.nodb as db:
            platform = NODBPlatform.find_by_uuid(db, platform_uuid)
            if platform:
                if platform.metadata.get("skip_speed_check", False):
                    return None
                return self._parse_top_speed(platform.metadata.get("top_speed", self.DEFAULT_TOP_SPEED))
            else:
                return self._parse_top_speed(self.DEFAULT_TOP_SPEED)

    def _parse_top_speed(self, top_speed: t.Any) -> amath.AnyNumber | None:
        if isinstance(top_speed, (int, float)):
            return decimal.Decimal(top_speed)
        elif isinstance(top_speed, str):
            if " " in top_speed:
                speed, units = top_speed.split(" ", 1)
            else:
                speed = top_speed
                units = "m s-1"
            return self.converter.convert(decimal.Decimal(speed.strip()), units.strip(), "m s-1")
        elif isinstance(top_speed, dict):
            try:
                element = AbstractElement.build_from_mapping(top_speed)
                if element.is_numeric():
                    return element.to_numeric("m s-1")
                return None
            except KeyError:
                return None
        else:
            return None

