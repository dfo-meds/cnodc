import datetime
import decimal
import typing as t
import medsutil.math as amath
from medsutil import geodesy
from medsutil.awaretime import AwareDateTime
from medsutil.geodesy import YXPoint
from medsutil.ocproc2 import AbstractElement
from medsutil.ocproc2.util import RequiredQuality, Quality
from nodb.observations import NODBPlatform
from pipeman.programs.qc.base import DeepDiveChecker
from medsutil.ocproc2.refs import ParentRecordRef
from autoinject import injector
from nodb.interface import NODB

@injector.construct
class GTSPPSpeedCheck(DeepDiveChecker):

    # CCG "Specialty Vessel" has a maximum speed of 32 knots
    # this is the fastest CCG ship other than hovercraft and S&R lifeboats
    # it is also commonly used for scientific missions
    # see https://www.canada.ca/en/canadian-coast-guard/services/fleet/fleet-database.html
    # therefore it's a reasonable choice for a "default" top speed
    # where the top speed is known, it can be overridden on a platform-by-platform basis
    # Equivalents: 32 knots / 59.3 km h-1 / 16.5 m s-1 / 36.9 mi h-1
    DEFAULT_TOP_SPEED = 16.5  # m s-1 (actually 16.462222 but rounded up)

    def __init__(self,
                 past_time_days: int | float = 5,
                 international_dateline_check_threshold_degrees: int | float | None = 350,
                 searcher_cls=None):
        super().__init__(
            test_name='gtspp_speed',
            test_version='1.0',
            searcher_cls=searcher_cls,
            test_tags=['GTSPP_1.5'],
            working_sort=('obs_time', True),
        )
        self._rewrite_threshold = international_dateline_check_threshold_degrees
        self._past_time_days = past_time_days
        if self._rewrite_threshold is not None and not self._rewrite_threshold >= 181:
            raise ValueError("Invalid rewrite threshold, must be at least 181 degrees or the math causes issues")

    def parent_record_check(self, ref: ParentRecordRef):
        pid = self.get_current_platform_id()
        if pid is None:
            self.skip_review("no_platform")
            return
        lat_ref = ref.setdefault_coordinate_ref("Latitude")
        lon_ref = ref.setdefault_coordinate_ref("Longitude")
        time_ref = ref.setdefault_coordinate_ref("Time")
        ref_time = None
        for x in time_ref.single_element_refs():
            if x.element.is_iso_datetime():
                check_time = x.element.to_datetime()
                if ref_time is None or check_time < ref_time:
                    ref_time = check_time
        top_speed = self._get_top_speed(pid)
        if top_speed is None:
            self.skip_review("no_top_speed")
        # TODO: exit if all lat, lon, and times have failed the test,
        for previous_position in self._get_previous_positions(pid, ref_time):
            previous_lats = previous_position.coordinate_ref("Latitude")
            previous_lons = previous_position.coordinate_ref("Longitude")
            previous_times = previous_position.coordinate_ref("Time")
            if previous_lats is None or previous_lons is None or previous_times is None:
                continue
            for lat, lon, time in self.group_by_sensor_rank(lat_ref, lon_ref, time_ref):
                if lat is None or lon is None or time is None:
                    continue
                previous_lat = previous_lats.value_for_sensor_rank(lat.sensor_rank)
                previous_lon = previous_lons.value_for_sensor_rank(lon.sensor_rank)
                previous_time = previous_times.value_for_sensor_rank(time.sensor_rank)
                if previous_lat is None or previous_lon is None or previous_time is None:
                    continue
                with self.review_all("valid_speed", [lat, lon, time], fail_flag=3, pass_flag=1) as ctx:
                    ctx.check_review_already_complete(RequiredQuality.GOOD_VALUE)
                    self.require_quality(lat.element, RequiredQuality.HAS_UNITS)
                    self.require_quality(lon.element, RequiredQuality.HAS_UNITS)
                    self.require_quality(previous_time.element)
                    self.require_quality(previous_lat.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)
                    self.require_quality(previous_lon.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)

                    self._run_speed_test((
                        lon.element.to_numeric("degrees_east"),
                        lat.element.to_numeric("degrees_north"),
                        time.element.to_datetime()
                    ), (
                        previous_lon.element.to_numeric("degrees_east"),
                        previous_lat.element.to_numeric("degrees_north"),
                        previous_time.element.to_datetime()
                    ), t.cast(amath.AnyNumber, top_speed))

    def _get_previous_positions(self,
                                pid: str,
                                ref_time: AwareDateTime | None) -> t.Iterable[ParentRecordRef]:
        if ref_time is not None:
            start_time = ref_time - datetime.timedelta(days=self._past_time_days)
            end_time = ref_time - datetime.timedelta(seconds=1)
            for working_record in self.searcher.geosearch_working_records(
                    platform_uuid=pid,
                    start_time=start_time,
                    end_time=end_time):
                rec = working_record.record
                if rec is None:
                    continue
                if "Latitude" not in rec.coordinates or "Longitude" not in rec.coordinates or "Time" not in rec.coordinates:
                    continue
                yield ParentRecordRef(rec)
            for observation in self.searcher.geosearch_observations(
                platform_uuid=pid,
                start_time=start_time,
                end_time=end_time
            ):
                rec = observation.record
                if rec is None:
                    continue
                if "Latitude" not in rec.coordinates or "Longitude" not in rec.coordinates or "Time" not in rec.coordinates:
                    continue
                yield ParentRecordRef(rec)

    def _run_speed_test(self,
                        xyt2: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        xyt1: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        top_speed: amath.AnyNumber):
        # When crossing the international date line, we can
        # get some situations like (-179, y1) -> (179, y2)
        # This is not actually an error
        # We therefore define a threshold (default 350 degrees)
        # If the ship has traveled more than the threshold in terms
        # of longitude (dx), then we look at the alternative path instead
        # This is done by rewriting the coordinates to (0, y1) -> (dx, y2)
        # Experimental testing with the geographicalpy library indicates that
        # the WGS84 ellipsoid is insensitive to the actual longitudes used
        # as long as dx is the same, that is, given:
        #   da = distance( (a, y1), (a + dx, y2) )
        #   db = distance( (b, y1), (b + dx, y2) )
        # for constant y1, y2, dx, then da = db regardless of the values
        # chosen for a, b >= -180 and a+dx, b+dx < 180
        # so we can choose any x coordinate such that a + dx < 180
        # and obtain the same distance
        # the default threshold is 350 degrees which suggests the points are
        # within a 10 degree band around the international date line if they're
        # standardized to -180, 180 (which they should be).
        dx = abs(amath.sub(xyt2[0], xyt1[0]))
        if self._rewrite_threshold is not None and amath.gt(dx, self._rewrite_threshold):
            checked = (YXPoint(xyt2[1], 0), YXPoint(xyt1[1], amath.sub(360, dx)))
            distance = geodesy.geodesic_distance(*checked)
        else:
            checked = (YXPoint(xyt2[1], xyt2[0]), YXPoint(xyt1[1], xyt1[0]))
            distance = geodesy.geodesic_distance(*checked)
        time = abs((xyt2[2] - xyt1[2]).total_seconds())
        avg_speed = amath.div(distance, time)
        self._log.info(
            "speed: %s; expected: %s; distance: %s; time: %s; yx_new (%s), yx_old (%s)",
            avg_speed, top_speed, distance, time, *checked
        )
        self.assert_not_nan(avg_speed, msg="invalid_coordinates")
        self.assert_less_or_close(avg_speed, top_speed, msg="too_fast")

    def _get_top_speed(self, platform_uuid: str) -> amath.AnyNumber | None:
        if "top_speeds" not in self.batch_memory:
            self.batch_memory["top_speeds"] = top_speeds = {}
        else:
            top_speeds = self.batch_memory["top_speeds"]
        if platform_uuid not in top_speeds:
            top_speeds[platform_uuid] = self._real_get_top_speed(platform_uuid)
        return top_speeds[platform_uuid]

    def _real_get_top_speed(self, platform_uuid: str) -> amath.AnyNumber | None:
        platform = self.searcher.load_platform(platform_uuid)
        if platform:
            if platform.skip_speed_check:
                return None  # this indicates we should skip the check
            top_speed = platform.top_speed
            if top_speed is not None:
                if isinstance(top_speed, tuple):
                    return self.converter.convert(top_speed[0], top_speed[1], "m s-1")
                else:
                    return top_speed
        return self.DEFAULT_TOP_SPEED



