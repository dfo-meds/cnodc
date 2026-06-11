import decimal
import typing as t
import medsutil.math as amath
from medsutil import geodesy
from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import AbstractElement
from nodb.observations import NODBPlatform
from pipeman.programs.qc.base import DeepDiveChecker, ParentRecordRef
from autoinject import injector
from nodb.interface import NODB

@injector.construct
class GTSPPSpeedTest(DeepDiveChecker):

    nodb: NODB

    DEFAULT_TOP_SPEED = 40

    def __init__(self, **kwargs):
        super().__init__(
            'gtspp_speed',
            '1.0',
            test_tags=['GTSPP_1.5'],
            working_sort='obs_time_asc',
            **kwargs
        )

    def parent_record_check(self, ref: ParentRecordRef):
        with self.review_all("valid_speed", [
            self.get_record_coordinate_ref(ref, "Latitude", True),
            self.get_record_coordinate_ref(ref, "Longitude", True),
            self.get_record_coordinate_ref(ref, "Time", True),
        ], fail_flag=3, pass_flag=1) as ctx:
            ctx.check_review_already_complete(skip_dubious=True, skip_empty=True)

            record = ref.record

            # ensure we have all these values
            self.require_value(record.metadata["CNODCPlatform"])
            self.require_value(record.metadata["Latitude"])
            self.require_value(record.metadata["Longitude"])
            self.require_value(record.metadata["Time"])

            # make our nice little record tuples
            xx = record.coordinates['Longitude'].to_numeric("degrees_east")
            yy = record.coordinates['Latitude'].to_numeric("degrees_north")
            tt = record.coordinates['Time'].to_datetime()
            info = (amath.radians(xx), amath.radians(yy), tt)
            pid = record.metadata['CNODCPlatform'].to_string()

            top_speed = ...

            try:
                for position in self._get_previous_positions(pid):
                    if top_speed is ...:
                        top_speed = self._get_top_speed(pid)
                    self._run_speed_test(info, position, top_speed)
            finally:
                self._add_previous_position(pid, info)

    def _run_speed_test(self,
                        xyt2: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        xyt1: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime],
                        top_speed: amath.AnyNumber | None):
        if top_speed is None:
            return
        distance = geodesy.haversine((xyt2[1], xyt2[0]), (xyt1[1], xyt1[0]))
        time = (xyt2[2] - xyt1[2]).total_seconds()
        self.assert_less_or_close(amath.div(distance, time), top_speed, flag=3)

    def _add_previous_position(self, sid: str, info: tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime]):
        if "positions" not in self.batch_memory:
            self.batch_memory["positions"] = {sid: [info]}
        elif sid not in self.batch_memory["positions"]:
            self.batch_memory["positions"][sid] = [info]
        else:
            self.batch_memory["positions"][sid].append(info)

    def _get_previous_positions(self, sid: str) -> list[tuple[amath.AnyNumber, amath.AnyNumber, AwareDateTime]]:
        # TODO: should we stream records from the database?
        positions: dict[str, t.Any]
        if "positions" in self.batch_memory and sid in self.batch_memory["positions"]:
            return self.batch_memory["positions"][sid]
        return []

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

