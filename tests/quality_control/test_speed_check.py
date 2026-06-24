import datetime
import decimal

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import MultiElement, SingleElement, ParentRecord, QCResult
from medsutil.ocproc2.refs import ParentRecordRef
from pipeman.programs.gtspp.speed import GTSPPSpeedCheck
from pipeman.programs.qc.platform import NODBPlatformCheck
from tests.helpers.base_test_case import sub_tests
from tests.helpers.qc_check_base import QCCheckerTestCase
from tests.helpers.mock_station_db import TestStationDatabase


class TestGTSPPSpeedCheck(QCCheckerTestCase):

    def test_no_platform(self):
        record = ParentRecord()
        ref = ParentRecordRef(record)
        with self.assertSkipsReview():
            GTSPPSpeedCheck().parent_record_check(ref)

    @sub_tests([(None, 9), ("5", 4), ("4", 3)])
    def test_bad_platform(self, platform, quality):
        record = ParentRecord()
        record.metadata["CNODCPlatform"] = SingleElement(platform, Quality=quality)
        ref = ParentRecordRef(record)
        with self.assertSkipsReview():
            GTSPPSpeedCheck().parent_record_check(ref)

    # in decimal degrees, seconds, and m s-1
    @sub_tests([
        (1.0, 1.0, 1.01, 1.01, 120, 16.5),
        (1.01, 1.01, 1.0, 1.0, -120, 16.5),  # should work backwards as well
        (45, 1, 45.01, 1.01, 120, 16.5),
        (89, 89, 89, 92, 600, 16.5),
        (89, 89, 89, 92, 355, 16.5),
        (45, 0, 45, 0.2, 956, 16.5),
        (45, 179.9, 45, -179.9, 956, 16.5),
        (45, -179.9, 45, 179.9, 956, 16.5),
        (90, 0, 90, 180, 100, 16.5),
    ], [
        (89, 89, 89, 92, 354, 16.5),
        (45, 0, 45, 0.2, 955, 16.5),
        (45, 179.9, 45, -179.9, 955, 16.5),
        (91, 0, 85, 0, 100, 16.5),  # gives nan
        (0, 0, 95, 0, 100, 16.5),  # gives nan
    ])
    def test_speed_test(self, y1, x1, y2, x2, dt, top_speed):
        with self.assertPassesQC():
            t_start = AwareDateTime(2015, 1, 1, 0, 0, 0, tzinfo="Etc/UTC")
            t_end = t_start + datetime.timedelta(seconds=dt)
            GTSPPSpeedCheck()._run_speed_test((x2, y2, t_end), (x1, y1, t_start), top_speed)

    @sub_tests([
        ('top_speed_knots', 10.2888889),
        ('top_speed_mps', 20),
        ('top_speed_kph', 5.55555556),
        ('top_speed_mph', 8.9408),
        ('top_speed_integer', 20),
        ('top_speed_float', 20),
        ('top_speed_dict_knots', 10.28888889),
        ('top_speed_missing', 16.5),
        ('top_speed_none', 16.5),
        ('top_speed_skip', None),
    ])
    def test_get_speed(self, platform_uuid, expected_speed_mps):
        top_speed = GTSPPSpeedCheck(searcher_cls=TestStationDatabase)._real_get_top_speed(platform_uuid)
        print(expected_speed_mps, top_speed)
        threshold = 0.00001
        if expected_speed_mps is None:
            self.assertIsNone(top_speed)
        else:
            self.assertIsInstance(top_speed, decimal.Decimal | float | int)
            if isinstance(top_speed, decimal.Decimal):
                expected_speed_mps = decimal.Decimal(expected_speed_mps)
                threshold = decimal.Decimal(threshold)
            diff = abs(expected_speed_mps - top_speed)
            self.assertLess(diff, threshold)
