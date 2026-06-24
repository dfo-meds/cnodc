from medsutil.ocproc2 import MultiElement, SingleElement, ParentRecord, QCResult
from pipeman.programs.qc.platform import NODBPlatformCheck
from tests.helpers.base_test_case import sub_tests
from tests.helpers.qc_check_base import QCCheckerTestCase
from tests.helpers.mock_station_db import TestStationDatabase


class TestNODBPlatformCheck(QCCheckerTestCase):

    @sub_tests([
        ["assigned_platform", None, None, None, None, None, "assigned_platform"],
        [None, None, "12345", None, None, None, "wmo_12345"],
        [None, None, None, "SHIP", None, None, "id_ship"],
        [None, None, None, None, "ShippyMcShip", None, "name_shippymcship"],
        [None, None, None, None, None, "123456", "wigos_123456"],
        [None, "2020-01-02T03:04:05+00:00", "23456", None, None, None, "wmo_23456_2020"],
        [None, "2024-01-02T03:04:05+00:00", "23456", None, None, None, "wmo_23456_2024"],
        [None, None, "1200345", None, None, None, "wmo_12345"],
    ], [
        [MultiElement([SingleElement("12345"), SingleElement("23456")]), None, None, None, None, None, None],
        ["unassigned_platform", None, None, None, None, None, None],
        [None, None, None, None, None, None, None],
        [None, None, "123459", None, None, None, None],
        [None, None, None, "SHIP9", None, None, None],
        [None, None, None, None, "ShippyMcShip9", None, None],
        [None, None, None, None, None, "1234569", None],
        [None, "2023-01-02T03:04:05+00:00", "23456", None, None, None, None],
        [None, None, "12346", None, None, None, None],
        [None, None, "23456", None, None, None, ["wmo_23456_2020", "wmo_23456_2024"]],
        [None, None, "12345", "SHIP", None, None, ["wmo_12345", "id_ship"]],
    ])
    def test_platform_record_exists(self, cnodc_platform, record_time, wmo_id, platform_id, platform_name, wigos_id, expected_platform_uuid):
        record = ParentRecord()
        if cnodc_platform is not None:
            record.metadata['CNODCPlatform'] = cnodc_platform
        if wmo_id is not None:
            record.metadata['WMOID'] = wmo_id
        if wigos_id is not None:
            record.metadata['WIGOSID'] = wigos_id
        if platform_id is not None:
            record.metadata['PlatformID'] = platform_id
        if platform_name is not None:
            record.metadata['PlatformName'] = platform_name
        if record_time is not None:
            record.coordinates['Time'] = record_time

        checker = NODBPlatformCheck(
            searcher_cls=TestStationDatabase
        )
        qc_result = checker.run_record_check(record, None)
        if isinstance(expected_platform_uuid, str):
            self.assertEqual(record.metadata['CNODCPlatform'].value, expected_platform_uuid)
        elif expected_platform_uuid is None:
            self.assertIsNone(record.metadata['CNODCPlatform'].value)
        else:
            self.assertEqual(record.metadata.best("CNODCPlatformCandidates"), expected_platform_uuid)
        self.assertIs(qc_result.result, QCResult.PASS)
        self.assertEqual(record.metadata['CNODCPlatform'].quality, 1)
