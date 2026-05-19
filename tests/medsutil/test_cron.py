from medsutil.awaretime import AwareDateTime
from medsutil.cron import CRON_SPEC, CompiledCron
from tests.helpers.base_test_case import BaseTestCase


TEST_CASES: list[tuple[
    CRON_SPEC,
    AwareDateTime,
    AwareDateTime
]] = [
    ("@annually", AwareDateTime(2015, 2, 1, 0, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
    ("@annually", AwareDateTime(2015, 1, 2, 0, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
    ("@annually", AwareDateTime(2015, 1, 1, 1, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
    ("@annually", AwareDateTime(2015, 1, 1, 0, 1, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
    ("@annually", AwareDateTime(2015, 1, 1, 0, 0, 1, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
    ("@yearly", AwareDateTime(2014, 1, 5, 9, 2, 1, 59), AwareDateTime(2015, 1, 1, 0, 0, 0, 0)),
    ((0, 0, 1, 1, "*"), AwareDateTime(2014, 12, 31, 23, 59, 59, 59301), AwareDateTime(2015, 1, 1, 0, 0, 0, 0)),
    # TODO: more test cases!
]


class TestCron(BaseTestCase):

    def test_cron_next_execution(self):
        for cron_spec, current_time, expected_result in TEST_CASES:
            with self.subTest(cron_spec=cron_spec, current_time=current_time.isoformat(), expected_result=expected_result.isoformat()):
                cron = CompiledCron(cron_spec)
                self.assertSameTime(expected_result, cron.next_execution(current_time))

    def test_cron_same_hash(self):
        cron1 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        cron2 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        self.assertEqual(cron1._cron_config, cron2._cron_config)

    def test_cron_different_hash(self):
        cron1 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        cron2 = CompiledCron(("H", "H", "*", "*", 6), hash_key="six")
        self.assertNotEqual(cron1._cron_config, cron2._cron_config)