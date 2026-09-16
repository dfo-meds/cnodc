from medsutil.awaretime import AwareDateTime
from medsutil.cron import CompiledCron
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestCron(BaseTestCase):

    @sub_tests([
        ("@annually", AwareDateTime(2015, 2, 1, 0, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
        ("@annually", AwareDateTime(2015, 1, 2, 0, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
        ("@annually", AwareDateTime(2015, 1, 1, 1, 0, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
        ("@annually", AwareDateTime(2015, 1, 1, 0, 1, 0, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
        ("@annually", AwareDateTime(2015, 1, 1, 0, 0, 1, 254), AwareDateTime(2016, 1, 1, 0, 0, 0, 0)),
        ("@yearly", AwareDateTime(2014, 1, 5, 9, 2, 1, 59), AwareDateTime(2015, 1, 1, 0, 0, 0, 0)),
        ((0, 0, 1, 1, "*"), AwareDateTime(2014, 12, 31, 23, 59, 59, 59301), AwareDateTime(2015, 1, 1, 0, 0, 0, 0)),
        ("0 0 * * 2", AwareDateTime(2015, 1, 5, 0, 0, 0, 0), AwareDateTime(2015, 1, 6, 0, 0, 0, 0)),
        ("0 0 * * tue", AwareDateTime(2015, 1, 5, 0, 0, 0, 0), AwareDateTime(2015, 1, 6, 0, 0, 0, 0)),
        ("0 0 * * 2", AwareDateTime(2015, 1, 6, 0, 0, 1, 0), AwareDateTime(2015, 1, 13, 0, 0, 0, 0)),
        ("0 0 * * 2", AwareDateTime(2015, 1, 7, 0, 0, 0, 0), AwareDateTime(2015, 1, 13, 0, 0, 0, 0)),
        ("0 0 * 2 *", AwareDateTime(2015, 12, 7, 0, 0, 0, 0), AwareDateTime(2016, 2, 1, 0, 0, 0, 0)),
        ("0 0 * feb *", AwareDateTime(2015, 12, 7, 0, 0, 0, 0), AwareDateTime(2016, 2, 1, 0, 0, 0, 0)),
        ("0 0,2 * * *", AwareDateTime(2015, 1, 1, 1, 0, 0), AwareDateTime(2015, 1, 1, 2, 0, 0, 0)),
        ("0 */2 * * *", AwareDateTime(2015, 1, 1, 1, 0, 0), AwareDateTime(2015, 1, 1, 2, 0, 0, 0)),
        ("0 0-2 * * *", AwareDateTime(2015, 1, 1, 1, 2, 0), AwareDateTime(2015, 1, 1, 2, 0, 0, 0)),
    ])
    def test_cron_next_execution(self, cron_spec, current_time, expected_result):
        cron = CompiledCron(cron_spec)
        self.assertSameTime(expected_result, cron.next_execution(current_time))

    @sub_tests([
        ("* * * *",),
        ("* * * * * *",),
        (("*", "*", "*", "*",),),
        (("*", "*", "*", "*", "*", "*"),),
        (("*/-2", "*", "*", "*", "*"),),
        (("5h32", "*", "*", "*", "*"),),
        ((-1, "*", "*", "*", "*"),),
        ((60, "*", "*", "*", "*"),),
        (("*", -1, "*", "*", "*"),),
        (("*", 24, "*", "*", "*"),),
        (("*", "*", -1, "*", "*"),),
        (("*", "*", 32, "*", "*"),),
        (("*", "*", "*", -1, "*"),),
        (("*", "*", "*", 13, "*"),),
        (("*", "*", "*", "foo", "*"),),
        (("*", "*", "*", "*", -1),),
        (("*", "*", "*", "*", 8),),
        (("*", "*", "*", "*", "foo"),),
    ])
    def test_cron_bad_format(self, cron_spec):
        with self.assertRaises(ValueError):
            cron = CompiledCron(cron_spec)

    def test_cron_same_hash(self):
        cron1 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        cron2 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        self.assertEqual(cron1._cron_config, cron2._cron_config)

    def test_cron_different_hash(self):
        cron1 = CompiledCron(("H", "H", "*", "*", 6), hash_key="five")
        cron2 = CompiledCron(("H", "H", "*", "*", 6), hash_key="six")
        self.assertNotEqual(cron1._cron_config, cron2._cron_config)