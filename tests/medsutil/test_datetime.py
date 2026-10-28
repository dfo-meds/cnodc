import zoneinfo
import datetime

from medsutil.awaretime import AwareDateTime, ScienceDateTime
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestDateTime(BaseTestCase):

    @sub_tests([
        (ScienceDateTime(2023, 1, 1, 5, 0, 0, uncertainty=datetime.timedelta(minutes=30)), AwareDateTime(2023, 1, 1, 4, 54)),
        (ScienceDateTime(2023, 1, 1, 5, 0, 0, uncertainty=datetime.timedelta(minutes=30)), AwareDateTime(2023, 1, 1, 5, 24)),
    ], [
        (ScienceDateTime(2023, 1, 1, 5, 0, 0, uncertainty=datetime.timedelta(minutes=30)), AwareDateTime(2023, 1, 1, 4, 24)),
        (ScienceDateTime(2023, 1, 1, 5, 0, 0, uncertainty=datetime.timedelta(minutes=30)), AwareDateTime(2023, 1, 1, 5, 54)),
    ])
    def test_is_compatible(self, sdt: ScienceDateTime, od: AwareDateTime):
        self.assertTrue(sdt.is_compatible(od))

    def test_range_none(self):
        x = ScienceDateTime(2015, 1, 2, 3, 4, 5, uncertainty=None)
        min_x, max_x = x.range()
        self.assertIs(min_x, max_x)

    def test_copy(self):
        x = AwareDateTime(2015, 1, 2, 3, 4, 5)
        y = x.__copy__()
        self.assertIsNot(x, y)
        self.assertEqual(x, y)

    def test_deep_copy(self):
        x = AwareDateTime(2015, 1, 2, 3, 4, 5)
        y = x.__deepcopy__()
        self.assertIsNot(x, y)
        self.assertEqual(x, y)

    def test_datetime_from_none(self):
        x = AwareDateTime.from_datetime(None)
        self.assertIsNone(x)

    def test_datetime_from_str(self):
        x = AwareDateTime.from_datetime("2015-01-02T03:04:05+00:00")
        self.assertEqual(x, AwareDateTime(2015, 1, 2, 3, 4, 5, tzinfo="Etc/UTC"))

    def test_datetime_from_date(self):
        x = AwareDateTime.from_datetime(datetime.date(2015, 1, 2), tzinfo="Etc/UTC")
        self.assertEqual(x, AwareDateTime(2015, 1, 2, 0, 0, 0, tzinfo="Etc/UTC"))

    def test_now(self):
        n = AwareDateTime.now()
        boring_now = datetime.datetime.now()
        self.assertIsNotNone(n.tzinfo)
        self.assertEqual(n.day, boring_now.day)
        self.assertEqual(n.hour, boring_now.hour)
        self.assertEqual(n.minute, boring_now.minute)
        self.assertEqual(n.second, boring_now.second)

    def test_utc_from_string(self):
        n = AwareDateTime.utcstrptime('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('Etc/UTC'))

    def test_from_string_with_timedelta(self):
        n = AwareDateTime.strptime('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S', default_tz=datetime.timedelta(hours=-7))
        self.assertSameTime(n, datetime.datetime(2015, 1, 2,3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=-7))))

    def test_from_string(self):
        n = AwareDateTime.strptime('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('America/Vancouver'))

    def test_from_string_no_override(self):
        n = AwareDateTime.strptime('2015-01-02 03:04:05+01:30', '%Y-%m-%d %H:%M:%S%z', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertEqual(n.tzinfo, datetime.timezone(datetime.timedelta(seconds=5400)))

    def test_utc_from_isoformat(self):
        n = AwareDateTime.utcfromisoformat('2015-01-02T03:04:05')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('Etc/UTC'))

    def test_from_isoformat(self):
        n = AwareDateTime.fromisoformat('2015-01-02T03:04:05', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('America/Vancouver'))

    def test_from_isformat_no_override(self):
        n = AwareDateTime.fromisoformat('2015-01-02T03:04:05+01:30', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertEqual(n.tzinfo, datetime.timezone(datetime.timedelta(seconds=5400)))

    def test_from_timestamp(self):
        n = AwareDateTime.utcfromtimestamp(0)
        self.assertEqual(n, datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc))

    def test_today(self):
        dt = AwareDateTime.today()
        self.assertIsNotNone(dt.tzinfo)

    def test_time(self):
        dt = AwareDateTime.today()
        self.assertIsNotNone(dt.time().tzinfo)

    def test_asutc(self):
        dt = AwareDateTime.today()
        dt_utc = dt.asutc()
        self.assertEqual(dt_utc.tzinfo.utcoffset(None).total_seconds(), 0)

    def test_from_awaretime(self):
        dt = AwareDateTime.today()
        dt2 = AwareDateTime.from_datetime(dt)
        self.assertIs(dt, dt2)
