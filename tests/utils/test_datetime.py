import zoneinfo
import datetime

from cnodc.util.awaretime import utc_now, now, utc_from_string, from_string, from_isoformat, utc_from_isoformat, \
    from_timestamp, AwareDateTime, awaretime
from helpers.base_test_case import BaseTestCase


class TestDateTime(BaseTestCase):

    def test_now_utc(self):
        n = utc_now()
        self.assertIs(n.tzinfo, datetime.timezone.utc)

    def test_now(self):
        n = now()
        boring_now = datetime.datetime.now()
        self.assertIsNotNone(n.tzinfo)
        self.assertEqual(n.day, boring_now.day)
        self.assertEqual(n.hour, boring_now.hour)
        self.assertEqual(n.minute, boring_now.minute)
        self.assertEqual(n.second, boring_now.second)

    def test_utc_from_string(self):
        n = utc_from_string('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('Etc/UTC'))

    def test_from_string_with_timedelta(self):
        n = from_string('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S', default_tz=datetime.timedelta(hours=-7))
        self.assertSameTime(n, datetime.datetime(2015, 1, 2,3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=-7))))

    def test_from_string(self):
        n = from_string('2015-01-02 03:04:05', '%Y-%m-%d %H:%M:%S', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('America/Vancouver'))

    def test_from_string_no_override(self):
        n = from_string('2015-01-02 03:04:05+01:30', '%Y-%m-%d %H:%M:%S%z', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertEqual(n.tzinfo, datetime.timezone(datetime.timedelta(seconds=5400)))

    def test_utc_from_isoformat(self):
        n = utc_from_isoformat('2015-01-02T03:04:05')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('Etc/UTC'))

    def test_from_isoformat(self):
        n = from_isoformat('2015-01-02T03:04:05', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertIs(n.tzinfo, zoneinfo.ZoneInfo('America/Vancouver'))

    def test_from_isformat_no_override(self):
        n = from_isoformat('2015-01-02T03:04:05+01:30', 'America/Vancouver')
        self.assertEqual(n.day, 2)
        self.assertEqual(n.hour, 3)
        self.assertEqual(n.minute, 4)
        self.assertEqual(n.second, 5)
        self.assertEqual(n.tzinfo, datetime.timezone(datetime.timedelta(seconds=5400)))

    def test_from_timestamp(self):
        n = from_timestamp(0)
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

    def test_awaretime(self):
        dt = awaretime(2015, 1, 2)
        self.assertIsInstance(dt, AwareDateTime)
        self.assertIsNotNone(dt.tzinfo)