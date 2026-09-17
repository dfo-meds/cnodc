from medsutil.awaretime import AwareDateTime
from medsutil.iso_duration import ISODuration, DurationUnit
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestISODuration(BaseTestCase):

    @sub_tests([
        (0, 0, 0, 0, 0, 5, "PT5S"),
        (0, 0, 0, 0, 5, 0, "PT5M"),
        (0, 0, 0, 5, 0, 0, "PT5H"),
        (0, 0, 5, 0, 0, 0, "P5D"),
        (0, 5, 0, 0, 0, 0, "P5M"),
        (5, 0, 0, 0, 0, 0, "P5Y"),
        (1, 2, 3, 4, 5, 6, "P1Y2M3DT4H5M6S"),
    ])
    def test_isoformat(self, y, m, d, h, mi, s, expected):
        d = ISODuration(years=y, months=m, days=d, hours=h, minutes=mi, seconds=s)
        self.assertEqual(d.isoformat(), expected)

    @sub_tests([
        (0, 0, 0, 0, 0, 1, "s", 1),
        (0, 0, 0, 0, 1, 0, "s", 60),
        (0, 0, 0, 0, 1, 0, "min", 1),
        (0, 0, 0, 1, 0, 0, "s", 3600),
        (0, 0, 0, 1, 0, 0, "h", 1),
        (0, 0, 0, 1, 0, 0, "min", 60),
        (0, 0, 1, 0, 0, 0, "s", 86400),
        (0, 0, 1, 0, 0, 0, "h", 24),
        (0, 0, 1, 1, 1, 1, "s", 86400 + 3600 + 60 + 1),
        (0, 0, 5, 5, 5, 5, "s", 5 * (86400 + 3600 + 60 + 1)),
        (0, 1, 0, 0, 0, 0, "mon", 1),
        (1, 0, 0, 0, 0, 0, "mon", 12),
        (1, 1, 0, 0, 0, 0, "mon", 13),
        (5, 5, 0, 0, 0, 0, "mon", 13 * 5),
        (1, 0, 1, 0, 0, 0, "s", None),
        (1, 0, 0, 1, 0, 0, "s", None),
        (1, 0, 0, 0, 1, 0, "s", None),
        (1, 0, 0, 0, 0, 1, "s", None),
        (0, 1, 1, 0, 0, 0, "s", None),
        (0, 1, 0, 1, 0, 0, "s", None),
        (0, 1, 0, 0, 1, 0, "s", None),
        (0, 1, 0, 0, 0, 1, "s", None),
    ])
    def test_to_duration(self, y, m, d, h, mi, s, output, expected):
        d = ISODuration(years=y, months=m, days=d, hours=h, minutes=mi, seconds=s)
        if expected is None:
            with self.assertRaises(ValueError):
                _ = d.to_duration(DurationUnit(output))
        else:
            self.assertEqual(d.to_duration(DurationUnit(output)), expected)

    @sub_tests([
        (0, 0, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2015-01-01T00:00:00+00:00"),
        (0, 0, 0, 0, 0, 1, "2015-01-01T00:00:00+00:00", "2015-01-01T00:00:01+00:00"),
        (0, 0, 0, 0, 1, 0, "2015-01-01T00:00:00+00:00", "2015-01-01T00:01:00+00:00"),
        (0, 0, 0, 1, 0, 0, "2015-01-01T00:00:00+00:00", "2015-01-01T01:00:00+00:00"),
        (0, 0, 1, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2015-01-02T00:00:00+00:00"),
        (0, 1, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2015-02-01T00:00:00+00:00"),
        (1, 0, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2016-01-01T00:00:00+00:00"),
        (0, 13, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2016-02-01T00:00:00+00:00"),
        (0, -1, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2014-12-01T00:00:00+00:00"),
    ])
    def test_add_to(self, y, m, d, h, mi, s, start, expected):
        d = ISODuration(years=y, months=m, days=d, hours=h, minutes=mi, seconds=s)
        start = AwareDateTime.fromisoformat(start)
        self.assertSameTime(d.add_to(start), AwareDateTime.fromisoformat(expected))

    @sub_tests([
        (0, 0, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2015-01-01T00:00:00+00:00"),
        (0, 0, 0, 0, 0, 1, "2015-01-01T00:00:02+00:00", "2015-01-01T00:00:01+00:00"),
        (0, 0, 0, 0, 1, 0, "2015-01-01T00:02:00+00:00", "2015-01-01T00:01:00+00:00"),
        (0, 0, 0, 1, 0, 0, "2015-01-01T02:00:00+00:00", "2015-01-01T01:00:00+00:00"),
        (0, 0, 1, 0, 0, 0, "2015-01-03T00:00:00+00:00", "2015-01-02T00:00:00+00:00"),
        (0, 1, 0, 0, 0, 0, "2015-03-01T00:00:00+00:00", "2015-02-01T00:00:00+00:00"),
        (1, 0, 0, 0, 0, 0, "2017-01-01T00:00:00+00:00", "2016-01-01T00:00:00+00:00"),
        (0, 13, 0, 0, 0, 0, "2017-03-01T00:00:00+00:00", "2016-02-01T00:00:00+00:00"),
        (0, -1, 0, 0, 0, 0, "2015-01-01T00:00:00+00:00", "2015-02-01T00:00:00+00:00"),
    ])
    def test_subtract_from(self, y, m, d, h, mi, s, start, expected):
        d = ISODuration(years=y, months=m, days=d, hours=h, minutes=mi, seconds=s)
        start = AwareDateTime.fromisoformat(start)
        self.assertSameTime(d.subtract_from(start), AwareDateTime.fromisoformat(expected))

    @sub_tests([
        (1, "s", 1, "s"),
        (1, "s", 1 / 60, "min"),
        (1, "s", 1 / 3600, "h"),
        (1, "s", 1 / 86400, "d"),
        (1, "s", 1 / 604800, "w"),
        (1, "s", None, "mon"),
        (1, "s", None, "a"),
        (1, "min", 60, "s"),
        (1, "min", 1, "min"),
        (1, "min", 1 / 60, "h"),
        (1, "min", 1 / 1440, "d"),
        (1, "min", 1 / 10080, "w"),
        (1, "min", None, "mon"),
        (1, "min", None, "a"),
        (1, "h", 3600, "s"),
        (1, "h", 60, "min"),
        (1, "h", 1, "h"),
        (1, "h", 1 / 24, "d"),
        (1, "h", 1 / (24 * 7), "w"),
        (1, "h", None, "mon"),
        (1, "h", None, "a"),
        (1, "d", 86400, "s"),
        (1, "d", 1440, "min"),
        (1, "d", 24, "h"),
        (1, "d", 1, "d"),
        (1, "d", 1 / 7, "w"),
        (1, "d", None, "mon"),
        (1, "d", None, "a"),
        (1, "w", 604800, "s"),
        (1, "w", 10080, "min"),
        (1, "w", 168, "h"),
        (1, "w", 7, "d"),
        (1, "w", 1, "w"),
        (1, "w", None, "mon"),
        (1, "w", None, "a"),
        (1, "mon", None, "s"),
        (1, "mon", None, "min"),
        (1, "mon", None, "h"),
        (1, "mon", None, "d"),
        (1, "mon", None, "w"),
        (1, "mon", 1, "mon"),
        (1, "mon", 1 / 12, "a"),
        (1, "a", None, "s"),
        (1, "a", None, "min"),
        (1, "a", None, "h"),
        (1, "a", None, "d"),
        (1, "a", None, "w"),
        (1, "a", 12, "mon"),
        (1, "a", 1, "a"),
    ])
    def test_convert_duration_units(self, x, from_unit, expected, to_unit):
        if expected is None:
            with self.assertRaises(ValueError):
                _ = ISODuration.convert_duration_units(x, DurationUnit(from_unit), DurationUnit(to_unit))
        else:
            self.assertEqual(ISODuration.convert_duration_units(x, DurationUnit(from_unit), DurationUnit(to_unit)), expected)

    @sub_tests([
        (1, "s", 1, "s"),
        (1, "s", 1 / 60, "min"),
        (1, "s", 1 / 3600, "h"),
        (1, "s", None, "d"),
        (1, "s", None, "w"),
        (1, "s", None, "mon"),
        (1, "s", None, "a"),
        (1, "min", 60, "s"),
        (1, "min", 1, "min"),
        (1, "min", 1 / 60, "h"),
        (1, "min", None, "d"),
        (1, "min", None, "w"),
        (1, "min", None, "mon"),
        (1, "min", None, "a"),
        (1, "h", 3600, "s"),
        (1, "h", 60, "min"),
        (1, "h", 1, "h"),
        (1, "h", None, "d"),
        (1, "h", None, "w"),
        (1, "h", None, "mon"),
        (1, "h", None, "a"),
        (1, "d", None, "s"),
        (1, "d", None, "min"),
        (1, "d", None, "h"),
        (1, "d", 1, "d"),
        (1, "d", 1 / 7, "w"),
        (1, "d", None, "mon"),
        (1, "d", None, "a"),
        (1, "w", None, "s"),
        (1, "w", None, "min"),
        (1, "w", None, "h"),
        (1, "w", 7, "d"),
        (1, "w", 1, "w"),
        (1, "w", None, "mon"),
        (1, "w", None, "a"),
        (1, "mon", None, "s"),
        (1, "mon", None, "min"),
        (1, "mon", None, "h"),
        (1, "mon", None, "d"),
        (1, "mon", None, "w"),
        (1, "mon", 1, "mon"),
        (1, "mon", 1 / 12, "a"),
        (1, "a", None, "s"),
        (1, "a", None, "min"),
        (1, "a", None, "h"),
        (1, "a", None, "d"),
        (1, "a", None, "w"),
        (1, "a", 12, "mon"),
        (1, "a", 1, "a"),
    ])
    def test_convert_duration_units_calendar(self, x, from_unit, expected, to_unit):
        if expected is None:
            with self.assertRaises(ValueError):
                _ = ISODuration.convert_duration_units(x, DurationUnit(from_unit), DurationUnit(to_unit), use_calendar_days=True)
        else:
            self.assertEqual(ISODuration.convert_duration_units(x, DurationUnit(from_unit), DurationUnit(to_unit), use_calendar_days=True), expected)

    @sub_tests([
        (2, "a", (2, 0, 0, 0, 0, 0)),
        (2, "mon", (0, 2, 0, 0, 0, 0)),
        (2, "w", (0, 0, 14, 0, 0, 0)),
        (2, "d", (0, 0, 2, 0, 0, 0)),
        (2, "h", (0, 0, 0, 2, 0, 0)),
        (2, "min", (0, 0, 0, 0, 2, 0)),
        (2, "s", (0, 0, 0, 0, 0, 2)),
        (2, "", None)
    ])
    def test_from_duration(self, x, from_unit, ymdhms: tuple[int, int, int, int, int, int] | None):
        if ymdhms is None:
            with self.assertRaises(ValueError):
                units = None
                try:
                    units = DurationUnit(from_unit)
                except ValueError:
                    ...
                _ = ISODuration.from_duration(x, units)
        else:
            self.assertEqual(
                ISODuration.from_duration(x, DurationUnit(from_unit)),
                ISODuration(
                    years=ymdhms[0],
                    months=ymdhms[1],
                    days=ymdhms[2],
                    hours=ymdhms[3],
                    minutes=ymdhms[4],
                    seconds=ymdhms[5]
                )
            )