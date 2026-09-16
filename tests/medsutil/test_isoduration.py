from medsutil.iso_duration import ISODuration
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