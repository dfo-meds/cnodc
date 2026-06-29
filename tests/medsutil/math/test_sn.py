from medsutil.math import ScienceNumber
from tests.helpers.base_test_case import BaseTestCase


class TestScienceNumber(BaseTestCase):

    def test_override_stdev(self):
        a = ScienceNumber(100, 0.5, min_std_dev=2.0)
        self.assertAlmostEqual(a.std_dev, 2.0)

    def test_compatible(self):
        a = ScienceNumber.round_with_error(12.123, 2)
        b = ScienceNumber.round_with_error(12.123, 1)
        self.assertTrue(a.is_compatible(b))

    def test_add_float_with_error(self):
        a = ScienceNumber(5.1, 0.1)
        b = ScienceNumber(11.52, 0.01)
        c = a + b
        self.assertAlmostEqual(c.nominal_value, 5.1 + 11.52)
        self.assertAlmostEqual(c.std_dev, 0.100498756)

    def test_sub_float_with_error(self):
        a = ScienceNumber(5.1, 0.1)
        b = ScienceNumber(11.52, 0.01)
        c = b-a
        self.assertAlmostEqual(c.nominal_value, 11.52 - 5.1)
        self.assertAlmostEqual(c.std_dev, 0.100498756)

    def test_mul_float_with_error(self):
        a = ScienceNumber(5.1, 0.1)
        b = ScienceNumber(11.52, 0.01)
        c = b * a
        self.assertAlmostEqual(c.nominal_value, 11.52 * 5.1)
        self.assertAlmostEqual(c.std_dev, 1.153128354)

    def test_div_float_with_error(self):
        a = ScienceNumber(5.1, 0.1)
        b = ScienceNumber(11.52, 0.01)
        c = b / a
        self.assertAlmostEqual(c.nominal_value, 11.52 / 5.1)
        self.assertAlmostEqual(c.std_dev, 0.044334039)