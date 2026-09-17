import decimal
import math

import medsutil.math as amath
import medsutil.math._common as common
from medsutil.math import ScienceNumber, NumberString
from medsutil.math.types import MathTypeError

from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestCommonMath(BaseTestCase):

    @sub_tests([
        (0, True),
        (1, False),
        (0.0, True),
        (decimal.Decimal(0), True),
        (decimal.Decimal(1), False),
        (amath.NumberString("0"), True),
        (amath.NumberString("0.0"), True),
        (amath.NumberString("1.0"), False),
    ])
    def test_is_zero(self, value, expected):
        self.assertIs(common.is_zero(value), expected)

    @sub_tests([
        (0, 1, False),
        (1, 0, True),
        (1.0, 0, True),
        (0, 1.0, False),
        (decimal.Decimal(0), 1, False),
        (decimal.Decimal(1), 0, True),
        (decimal.Decimal(1), decimal.Decimal(0), True),
        (amath.NumberString("0"), 1, False),
        (amath.NumberString("2"), amath.NumberString("1"), True),
    ])
    def test_gt_and_lt(self, x, y, is_gt):
        self.assertIs(common.gt(x, y), is_gt)
        self.assertIs(common.lt(x, y), not is_gt)

    @sub_tests([
        (decimal.Decimal("1"), decimal.Decimal("2.71828")),
        (1.0, 2.71828),
        (amath.NumberString("1"), 2.71828),
    ])
    def test_exp(self, x, expected):
        self.assertAlmostEqual(common.exp(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3"), decimal.Decimal("1.09861")),
        (3.0, 1.09861),
        (amath.NumberString("3.0"), 1.09861),
    ])
    def test_ln(self, x, expected):
        self.assertAlmostEqual(common.ln(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3"), decimal.Decimal("0.47712")),
        (3.0, 0.47712),
        (amath.NumberString("3.0"), 0.47712),
    ])
    def test_log10(self, x, expected):
        self.assertAlmostEqual(common.log10(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3"), decimal.Decimal("1.58496")),
        (3.0, 1.58496),
        (amath.NumberString("3.0"), 1.58496),
    ])
    def test_log2(self, x, expected):
        self.assertAlmostEqual(common.log2(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3.4"), decimal.Decimal("3")),
        (3.4, 3.0),
        (amath.NumberString("3.4"), 3.0),
        (decimal.Decimal("-3.4"), decimal.Decimal("-4")),
        (-3.4, -4.0),
        (amath.NumberString("-3.4"), -4.0),
    ])
    def test_floor(self, x, expected):
        self.assertAlmostEqual(common.floor(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3.4"), decimal.Decimal("4")),
        (3.4, 4.0),
        (amath.NumberString("3.4"), 4.0),
        (decimal.Decimal("-3.4"), decimal.Decimal("-3")),
        (-3.4, -3.0),
        (amath.NumberString("-3.4"), -3.0),
    ])
    def test_ceiling(self, x, expected):
        self.assertAlmostEqual(common.ceil(x), expected, places=5)

    @sub_tests([
        (decimal.Decimal("3.4"), 3),
        (3.4, 3),
        (amath.NumberString("3.4"), 3),
        (decimal.Decimal("-3.4"), -3),
        (-3.4, -3),
        (amath.NumberString("-3.4"), -3),
        (decimal.Decimal("3.6"), 4),
        (3.6, 4),
        (amath.NumberString("3.6"), 4),
        (decimal.Decimal("-3.6"), -4),
        (-3.6, -4),
        (amath.NumberString("-3.6"), -4),
    ])
    def test_round_to_int(self, x, expected):
        self.assertEqual(common.round_to_int(x), expected)

    @sub_tests([
        (decimal.Decimal("3.04"), decimal.Decimal("3.0")),
        (3.04, 3.0),
        (amath.NumberString("3.04"), 3.0),
        (decimal.Decimal("-3.04"), decimal.Decimal("-3.0")),
        (-3.04, -3.0),
        (amath.NumberString("-3.04"), -3.0),
        (decimal.Decimal("3.06"), decimal.Decimal("3.1")),
        (3.06, 3.1),
        (amath.NumberString("3.06"), 3.1),
        (decimal.Decimal("-3.06"), decimal.Decimal("-3.1")),
        (-3.06, -3.1),
        (amath.NumberString("-3.06"), -3.1),
    ])
    def test_round_to_place(self, x, expected):
        self.assertEqual(common.round_to_place(x, 1), expected)

    @sub_tests([
        (decimal.Decimal("inf"), True),
        (math.inf, True),
        (1, False),
        (amath.NumberString("5"), False),
        (decimal.Decimal("5"), False)
    ])
    def test_is_infinity(self, x, expected):
        self.assertIs(common.is_infinity(x), expected)

    def test_not_supported_raises(self):
        with self.assertRaises(MathTypeError):
            _ = common.nominal_value(self)

    def test_convert_from_none(self):
        self.assertIsNone(common.convert(None))

    def test_convert_placeholder(self):
        self.assertIs(common.convert(amath.Placeholder()), amath.Placeholder())

    def test_convert_raises(self):
        with self.assertRaises(MathTypeError):
            _ = common.convert(self)

    def test_match_units(self):
        x = ScienceNumber(5, units="m")
        y = ScienceNumber(10, units="km")
        x2, y2 = common.match_units(x, y)
        self.assertIsInstance(x2, ScienceNumber)
        self.assertIsInstance(y2, ScienceNumber)
        self.assertEqual(x2.units, "m")
        self.assertEqual(y2.units, "m")
        self.assertEqual(x2.nominal_value, 5)
        self.assertEqual(y2.nominal_value, 10000)

    def test_match_convert_scinum_decimal(self):
        x = ScienceNumber(decimal.Decimal(5))
        y = NumberString("5")
        x2, y2 = common.match_convert(x, y)
        self.assertIsInstance(x2, decimal.Decimal)
        self.assertIsInstance(y2, decimal.Decimal)

    def test_sort_by_type(self):
        test_scinum = ScienceNumber(5, units="km")
        test_str = amath.NumberString("5")
        test_float = 5.0
        test_int = 5
        test_decimal = decimal.Decimal("5")
        sn, dc, ns, flint = common.separate_by_type([
            test_scinum, test_str, test_float, test_int, test_decimal
        ])
        self.assertIn(test_scinum, sn)
        self.assertIn(test_str, ns)
        self.assertIn(test_float, flint)
        self.assertIn(test_int, flint)
        self.assertIn(test_decimal, dc)
