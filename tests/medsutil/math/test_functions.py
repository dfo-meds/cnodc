import decimal
import math

from medsutil.math import _functions as functions, NumberString, ScienceNumber
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestFunctions(BaseTestCase):

    @sub_tests([
        (5, 5, 7.07106781),
        (5.0, 5.0, 7.07106781),
        (NumberString("5.0"), 5, 7.07106781),
        (5, NumberString("5.0"), 7.07106781),
        (decimal.Decimal("5"), 5, decimal.Decimal("7.07106781")),
        (decimal.Decimal("5"), NumberString("5"), decimal.Decimal("7.07106781")),
    ])
    def test_add_in_quad(self, x, y, expected):
        self.assertAlmostEqual(functions.add_in_quadrature(x, y), expected)

    def test_add_scinum_in_quad(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(5, std_dev=2, units="m")
        result = functions.add_in_quadrature(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 7.07106781)
        self.assertEqual(result.units, "m")
        self.assertAlmostEqual(result.std_dev, 2)

    def test_add_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(5, std_dev=2, units="m")
        result = functions.add(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 10)
        self.assertEqual(result.units, "m")
        self.assertAlmostEqual(result.std_dev, 2.82842712)

    def test_sub_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(5, std_dev=2, units="m")
        result = functions.sub(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 0)
        self.assertEqual(result.units, "m")
        self.assertAlmostEqual(result.std_dev, 2.82842712)

    def test_mul_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(5, std_dev=2, units="m")
        result = functions.mul(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 25)
        self.assertEqual(result.units, "((m)^2)")
        self.assertAlmostEqual(result.std_dev, 14.142135624)

    def test_div_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(5, std_dev=2, units="m")
        result = functions.div(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 1)
        self.assertEqual(result.units, "")
        self.assertAlmostEqual(result.std_dev, 0.5656854249)

    def test_neg_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        result = functions.neg(x)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, -5)
        self.assertEqual(result.units, "m")
        self.assertAlmostEqual(result.std_dev, 2)

    def test_sqrt_scinum(self):
        x = ScienceNumber(25, std_dev=2, units="m^2")
        result = functions.sqrt(x)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 5)
        self.assertEqual(result.units, "((m^2)^0.5)")
        self.assertAlmostEqual(result.std_dev, 0.2)

    def test_power_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = ScienceNumber(1.5, std_dev=0.05)
        result = functions.pow_(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 11.1803398875)
        self.assertEqual(result.units, "((m)^1.5)")
        self.assertAlmostEqual(result.std_dev, 6.7682690363)

    def test_modulo_scinum(self):
        x = ScienceNumber(5, std_dev=2, units="m")
        y = 2
        result = functions.modulo(x, y)
        self.assertIsInstance(result, ScienceNumber)
        self.assertAlmostEqual(result.nominal_value, 1)
        self.assertIsNone(result.units)
        self.assertAlmostEqual(result.std_dev, 2)


    @sub_tests([
        (1, True),
        (100, True),
        (1e4, True),
        (1e-3, False),
        (0, True),
        (decimal.Decimal(0), True),
        (decimal.Decimal("0.5"), False),
        (decimal.Decimal("1.0"), True),
        (NumberString("0"), True),
        (NumberString("-203"), True),
        (NumberString("-0.1"), False),
    ])
    def test_is_exact_int(self, val, expected):
        self.assertIs(functions.is_exact_int(val), expected)


    @sub_tests([
        (5, -1, -5),
        (-5, 1, 5),
        (5, 1, 5),
        (-5, -1, -5),
        (5, 0, 5),
        (-5, 0, -5),
        (NumberString("5"), NumberString("-1"), -5),
        (decimal.Decimal("5"), -1, decimal.Decimal("-5")),
    ])
    def test_copy_sign(self, take_value, take_sign, expected):
        self.assertEqual(expected, functions.copy_sign(take_value, take_sign))

    @sub_tests([
        (5, 1,),
        (1, 1,),
        (0, 0,),
        (-1, -1),
        (-5, -1),
        (math.nan, None),
    ])
    def test_sign(self, value, sign):
        if sign is None:
            self.assertIsNone(functions.sign(value))
        else:
            self.assertEqual(sign, functions.sign(value))

    @sub_tests([
        (5, 5, True),
        (5.0, 5, True),
        (-5.0, 5, False),
        (NumberString("5"), 5, True),
        (ScienceNumber(5, units="m"), ScienceNumber(500, units="cm"), True),
        (ScienceNumber(5, units="m"), ScienceNumber(5, units="cm"), False),
    ])
    def test_eq(self, x, y, result):
        self.assertIs(functions.eq(x, y), result)

    @sub_tests([
        (1, 3, 5, True),
        (1, 1, 5, True),
        (1, 5, 5, True),
        (1, 5, 5, False, False, False),
        (1, 1, 5, False, False, False),
    ])
    def test_between(self, lower, x, upper, result, include_lower=True, include_upper=True):
        self.assertIs(
            functions.between(lower, x, upper, include_lower_bound=include_lower, include_upper_bound=include_upper),
            result
        )

    def test_sum_scinum(self):
        args = [
            ScienceNumber(k, std_dev=1)
            for k in range(1, 11)
        ]
        s = functions.sum_(*args)
        self.assertIsInstance(s, ScienceNumber)
        self.assertAlmostEqual(s.nominal_value, 55)
        self.assertAlmostEqual(s.std_dev, 3.1622776602)

    def test_product_scinum(self):
        args = [
            ScienceNumber(k, std_dev=1)
            for k in range(1, 11)
        ]
        s = functions.product(*args)
        self.assertIsInstance(s, ScienceNumber)
        self.assertAlmostEqual(s.nominal_value, 3628800)
        self.assertAlmostEqual(s.std_dev, 4517481.053861764)

    def test_min(self):
        args = [
            5,
            ScienceNumber(6),
            NumberString("7"),
            decimal.Decimal("8"),
        ]
        self.assertEqual(functions.min_(*args), 5)

    def test_max(self):
        args = [
            5,
            ScienceNumber(6),
            NumberString("7"),
            decimal.Decimal("8"),
        ]
        self.assertEqual(functions.max_(*args), decimal.Decimal("8"))


    def test_sig_figs(self):
        self.assertEqual(2, functions.significant_digits(0.005))

    def test_sig_figs_neg(self):
        self.assertIsNone(functions.significant_digits(-0.005))

