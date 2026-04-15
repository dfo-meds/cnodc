import decimal
import unittest as ut
import medsutil.units.structures as uns
from medsutil.exceptions import CodedError

INTEGER = uns.Integer("5")
SIMPLE_UNIT = uns.SimpleUnit("K")
REAL = uns.Real("273.15")
LOG = uns.Log(SIMPLE_UNIT, INTEGER)
OFFSET = uns.Offset(SIMPLE_UNIT, REAL)
BAD_OFFSET = uns.Offset(REAL, SIMPLE_UNIT)
QUOTIENT = uns.Quotient(INTEGER, REAL)
PRODUCT = uns.Product(INTEGER, REAL)
EXPONENT = uns.Exponent(SIMPLE_UNIT, INTEGER)
LINEAR_FUNCTION1 = uns.LinearFunction(decimal.Decimal("5"), decimal.Decimal("10"))
LINEAR_FUNCTION2 = uns.LinearFunction(decimal.Decimal("5"), decimal.Decimal("-10"))
LINEAR_FUNCTION3 = uns.LinearFunction(decimal.Decimal("5"), decimal.Decimal("0"))


class TestStructures(ut.TestCase):

    def test_bad_comparisons(self):
        self.assertNotEqual(INTEGER, SIMPLE_UNIT)
        self.assertNotEqual(SIMPLE_UNIT, REAL)
        self.assertNotEqual(LOG, SIMPLE_UNIT)
        self.assertNotEqual(OFFSET, SIMPLE_UNIT)
        self.assertNotEqual(QUOTIENT, SIMPLE_UNIT)
        self.assertNotEqual(PRODUCT, SIMPLE_UNIT)
        self.assertNotEqual(EXPONENT, SIMPLE_UNIT)

    def test_representations(self):
        self.assertEqual(repr(INTEGER), "5")
        self.assertEqual(repr(REAL), "273.15")
        self.assertEqual(repr(SIMPLE_UNIT), "[K]")
        self.assertEqual(repr(LOG), "(log base 5 of [K])")
        self.assertEqual(repr(OFFSET), "([K] @ 273.15)")
        self.assertEqual(repr(QUOTIENT), "(5 / 273.15)")
        self.assertEqual(repr(PRODUCT), "(5 * 273.15)")
        self.assertEqual(repr(EXPONENT), "([K] ** 5)")
        self.assertEqual(repr(LINEAR_FUNCTION1), "5x+10")
        self.assertEqual(repr(LINEAR_FUNCTION2), "5x-10")
        self.assertEqual(repr(LINEAR_FUNCTION3), "5x")

    def test_simple_linear_function_ops(self):
        self.assertIs(LINEAR_FUNCTION1, LINEAR_FUNCTION1.power(decimal.Decimal("1")))
        self.assertIs(LINEAR_FUNCTION1, LINEAR_FUNCTION1.shift(decimal.Decimal("0")))

    def test_bad_log_unit_info(self):
        with self.assertRaises(CodedError):
            LOG.get_unit_info(None)

    def test_bad_offset_unit_info(self):
        with self.assertRaises(CodedError):
            BAD_OFFSET.get_unit_info(None)

    def test_bad_product(self):
        with self.assertRaises(CodedError):
            LINEAR_FUNCTION1.product(SIMPLE_UNIT)

    def test_standardize(self):
        p = uns.Product(uns.Integer("5"), uns.Integer("6"))
        self.assertEqual(uns.Real("30"), p.standardize())

    def test_standardize_exp(self):
        p = uns.Product(uns.Exponent(uns.Integer("5"), uns.Integer("2")), uns.Integer("6"))
        self.assertEqual(uns.Real(f"150"), p.standardize())

    def test_standardize_exp_rev(self):
        p = uns.Product(uns.Integer("6"), uns.Exponent(uns.Integer("5"), uns.Integer("2")))
        self.assertEqual(uns.Real(f"150"), p.standardize())

    def test_standardize_long(self):
        p = uns.Product(uns.Integer("6"), uns.Product(uns.Integer("10"), uns.Product(uns.Integer("5"), uns.SimpleUnit("km"))))
        self.assertEqual(uns.Product(uns.Real(f"300"), uns.SimpleUnit("km")), p.standardize())

    def test_standardize_exponents(self):
        self.assertEqual(uns.Real("9"), uns.Exponent(uns.Integer("3"), uns.Integer("2")).standardize())
        self.assertEqual(uns.SimpleUnit("kg"), uns.Exponent(uns.SimpleUnit("kg"), uns.Integer("1")).standardize())
        self.assertEqual(uns.Exponent(uns.SimpleUnit("kg"), uns.Integer("6")), uns.Exponent(uns.Exponent(uns.SimpleUnit("kg"), uns.Integer("2")), uns.Integer("3")).standardize())
        self.assertEqual(
            uns.Product(uns.Exponent(uns.SimpleUnit("kg"), uns.Integer("2")), uns.Exponent(uns.SimpleUnit("m"), uns.Integer("2"))),
            uns.Exponent(uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m")), uns.Integer("2")).standardize()
        )
        self.assertEqual(
            uns.Product(uns.Exponent(uns.SimpleUnit("kg"), uns.Integer("2")),
                        uns.Exponent(uns.SimpleUnit("m"), uns.Integer("-2"))),
            uns.Exponent(uns.Quotient(uns.SimpleUnit("kg"), uns.SimpleUnit("m")), uns.Integer("2")).standardize()
        )
