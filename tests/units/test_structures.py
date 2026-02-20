import decimal
import unittest as ut
import cnodc.units.structures as uns
from cnodc.util import CNODCError

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
        with self.assertRaises(CNODCError):
            LOG.get_unit_info(None)

    def test_bad_offset_unit_info(self):
        with self.assertRaises(CNODCError):
            BAD_OFFSET.get_unit_info(None)

    def test_bad_product(self):
        with self.assertRaises(CNODCError):
            LINEAR_FUNCTION1.product(SIMPLE_UNIT)