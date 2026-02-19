import decimal
import itertools
import unittest as ut
import cnodc.units.units as un
import typing as t

from cnodc.units.units import Integer
from cnodc.util import CNODCError


class TestUnitParsing(ut.TestCase):

    def test_simple_unit(self):
        self.assertEqual(un.parse_unit_string("m"), un.SimpleUnit("m"))

    def test_implicit_squared_unit(self):
        self.assertEqual(un.parse_unit_string("m2"), un.Exponent(un.SimpleUnit("m"), un.Integer("2")))

    def test_caret_squared_unit(self):
        self.assertEqual(un.parse_unit_string("m^2"), un.Exponent(un.SimpleUnit("m"), un.Integer("2")))

    def test_double_asterisk_squared_unit(self):
        self.assertEqual(un.parse_unit_string("m**2"), un.Exponent(un.SimpleUnit("m"), un.Integer("2")))

    def test_implicit_negexp_unit(self):
        self.assertEqual(un.parse_unit_string("m-2"), un.Exponent(un.SimpleUnit("m"), un.Integer("-2")))

    def test_caret_negexp_unit(self):
        self.assertEqual(un.parse_unit_string("m^-2"), un.Exponent(un.SimpleUnit("m"), un.Integer("-2")))

    def test_double_asterisk_negexp_unit(self):
        self.assertEqual(un.parse_unit_string("m**-2"), un.Exponent(un.SimpleUnit("m"), un.Integer("-2")))

    def test_implicit_multiplication(self):
        self.assertEqual(un.parse_unit_string("kg m"), un.Product(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_dot_multiplication(self):
        self.assertEqual(un.parse_unit_string("kg.m"), un.Product(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_star_multiplication(self):
        self.assertEqual(un.parse_unit_string("kg*m"), un.Product(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_slash_division(self):
        self.assertEqual(un.parse_unit_string("kg/m"), un.Quotient(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_per_division(self):
        self.assertEqual(un.parse_unit_string("kg per m"), un.Quotient(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_caps_per_division(self):
        self.assertEqual(un.parse_unit_string("kg PER m"), un.Quotient(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_nospace_per_division(self):
        self.assertEqual(un.parse_unit_string("kgperm"), un.Quotient(un.SimpleUnit("kg"), un.SimpleUnit("m")))

    def test_exp_division(self):
        self.assertEqual(un.parse_unit_string("m / s2"), un.Quotient(un.SimpleUnit("m"), un.Exponent(un.SimpleUnit("s"), un.Integer("2"))))

    def test_repeated_multiplication(self):
        self.assertEqual(un.parse_unit_string("kg s2 m"), un.Product(
            un.Product(
                un.SimpleUnit("kg"),
                un.Exponent(un.SimpleUnit("s"), un.Integer("2")),
            ),
            un.SimpleUnit("m")
        ))

    def test_repeated_division(self):
        self.assertEqual(un.parse_unit_string("kg / m / s2"), un.Quotient(
            un.Quotient(
                un.SimpleUnit("kg"),
                un.SimpleUnit("m")
            ),
            un.Exponent(un.SimpleUnit("s"), un.Integer("2"))
        ))

    def test_shift(self):
        self.assertEqual(un.parse_unit_string("K @ 273.15"), un.Offset(un.SimpleUnit("K"), un.Real("273.15")))

    def test_combo_no_groups(self):
        self.assertEqual(un.parse_unit_string("kg K / Pa ref 273.15"), un.Offset(
            un.Quotient(
                un.Product(
                    un.SimpleUnit("kg"),
                    un.SimpleUnit("K")
                ),
                un.SimpleUnit("Pa")
            ),
            un.Real("273.15")
        ))

    def test_literal(self):
        self.assertEqual(un.parse_unit_string("60 sec"), un.Product(un.Integer("60"), un.SimpleUnit("sec")))


class TestConversions(ut.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls._converter = un.UnitConverter()

    def assertAlmostEqual(self, first: t.Union[float, decimal.Decimal], second: t.Union[float, decimal.Decimal], *args, **kwargs) -> None:
        super().assertAlmostEqual(first, second, *args, **kwargs)

    def test_speed(self):
        self.assertAlmostEqual(self._converter.convert(100, "m s-1", "km h-1"), decimal.Decimal('360'))
        self.assertAlmostEqual(self._converter.convert(decimal.Decimal(1852), "m h-1", "knot"), decimal.Decimal('1'))

    def test_m2(self):
        self.assertAlmostEqual(self._converter.convert(1, "km2", "m2"), decimal.Decimal('1000000'))

    def test_celsius(self):
        self.assertEqual(
            self._converter.convert(decimal.Decimal(27), '°C', 'K'),
            decimal.Decimal('300.15')
        )

    def test_celsius_relative(self):
        self.assertEqual(
            self._converter.convert(27, '°C m-1', 'K m-1'),
            decimal.Decimal('27')
        )

    def test_pressure(self):
        self.assertEqual(self._converter.convert(1000, 'hPa', 'kPa'), 100)
        self.assertEqual(self._converter.convert(1000, 'hPa', 'Pa'), 100000)
        self.assertEqual(self._converter.convert(1, 'dbar', 'Pa'), 10000)
        self.assertEqual(self._converter.convert(1, 'bar', 'Pa'), 100000)
        self.assertEqual(self._converter.convert(1, 'mbar', 'Pa'), 100)

    def test_pressure_delta(self):
        self.assertEqual(self._converter.convert(36, 'hPa h-1', 'Pa s-1'), 1)
        self.assertEqual(self._converter.convert(1, 'Pa s-1', 'hPa h-1'), 36)

    def test_concentration(self):
        self.assertEqual(self._converter.convert(decimal.Decimal(1000), "umol L-1", "mol L-1"), decimal.Decimal("0.001"))
        self.assertEqual(self._converter.convert(decimal.Decimal("0.001"), "mol L-1", "umol L-1"), 1000)

    def test_non_convertible(self):
        self.assertRaises(ValueError, self._converter.convert, 1, 'm', '°C')


class TestLowLevel(ut.TestCase):

    INTEGERS = ["+0", "-1", "0", "1234567"]
    DECIMALS = ["3.0", "-3.0", "3.7", "+3.7"]
    LITERALS = ['+3E7', '-4.21E9', '+3.3E-12', '3.4e9', '-2.31e-12']
    NOT_NUMBERS = ["foo", "hello", "bar", "3.4.1", "+-8", "", "2.3E2.1", "-2.3e2.7"]

    def test_is_integer(self):
        for case in itertools.chain(TestLowLevel.NOT_NUMBERS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertFalse(un._is_integer_number(case))
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertTrue(un._is_integer_number(case))
        
    def test_is_decimal(self):
        for case in itertools.chain(TestLowLevel.INTEGERS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertTrue(un._is_decimal_number(case))
        for case in TestLowLevel.NOT_NUMBERS:
            with self.subTest(case=case):
                self.assertFalse(un._is_decimal_number(case))

    def test_is_literal(self):
        for case in itertools.chain(TestLowLevel.INTEGERS, TestLowLevel.DECIMALS, TestLowLevel.LITERALS):
            with self.subTest(case=case):
                self.assertTrue(un._is_literal(case))
        for case in TestLowLevel.NOT_NUMBERS:
            with self.subTest(case=case):
                self.assertFalse(un._is_decimal_number(case))

    def test_parse_literal(self):
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertIsInstance(un._parse_literal(case), Integer)
        for case in itertools.chain(TestLowLevel.DECIMALS, TestLowLevel.LITERALS):
            with self.subTest(case=case):
                self.assertIsInstance(un._parse_literal(case), un.Real)

    def test_extract_leading_int(self):
        self.assertEqual(un._extract_leading_int('1234 kg'), ('1234', ' kg'))
        self.assertEqual(un._extract_leading_int('+1234 kg'), ('+1234', ' kg'))
        self.assertEqual(un._extract_leading_int('-1234 kg'), ('-1234', ' kg'))
        self.assertEqual(un._extract_leading_int('1234+567 kg'), ('1234', '+567 kg'))
        self.assertEqual(un._extract_leading_int('1234-567 k'), ('1234', '-567 k'))
        self.assertEqual(un._extract_leading_int('1234-567k'), ('1234', '-567k'))
        self.assertEqual(un._extract_leading_int('test'), ('', 'test'))

    def test_extract_leading_exponents(self):
        self.assertEqual(un._extract_leading_int_from_exp('12345 kg'), ('', '12345 kg'))
        self.assertEqual(un._extract_leading_int_from_exp('⁰0 kg'), ('0', '0 kg'))
        self.assertEqual(un._extract_leading_int_from_exp('⁺⁹ kg'), ('+9', ' kg'))

    def test_bad_unit_components(self):
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent(None)
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ** 2.3')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg **')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ** +')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ** -')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ^ 2.3')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ^')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ^ +')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg ^ -')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg+')
        with self.assertRaises(CNODCError):
            un._parse_unit_with_opt_exponent('kg-')

    def test_simple_unit_component(self):
        u = un._parse_unit_with_opt_exponent('kg')
        self.assertIsInstance(u, un.SimpleUnit)
        self.assertEqual(u.name, 'kg')

    def test_literal_components(self):
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertEqual(un._parse_unit_with_opt_exponent(case), Integer(case))
        for case in itertools.chain(TestLowLevel.LITERALS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertEqual(un._parse_unit_with_opt_exponent(case), un.Real(case))

    def test_exponent_component(self):
        x = un._parse_unit_with_opt_exponent('kg ** 2')
        self.assertIsInstance(x, un.Exponent)
        self.assertEqual(x.exponent, Integer('2'))
        self.assertIsInstance(x.base, un.SimpleUnit)
        self.assertEqual(x.base.name, 'kg')
        x = un._parse_unit_with_opt_exponent('kg^2')
        self.assertIsInstance(x, un.Exponent)
        self.assertEqual(x.exponent, Integer('2'))
        self.assertIsInstance(x.base, un.SimpleUnit)
        self.assertEqual(x.base.name, 'kg')
        x = un._parse_unit_with_opt_exponent('kg-2')
        self.assertIsInstance(x, un.Exponent)
        self.assertEqual(x.exponent, Integer('-2'))
        self.assertIsInstance(x.base, un.SimpleUnit)
        self.assertEqual(x.base.name, 'kg')
        x = un._parse_unit_with_opt_exponent('kg²')
        self.assertIsInstance(x, un.Exponent)
        self.assertEqual(x.exponent, Integer('2'))
        self.assertIsInstance(x.base, un.SimpleUnit)
        self.assertEqual(x.base.name, 'kg')
