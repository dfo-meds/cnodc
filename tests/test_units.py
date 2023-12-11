import decimal
import unittest as ut
import cnodc.units.units as un
import typing as t


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
            self._converter.convert(27, '°C', 'K'),
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
        self.assertEqual(self._converter.convert(1000, "umol L-1", "mol L-1"), decimal.Decimal("0.001"))
        self.assertEqual(self._converter.convert(decimal.Decimal("0.001"), "mol L-1", "umol L-1"), 1000)

    def test_non_convertible(self):
        self.assertRaises(ValueError, self._converter.convert, 1, 'm', '°C')
