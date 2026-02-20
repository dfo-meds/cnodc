import decimal
import unittest as ut
import cnodc.units.units as un
import typing as t

from cnodc.util.exceptions import CNODCError


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
        self.assertRaises(CNODCError, self._converter.convert, 1, 'm', '°C')
        self.assertRaises(CNODCError, self._converter.convert, 1, 'm s-2', '°C')
        self.assertRaises(CNODCError, self._converter.convert, 1, 'm s-2', 'm s')

    def test_valid_unit(self):
        valid_units = [
            'hPa', 'dbar', 'bar', 'mbar', 'h', 's', 'umol', 'L', 'm'
        ]
        for unit in valid_units:
            with self.subTest(unit=unit):
                self.assertTrue(self._converter.is_valid_unit(unit))

        invalid_units = [
            'foobarmonkeyhat',
            'smbar',
        ]
        for unit in invalid_units:
            with self.subTest(unit=unit):
                self.assertFalse(self._converter.is_valid_unit(unit))

    def test_standardize_units(self):
        self.assertEqual(self._converter.standardize('kg'), 'kg')
        self.assertEqual(self._converter.standardize('m s-1'), 'm s-1')
        self.assertEqual(self._converter.standardize('psu'), '0.001')
        self.assertEqual(self._converter.standardize('micromole/l'), 'umol L-1')
        self.assertIsNone(self._converter.standardize('foobarmonkeyhat'))

    def test_compatible_units(self):
        self.assertTrue(self._converter.compatible('m s', 's m'))
        self.assertTrue(self._converter.compatible('Pa', 'bar'))
        self.assertTrue(self._converter.compatible('N', 'kg m s-2'))
        self.assertTrue(self._converter.compatible('°C m-1', 'K m-1'))