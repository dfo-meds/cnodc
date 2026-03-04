import decimal
import unittest as ut
import cnodc.science.units.units as un
import typing as t

from cnodc.science.units.structures import UnitError
from cnodc.science.units.units import convert
from cnodc.util.exceptions import CNODCError
from science.units.test_parsing import TestUnitParsing


class TestConversions(ut.TestCase):

    VALID_UNITS = {
        'hPa': 'hPa',
        'dbar': 'dbar',
        'bar': 'bar',
        'mbar': 'mbar',
        'h': 'h',
        's': 's',
        'umol': 'umol',
        'L': 'L',
        'm': 'm',
        'm-1': 'm-1',
        'decibar': 'dbar',
        'mg/m3': 'mg m-3',
        'mg m-3': 'mg m-3',
        'K-1 J m': 'J m K-1',
        '1': '1',
        'K': 'K',
        '°C': 'degrees_Celsius',
        'degree_Celsius': 'degrees_Celsius',
        'degree_C': 'degrees_Celsius',
        'ppb': 'ppb',
        'umol l-1': 'umol L-1',
        'micromol/kg': 'umol kg-1',
        'micromol/l': 'umol L-1',
        'mhos/m': 'S m-1',
        's-1': 's-1',
        'Hz': 'Hz',
        'degree': 'arc_degree',
        'degree_east': 'degrees_east',
        'degree_north': 'degrees_north',
    }

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
        self.assertEqual(self._converter.convert(1, 'mbar', 'mbar'), 1)

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

        for unit in TestConversions.VALID_UNITS:
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
        self.assertEqual(self._converter.standardize('psu'), '0.001')
        for x in TestConversions.VALID_UNITS:
            with self.subTest(unit=x):
                self.assertEqual(TestConversions.VALID_UNITS[x], self._converter.standardize(x))
        self.assertIsNone(self._converter.standardize('foobarmonkeyhat'))

    def test_compatible_units(self):
        self.assertTrue(self._converter.compatible('m s', 's m'))
        self.assertTrue(self._converter.compatible('Pa', 'bar'))
        self.assertTrue(self._converter.compatible('N', 'kg m s-2'))
        self.assertTrue(self._converter.compatible('°C m-1', 'K m-1'))

    def test_quick_convert(self):
        self.assertEqual(1, convert(1000, "m", "km"))

    def test_quick_convert_same_units(self):
        self.assertEqual(1000, convert(1000, "m", "m"))

    def test_infinite_loop(self):
        self._converter._load_tables()
        self._converter._nested_tracker.add('m')
        with self.assertRaises(UnitError):
            self._converter._conversion_info('m')
        self._converter._nested_tracker.remove('m')

    def test_bad_file_load(self):
        with self.assertLogs("cnodc.units", "ERROR"):
            self._converter._load_units_table("not_a_file.xml")
        with self.assertLogs("cnodc.units", "ERROR"):
            self._converter._load_prefix_table("not_a_file.xml")

    def test_reload_units(self):
        self._converter._load_tables()

        with self.assertLogs("cnodc.units", "WARNING"):
            self._converter._load_units_table("udunits2-common.xml")

        with self.assertLogs("cnodc.units", "WARNING"):
            self._converter._load_prefix_table("udunits2-prefixes.xml")
