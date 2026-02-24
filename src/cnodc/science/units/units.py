import decimal
import pathlib
import typing as t
import xml.etree.ElementTree as ET

import zrlog
import threading
from autoinject import injector
from uncertainties import UFloat

from cnodc.science.units.parsing import parse_unit_string
from cnodc.science.units.structures import LinearFunction, Converter, UnitExpression, UnitError

COMMON_BAD_UNITS = {
    'psu': '0.001',
    'micromole/l': 'umol L-1',
}


@injector.injectable_global
class UnitConverter:

    def __init__(self, table_directory: t.Union[pathlib.Path, str] = None):
        self._table_dir = table_directory or pathlib.Path(__file__).resolve().parent / "udunits"
        self._loaded_tables = None
        self._log = zrlog.get_logger("cnodc.units")
        self._cache = {}
        self._cache_lock = threading.RLock()
        self._table_lock = threading.Lock()
        self._nested_tracker = set()

    def _load_tables(self):
        if self._loaded_tables is None:
            with self._table_lock:
                if self._loaded_tables is None:
                    self._loaded_tables = {
                        'prefixes': {},
                        'units': {}
                    }
                    self._load_prefix_table('udunits2-prefixes.xml')
                    self._load_units_table('udunits2-base.xml')
                    self._load_units_table('udunits2-derived.xml')
                    self._load_units_table('udunits2-common.xml')
                    self._load_units_table('udunits2-accepted.xml')

    def _conversion_info(self, unit_str: str) -> tuple[Converter, dict[str, int], UnitExpression]:
        if unit_str not in self._cache:
            with self._cache_lock:
                if unit_str in self._nested_tracker:
                    raise UnitError("Infinite loop detected", 2000)
                self._nested_tracker.add(unit_str)
                if unit_str not in self._cache:
                    try:
                        expr = parse_unit_string(unit_str)
                        self._cache[unit_str] = [*expr.get_unit_info(self), expr]
                    except Exception as ex:
                        self._cache[unit_str] = ex
        if isinstance(self._cache[unit_str], Exception):
            raise self._cache[unit_str]
        return self._cache[unit_str]

    def _get_dimensions(self, unit_str: str) -> dict[str, int]:
        _, dims, _ = self._conversion_info(unit_str)
        return dims

    def _get_converter(self, unit_str: str) -> Converter:
        factor, _, _ = self._conversion_info(unit_str)
        return factor

    def _check_compatibility(self, dims_a: dict[str, int], dims_b: dict[str, int]) -> bool:
        if not len(dims_a) == len(dims_b):
            return False
        for x in dims_a.keys():
            if x not in dims_b:
                return False
            if dims_a[x] != dims_b[x]:
                return False
        return True

    def is_valid_unit(self, unit_str: str) -> bool:
        self._load_tables()
        try:
            _, _, _ = self._conversion_info(unit_str)
            return True
        except Exception as ex:
            return False

    def standardize(self, unit_name):
        if unit_name in COMMON_BAD_UNITS:
            unit_name = COMMON_BAD_UNITS[unit_name]
        if self.is_valid_unit(unit_name):
            return unit_name

    def convert(self, quantity: t.Union[float, int, UFloat, decimal.Decimal], original_units: str, output_units: str) -> t.Union[float, int, UFloat]:
        self._load_tables()
        factor_original, dims_original, expr_original = self._conversion_info(original_units)
        factor_output, dims_output, expr_output = self._conversion_info(output_units)
        if not self._check_compatibility(dims_original, dims_output):
            raise UnitError(f"Incompatible dimensions [{self._format_dims(dims_original)}] vs [{self._format_dims(dims_output)}]", 2001)
        return factor_output.invert().convert(factor_original.convert(quantity))

    def _format_dims(self, dims: dict[str, int]):
        s = []
        for d in sorted(dims.keys()):
            s.append(f"{d}^{dims[d]}")
        return " ".join(s)

    def compatible(self, units_a: str, units_b: str) -> bool:
        self._load_tables()
        return self._check_compatibility(
            self._get_dimensions(units_a),
            self._get_dimensions(units_b)
        )

    def _load_units_table(self, file_name: str):
        file = self._table_dir / file_name
        if not file.exists():
            self._log.error(f"File {file} does not exist, cannot load units")
            return
        et = ET.parse(file)
        for e in et.getroot().findall('unit'):
            names = []
            for s in e.findall('symbol'):
                names.append(s.text)
            for n in e.findall("name"):
                singular = n.find('singular')
                if singular is not None:
                    names.append(singular.text)
                plural = n.find('plural')
                if plural is not None:
                    names.append(plural.text)
            for a in e.findall("aliases"):
                for s in a.findall("symbol"):
                    names.append(s.text)
                for n in a.findall("name"):
                    singular = n.find('singular')
                    if singular is not None:
                        names.append(singular.text)
                    plural = n.find('plural')
                    if plural is not None:
                        names.append(plural.text)
            x = e.find('def')
            for name in names:
                if name in self._loaded_tables['units']:
                    self._log.warning(f"Unit [{name}] already defined")
                elif x is None:
                    self._loaded_tables['units'][name] = 'base'
                else:
                    self._loaded_tables['units'][name] = x.text

    def _load_prefix_table(self, file_name: str):
        file = self._table_dir / file_name
        if not file.exists():
            self._log.error(f"File {file} does not exist, cannot load prefixes")
            return
        et = ET.parse(file)
        for e in et.getroot().findall('prefix'):
            names = []
            for s in e.findall('symbol'):
                names.append(s.text)
            for n in e.findall('name'):
                names.append(n.text)
            val = decimal.Decimal(e.find('value').text)
            for n in names:
                if n in self._loaded_tables['prefixes']:
                    self._log.warning(f"Prefix [{n}] already defined")
                else:
                    self._loaded_tables['prefixes'][n] = val

    def _find_entry(self, simple_unit: str) -> tuple[decimal.Decimal, str, str]:
        self._load_tables()
        if simple_unit in self._loaded_tables['units']:
            return decimal.Decimal("1"), self._loaded_tables['units'][simple_unit], simple_unit
        for prefix in self._loaded_tables['prefixes']:
            if simple_unit.startswith(prefix):
                test_unit = simple_unit[len(prefix):]
                if test_unit not in self._loaded_tables['units']:
                    continue
                return self._loaded_tables['prefixes'][prefix], self._loaded_tables['units'][test_unit], test_unit
        raise UnitError(f"Unknown simple unit: [{simple_unit}]", 2002)

    def raw_unit_info(self, simple_unit: str) -> tuple[Converter, dict[str, int]]:
        factor, expr, real_unit = self._find_entry(simple_unit)
        if expr == 'base':
            return LinearFunction(factor), {real_unit: 1}
        else:
            return self._get_converter(expr).scale(factor), self._get_dimensions(expr)


def convert(v, from_units: str, to_units: str):
    if from_units is None or to_units is None or from_units == to_units or v is None:
        return v
    return _convert(v, from_units, to_units)


@injector.inject
def _convert(*args, uc: UnitConverter = None, **kwargs):
    return uc.convert(*args, **kwargs)


@injector.inject
def convert_units(quantity: t.Union[float, int, decimal.Decimal], units_in: str, units_out: str, converter: UnitConverter = None) -> decimal.Decimal:
    return converter.convert(quantity, units_in, units_out)
