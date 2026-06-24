import decimal
import pathlib
import typing as t
import xml.etree.ElementTree as ET  # nosec B314 # file is under our control

import zrlog
import threading
from autoinject import injector

from medsutil.math import AnyNumber
from medsutil.units.parsing import parse_unit_string
from medsutil.units.structures import LinearFunction, Converter, UnitExpression, UnitError
import medsutil.math as amath


# these are often seen in other files
# but aren't handled well. they're fixed only
# when standardize() is called on them.
STANDARDIZE_FIXES = {
    "degree true": "degrees_true",
    "degrees true": "degrees_true",
    "C": "celsius",
    "0/00": "1e-3",
    "/s": "1/s",
    "/m": "1/m",
    "mph": "mi hr-1",
    "kph": "km hr-1",
}

CUSTOM_PLURALS = {
    "pi": None,
    "amu": None,
    "hertz": None,
    "siemens": None,
    "lux": None,
    "sec": None,
    'celsius': None,
    'avogadro_constant': None,
    'astronomical_unit_BIPM_2006': 'astronomical_units_BIPM_2006'
}


# these units are handled as aliases
ADDITIONAL_UNITS = {
    "AU": "au",
    'psu': '1e-3',
    "‰": "1e-3",
    'mhos': 'S',
    "kn": "knots",
    'deg': 'degree',
    'degree_true': 'degree',
    "1": "1",
    "1e-2": "1e-2",
    "1e-3": "1e-3",
    "1e-6": "1e-6",
    "1e-9": "1e-9",
    "1e-12": "1e-12",
    "1e-15": "1e-15",
}

# normally we just let it pick the first one it finds, but I don't like some of them
PREFERRED_UNIT_OVERRIDES = {
    "Tbl": "tbsp",
    '"': 'arc_minute',
    "'": 'arc_second',
    "%": "1e-2",
    "C12_faraday": "faraday",
    "IT_Btu": "Btu",
    "degree_north": "degree_north",
    "degree_east": "degree_east",
    "US_dry_pint": "dry_pint",
    "US_dry_quart": "dry_quart",
    "US_liquid_cup": "cup",
    "US_liquid_gallon": "gallon",
    "US_liquid_gill": "gill",
    "US_liquid_quart": "quart",
    "international_knot": "knot",
    "kt": "knot",
    "nautical_mile": "nmile",
    "ppt": "1e-12",
    "ppm": "1e-6",
    "ppb": "1e-9",
    "ppq": "1e-15",
    "ppv": "1",
    "°": "degree",
    "°C": "degree_C",
    "℃": "degree_C",
    "°F": "degree_F",
    "°K": "K",
    "°R": "degree_R",
    "Å": "angstrom",
    "Ω": "ohm",

}
PREFERRED_PREFIX_OVERRIDES = {
    'µ': 'u'
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
                        'preferred_prefixes': {},
                        'units': {},
                        'preferred_units': {},
                    }
                    self._load_prefix_table('udunits2-prefixes.xml')
                    self._load_units_table('udunits2-base.xml')
                    self._load_units_table('udunits2-derived.xml')
                    self._load_units_table('udunits2-common.xml')
                    self._load_units_table('udunits2-accepted.xml')
                    for x in ADDITIONAL_UNITS:
                        if x not in self._loaded_tables['units']:
                            self._loaded_tables['units'][x] = ADDITIONAL_UNITS[x]
                            self._loaded_tables['preferred_units'][x] = ADDITIONAL_UNITS[x]
                    self._fix_preferred_units()

    def _conversion_info(self, unit_str: str) -> tuple[Converter, dict[str, int], UnitExpression]:
        if unit_str not in self._cache:
            with self._cache_lock:
                if unit_str in self._nested_tracker:
                    raise UnitError("Infinite loop detected", 2000)
                self._nested_tracker.add(unit_str)
                if unit_str not in self._cache:
                    try:
                        expr = parse_unit_string(unit_str)
                        if expr is None:
                            raise UnitError(f"Invalid unit string: [{unit_str}]", 2001)
                        expr = expr.standardize()
                        self._cache[unit_str] = [*expr.get_unit_info(self), expr]
                    except Exception as ex:
                        self._cache[unit_str] = ex
                        #self._log.exception(f"Error building conversion info for {unit_str}")
        if isinstance(self._cache[unit_str], Exception):
            raise self._cache[unit_str]
        return self._cache[unit_str]

    def get_dimensions(self, unit_str: str) -> dict[str, int]:
        _, dims, _ = self._conversion_info(unit_str)
        return dims

    def _get_converter(self, unit_str: str) -> Converter:
        factor, _, _ = self._conversion_info(unit_str)
        return factor

    def _check_compatibility(self, dims_a: dict[str, int], dims_b: dict[str, int]) -> bool:
        dims_a = {x: dims_a[x] for x in dims_a if dims_a[x] != 0}
        dims_b = {x: dims_b[x] for x in dims_b if dims_b[x] != 0}
        if not len(dims_a) == len(dims_b):
            return False
        for x in dims_a.keys():
            if x not in dims_b:
                return False
            if dims_a[x] != dims_b[x]:
                return False
        return True

    def is_valid_unit(self, unit_str: str) -> bool:
        try:
            return self.standardize(unit_str) is not None
        except UnitError:
            return False

    def standardize(self, unit_name):
        if unit_name in STANDARDIZE_FIXES:
            unit_name = STANDARDIZE_FIXES[unit_name]
        self._load_tables()
        _, _, expr = self._conversion_info(unit_name)
        return expr.udunit_str(self)

    def convert[T: AnyNumber](self, quantity: T, original_units: str, output_units: str) -> T:
        if amath.is_science_number(quantity) | isinstance(quantity, decimal.Decimal):
            return self._convert(quantity, original_units, output_units)
        elif isinstance(quantity, float):
            return float(self._convert(decimal.Decimal(quantity), original_units, output_units))
        elif isinstance(quantity, int):
            return int(self._convert(decimal.Decimal(quantity), original_units, output_units))
        else:
            raise TypeError('Invalid type for conversion')

    def _convert(self, quantity: decimal.Decimal, original_units: str, output_units: str) -> decimal.Decimal:
        self._load_tables()
        factor_original, dims_original, expr_original = self._conversion_info(original_units)
        factor_output, dims_output, expr_output = self._conversion_info(output_units)
        if not self._check_compatibility(dims_original, dims_output):
            raise UnitError(f"Incompatible dimensions [{self._format_dims(dims_original)}] vs [{self._format_dims(dims_output)}]", 2001)
        return factor_output.invert().convert(factor_original.convert(quantity))

    def _format_dims(self, dims: dict[str, int]) -> str:
        s = []
        for d in sorted(dims.keys()):
            if dims[d] == 0:
                continue
            elif dims[d] == 1:
                s.append(d)
            else:
                s.append(f"{d}{dims[d]}")
        return " ".join(s)

    def get_equivalent_base_units(self, units: str) -> str:
        dims = self.get_dimensions(units)
        if not dims:
            return ""
        else:
            return self._format_dims(dims)

    def compatible(self, units_a: str, units_b: str) -> bool:
        self._load_tables()
        try:
            return self._check_compatibility(
                self.get_dimensions(units_a),
                self.get_dimensions(units_b)
            )
        except Exception:
            return False

    def _load_units_table(self, file_name: str):
        file = self._table_dir / file_name
        if not file.exists():
            self._log.error(f"File {file} does not exist, cannot load units")
            return
        et = ET.parse(file)  # nosec B314 # file is under our control
        for e in et.getroot().findall('unit'):
            names = []
            for s in e.findall('symbol'):
                names.append(s.text)
            for n in e.findall("name"):
                singular = n.find('singular')
                if singular is not None:
                    names.append(singular.text)
                plural = n.find('plural')
                noplural = n.find('noplural')
                if plural is not None:
                    names.append(plural.text)
                elif singular is not None and noplural is None:
                    plural = self._pluralize(singular.text or "")
                    if plural is not None:
                        names.append(plural)
            for a in e.findall("aliases"):
                for s in a.findall("symbol"):
                    names.append(s.text)
                for n in a.findall("name"):
                    singular = n.find('singular')
                    if singular is not None:
                        names.append(singular.text)
                    plural = n.find('plural')
                    noplural = n.find('noplural')
                    if plural is not None:
                        names.append(plural.text)
                    elif singular is not None and noplural is None:
                        plural = self._pluralize(singular.text or "")
                        if plural is not None:
                            names.append(plural)
            x = e.find('def')
            for name in names:
                self._loaded_tables['preferred_units'][name] = names[0]
                if name in self._loaded_tables['units']:
                    self._log.warning(f"Unit [{name}] already defined")
                elif x is None:
                    self._loaded_tables['units'][name] = 'base'
                else:
                    self._loaded_tables['units'][name] = x.text

    def _fix_preferred_units(self):
        self._loaded_tables['preferred_units'] = {
            x: self._resolve_unit(y)
            for x, y in self._loaded_tables['preferred_units'].items()
        }

    def _resolve_unit(self, unit):
        if unit in PREFERRED_UNIT_OVERRIDES:
            return PREFERRED_UNIT_OVERRIDES[unit]
        if self._loaded_tables['preferred_units'][unit] == unit:
            return unit
        return self._resolve_unit(self._loaded_tables['preferred_units'][unit])

    def _pluralize(self, unit):
        if unit in CUSTOM_PLURALS:
            return CUSTOM_PLURALS[unit]
        elif unit.startswith("degree_"):
            return f"degrees_{unit[7:]}"
        elif unit.endswith("_international"):
            return f"{self._pluralize(unit[:-14])}_international"
        elif unit.endswith(('s', 'sh', 'ch', 'x')):
            return f"{unit}es"
        elif unit.endswith("y"):
            if unit[-2] in ("a", "e", "i", "o", "u"):
                return f"{unit}s"
            else:
                return f"{unit[:-1]}ies"
        elif unit.endswith("f"):
            return f"{unit[:-1]}ves"
        elif unit.endswith("fe"):
            return f"{unit[:-2]}ves"
        elif unit.endswith("is"):
            return f"{unit[:-2]}es"
        elif unit.endswith(("z", "o", "us")):
            # these ones are complex, we won't try
            return None
        else:
            return f"{unit}s"

    def _load_prefix_table(self, file_name: str):
        file = self._table_dir / file_name
        if not file.exists():
            self._log.error(f"File {file} does not exist, cannot load prefixes")
            return
        et = ET.parse(file)  # nosec B314 # file is under our control
        for e in et.getroot().findall('prefix'):
            names = []
            preferred = None
            for s in e.findall('symbol'):
                names.append(s.text)
                if preferred is None:
                    preferred = s.text
            for n in e.findall('name'):
                names.append(n.text)
                if preferred is None:
                    preferred = n.text
            if preferred is None:
                self._log.error(f"No name or symbol found")
                continue
            val = None
            v = e.find('value')
            if v is not None:
                txt = v.text
                if txt is not None:
                    val = decimal.Decimal(txt)
            if val is None:
                self._log.error(f"No prefix value found for [{preferred}]")
                continue
            for n in names:
                if n in self._loaded_tables['prefixes']:
                    self._log.warning(f"Prefix [{n}] already defined")
                else:
                    self._loaded_tables['prefixes'][n] = val
                    self._loaded_tables['preferred_prefixes'][n] = preferred if preferred not in PREFERRED_PREFIX_OVERRIDES else PREFERRED_PREFIX_OVERRIDES[preferred]

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

    def standardize_simple_unit(self, simple_unit: str):
        if simple_unit in self._loaded_tables['preferred_units']:
            return self._resolve_unit(simple_unit)
        for prefix in self._loaded_tables['preferred_prefixes']:
            if simple_unit.startswith(prefix):
                test_unit = simple_unit[len(prefix):]
                if test_unit not in self._loaded_tables['preferred_units']:
                    continue
                return self._loaded_tables['preferred_prefixes'][prefix] + self._resolve_unit(test_unit)

    def raw_unit_info(self, simple_unit: str) -> tuple[Converter, dict[str, int]]:
        factor, expr, real_unit = self._find_entry(simple_unit)
        if expr == 'base':
            return LinearFunction(factor), {real_unit: 1}
        else:
            return self._get_converter(expr).scale(factor), self.get_dimensions(expr)


def convert(v, from_units: str | None, to_units: str | None):
    if from_units is None or to_units is None or from_units == to_units or v is None:
        return v
    return _convert(v, from_units, to_units)


class _Convert:

    def __init__(self):
        self._converter = None

    def __call__(self, *args, **kwargs):
        if self._converter is None:
            self._converter = self._build_converter()
        return self._converter.convert(*args, **kwargs)

    @injector.inject
    def _build_converter(self, c: UnitConverter = None):
        return c


_convert = _Convert()


