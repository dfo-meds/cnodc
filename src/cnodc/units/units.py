import decimal
import math
import pathlib
import typing as t
import xml.etree.ElementTree as ET
import zrlog
import threading
from autoinject import injector
from uncertainties import UFloat


COMMON_BAD_UNITS = {
    'psu': '0.001',
}

class Converter:

    def convert(self, input_val):
        raise NotImplementedError()

    def scale(self, factor: decimal.Decimal):
        raise NotImplementedError()

    def power(self, factor: decimal.Decimal):
        raise NotImplementedError

    def product(self, other_converter):
        raise NotImplementedError

    def shift(self, factor: decimal.Decimal):
        raise NotImplementedError

    def invert(self):
        raise NotImplementedError()


class UnitExpression:

    def get_unit_info(self, ref_dict) -> tuple[Converter, dict[str, int]]:
        raise NotImplementedError()


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
                    raise ValueError("Infinite loop detected")
                self._nested_tracker.add(unit_str)
                if unit_str not in self._cache:
                    try:
                        expr = parse_unit_string(unit_str)
                        self._cache[unit_str] = [*expr.get_unit_info(self), expr]
                    except ValueError as ex:
                        self._cache[unit_str] = ex
        if isinstance(self._cache[unit_str], ValueError):
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
        except ValueError as ex:
            return False

    def standardize(self, unit_name):
        if unit_name in COMMON_BAD_UNITS:
            unit_name = COMMON_BAD_UNITS[unit_name]
        if self.is_valid_unit(unit_name):
            return unit_name

    def convert(self, quantity: t.Union[float, int, UFloat], original_units: str, output_units: str) -> t.Union[float, int, UFloat]:
        self._load_tables()
        factor_original, dims_original, expr_original = self._conversion_info(original_units)
        factor_output, dims_output, expr_output = self._conversion_info(output_units)
        if not self._check_compatibility(dims_original, dims_output):
            raise ValueError(f"Incompatible dimensions [{self._format_dims(dims_original)}] vs [{self._format_dims(dims_output)}]")
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
        raise ValueError(f"Invalid simple unit [{simple_unit}]")

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


class LinearFunction(Converter):

    def __init__(self, factor: decimal.Decimal, shift: decimal.Decimal = 0):
        self._scale = factor
        self._shift = shift

    def __repr__(self):
        if self._shift > 0:
            return f'{self._scale}x+{self._shift}'
        elif self._shift < 0:
            return f'{self._scale}x{self._shift}'
        else:
            return f'{self._scale}x'

    def convert(self, input_val):
        return (input_val * float(self._scale)) + float(self._shift)

    def scale(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return LinearFunction(self._scale * factor)

    def power(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return LinearFunction(self._scale ** factor)

    def shift(self, factor: decimal.Decimal):
        if factor == 0:
            return self
        return LinearFunction(self._scale, self._shift + factor)

    def invert(self):
        return LinearFunction(1 / self._scale, (-1 * self._shift) / self._scale)

    def product(self, other_converter):
        if isinstance(other_converter, LinearFunction):
            return LinearFunction(self._scale * other_converter._scale)
        raise ValueError("Cannot take products of other things yet")


class _NonLinearWrapper(Converter):

    def scale(self, factor: decimal.Decimal):
        return NonLinearFunction(self, factor)

    def shift(self, factor: decimal.Decimal):
        return NonLinearFunction(self, decimal.Decimal(1), factor)

    def power(self, factor: decimal.Decimal):
        return NonLinearFunction(self).power(factor)


class ExpFunction(_NonLinearWrapper):

    def __init__(self, base):
        self._base = base

    def convert(self, input_val):
        return math.pow(self._base, input_val)

    def invert(self):
        return LogFunction(self._base)


class LogFunction(Converter):

    def __init__(self, base):
        self._base = base

    def convert(self, input_val):
        return math.log(input_val, self._base)

    def invert(self):
        return ExpFunction(self._base)


class NonLinearFunction(Converter):

    def __init__(self, inner_func: Converter, scale: decimal.Decimal = 1, shift: decimal.Decimal = 0, inner_scale: decimal.Decimal = 1, inner_shift: decimal.Decimal = 0):
        self._inner = inner_func
        self._scale = scale
        self._inner_scale = inner_scale
        self._shift = shift
        self._inner_shift = inner_shift

    def convert(self, input_val):
        return (self._scale * self._inner.convert(input_val)) + self._shift

    def scale(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return NonLinearFunction(self._inner, self._scale * factor)

    def power(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return NonLinearFunction(self._inner, self._scale ** factor)

    def shift(self, factor: decimal.Decimal):
        if factor == 0:
            return self
        return NonLinearFunction(self._inner, self._scale, self._shift + factor)

    def invert(self):
        return NonLinearFunction(self._inner.invert())






class SimpleUnit(UnitExpression):

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"[{self.name}]"

    def __eq__(self, other):
        if isinstance(other, SimpleUnit):
            return other.name == self.name
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        return ref_dict.raw_unit_info(self.name)


class Literal(UnitExpression):
    pass


class Number(Literal):

    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, Number):
            return other.value == self.value
        return False

    def as_decimal(self):
        return decimal.Decimal(self.value)

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        return LinearFunction(self.as_decimal()), {}


class Integer(Number):
    pass


class Real(Number):
    pass


class Exponent(UnitExpression):

    def __init__(self, base: UnitExpression, exponent: Number):
        self.base = base
        self.exponent = exponent

    def __repr__(self):
        return f"({self.base} ** {self.exponent})"

    def __eq__(self, other):
        if isinstance(other, Exponent):
            return self.base == other.base and self.exponent == other.exponent
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:
        base_factor, base_dims = self.base.get_unit_info(ref_dict)
        exp_num = self.exponent.as_decimal()
        return base_factor.power(exp_num), {x: base_dims[x] * exp_num for x in base_dims}


class Product(UnitExpression):

    def __init__(self, left: UnitExpression, right: UnitExpression):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({str(self.left)} * {str(self.right)})"

    def __eq__(self, other):
        if isinstance(other, Product):
            return other.left == self.left and other.right == self.right
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:
        left_factor, left_dims = self.left.get_unit_info(ref_dict)
        right_factor, right_dims = self.right.get_unit_info(ref_dict)
        result_dims = {x: left_dims[x] for x in left_dims}
        for x in right_dims:
            if x in result_dims:
                result_dims[x] += right_dims[x]
            else:
                result_dims[x] = right_dims[x]
        return left_factor.product(right_factor), result_dims


class Quotient(UnitExpression):

    def __init__(self, left: UnitExpression, right: UnitExpression):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({str(self.left)} / {str(self.right)})"

    def __eq__(self, other):
        if isinstance(other, Quotient):
            return other.left == self.left and other.right == self.right
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:
        left_factor, left_dims = self.left.get_unit_info(ref_dict)
        right_factor, right_dims = self.right.get_unit_info(ref_dict)
        result_dims = {x: left_dims[x] for x in left_dims}
        for x in right_dims:
            if x in result_dims:
                result_dims[x] -= right_dims[x]
            else:
                result_dims[x] = -1 * right_dims[x]
        return left_factor.product(right_factor.invert()), result_dims


class Log(UnitExpression):

    def __init__(self, expression: UnitExpression, base: Number):
        self.expression = expression
        self.base = base

    def __eq__(self, other):
        if isinstance(other, Log):
            return other.base == self.base and other.expression == self.expression
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        raise ValueError("Unit info calculations not yet supported for log units")


class Offset(UnitExpression):

    def __init__(self, expression: UnitExpression, offset: Literal):
        self.expression = expression
        self.offset = offset

    def __eq__(self, other):
        if isinstance(other, Offset):
            return other.expression == self.expression and other.offset == self.offset
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        inner_convert, inner_dims = self.expression.get_unit_info(ref_dict)
        if isinstance(self.offset, Number):
            return inner_convert.shift(self.offset.as_decimal()), inner_dims
        raise ValueError("Shift not supported with other data types yet")


@injector.inject
def convert_units(quantity: t.Union[float, int, decimal.Decimal], units_in: str, units_out: str, converter: UnitConverter = None) -> decimal.Decimal:
    return converter.convert(quantity, units_in, units_out)



UNICODE_EXPONENTS = {
    '⁰': '0',
    '¹': '1',
    '²': '2',
    '³': '3',
    '⁴': '4',
    '⁵': '5',
    '⁶': '6',
    '⁷': '7',
    '⁸': '8',
    '⁹': '9',
    '⁺': '+',
    '⁻': '-'
}

UDUNITS_MULTIPLICATION = ('*', '.', '·')
UDUNITS_DIVISION = ('PER', 'per', '/')
UDUNITS_BOTH_OPS = (*UDUNITS_DIVISION, *UDUNITS_MULTIPLICATION)
UDUNITS_EXPONENTIATION = ('^', '**')


def parse_unit_string(units: str) -> UnitExpression:
    units = units.replace('**', '^')
    try:
        shift_factor = None
        units = units.strip(' ')
        for shift_op in ('@', 'after', 'from', 'since', 'ref'):
            if shift_op in units:
                rpos = units.rfind(shift_op)
                following = units[rpos + len(shift_op):].strip()
                if _is_literal(following):
                    units = units[:rpos]
                    shift_factor = _parse_literal(following)
                    break
        base_expr = _parse_unit_for_groups(units) if '(' in units or ')' in units else _parse_no_group_unit_string(units)
        if shift_factor:
            return Offset(base_expr, shift_factor)
        else:
            return base_expr
    except ValueError as ex:
        raise ValueError(f"Error parsing string [{units}]") from ex


def _parse_unit_for_groups(units: str) -> UnitExpression:
    pieces = [""]
    depth = 0
    for pos, char in enumerate(units):
        if char == '(':
            if depth == 0:
                pieces.append("")
            else:
                pieces[-1] += char
            depth += 1
        elif char == ")":
            depth -= 1
            if depth > 0:
                pieces[-1] += char
            elif depth == 0:
                pieces.append("")
            if depth < 0:
                raise ValueError(f"Parse error, `)` found without opening `(` at position {pos}")
        else:
            pieces[-1] += char
    pieces = [p.strip() for p in pieces if p.strip()]
    # Look for log pieces
    new_pieces = []
    for idx in range(0, len(pieces) - 1):
        if not pieces[idx+1].startswith("re"):
            new_pieces.append(pieces[idx])
        omit_len = 3 if pieces[idx+1].startswith("re:") else 2
        if pieces[idx].endswith("log"):
            if pieces[idx] != "log":
                new_pieces.append(pieces[idx][:-3].strip())
            new_pieces.append(Log(parse_unit_string(pieces[idx + 1][omit_len:].strip(" ")), Integer('10')))
        elif pieces[idx].endswith("lg"):
            if pieces[idx] != "lg":
                new_pieces.append(pieces[idx][:-2].strip())
            new_pieces.append(Log(parse_unit_string(pieces[idx + 1][omit_len:].strip(" ")), Integer('10')))
        elif pieces[idx].endswith("ln"):
            if pieces[idx] != "ln":
                new_pieces.append(pieces[idx][:-2].strip())
            new_pieces.append(Log(parse_unit_string(pieces[idx + 1][omit_len:].strip(" ")), Real('e')))
        elif pieces[idx].endswith("lb"):
            if pieces[idx] != "lb":
                new_pieces.append(pieces[idx][:-2].strip())
            new_pieces.append(Log(parse_unit_string(pieces[idx + 1][omit_len:].strip(" ")), Integer('2')))
    # Look for leading exponentiation operators
    exp_operators = UDUNITS_EXPONENTIATION
    new_pieces3 = [new_pieces[0]]
    for piece in new_pieces[1:]:
        found = False
        if isinstance(piece, str):
            for exp_op in exp_operators:
                if piece.startswith(exp_op):
                    exp_int, following = _extract_leading_int(piece[len(exp_op):])
                    if exp_int in ('', '+', '-'):
                        raise ValueError('Invalid exponentiation, must be an integer')
                    new_pieces3.append(f'^{exp_int}')
                    following = following.strip()
                    if following:
                        new_pieces3.append(following)
                    found = True
                    break
            if not found:
                if piece[0] in UNICODE_EXPONENTS:
                    exp_int, following = _extract_leading_int_from_exp(piece)
                    if exp_int in ('', '+', '-'):
                        raise ValueError('Invalid exponentiation, must be an integer')
                    new_pieces3.append(f'^{exp_int}')
                    following = following.strip()
                    if following:
                        new_pieces3.append(following)
                    found = True
        if not found:
            new_pieces3.append(piece)

    # Look for leading and trailing operators
    new_pieces2 = []
    for idx, piece in enumerate(new_pieces3):
        new_start = None
        new_end = None
        if not isinstance(piece, UnitExpression):
            for op in UDUNITS_BOTH_OPS:
                if new_start is None and piece.startswith(op):
                    new_start = op
                    piece = piece[len(op):].strip()
                if new_end is None and piece.endswith(op):
                    new_end = op
                    piece = piece[:len(op) * -1]
                if new_start is None and new_end is None:
                    break
        if new_start is not None:
            new_pieces2.append(new_start)
        new_pieces2.append(piece)
        if new_end is not None:
            new_pieces2.append(new_end)

    expr = None
    op = None
    for piece in new_pieces2:
        if expr is None:
            expr = piece if isinstance(piece, UnitExpression) else parse_unit_string(piece)
            op = None
        elif piece[0] == "^":
            if op is not None:
                raise ValueError(f"Operation cannot be followed by an exponentiation immediately")
            expr = Exponent(expr, int(piece[1:]))
            op = None
        elif piece in UDUNITS_MULTIPLICATION:
            op = '*'
        elif piece in UDUNITS_DIVISION:
            op = '/'
        else:
            expr2 = piece if isinstance(piece, UnitExpression) else parse_unit_string(piece)
            if op is None or op == '*':
                expr = Product(expr, expr2)
            elif op == '/':
                expr = Quotient(expr, expr2)
            else:
                raise ValueError("Invalid operation")
    return expr


def _parse_no_group_unit_string(units: str) -> UnitExpression:
    # units should be a product-spec string at this point and not contain a group (group decomposition handled above)
    # log functions also handled above
    # shift statements handled above
    """
    elements we might see:
    - multiply or divide
    - exponentiation
    """
    # remove leading and trailing spaces
    units = units.strip()
    # strip multiple spaces down to single spaces
    pieces = []
    buffer = ''
    skip = 0
    for i in range(0, len(units)):
        if skip > 0:
            skip -= 1
            continue
        if units[i] == ' ':
            if buffer != '':
                pieces.append(buffer)
                buffer = ''
            continue

        for op in UDUNITS_BOTH_OPS:
            if units[i:i+len(op)] == op:
                if buffer != '':
                    pieces.append(buffer)
                pieces.append(op)
                buffer = ''
                skip = len(op) - 1
                break
        else:
            buffer += units[i]
    pieces.append(buffer)
    expr = None
    op = None
    for piece in pieces:
        if expr is None:
            expr = _parse_unit_with_opt_exponent(piece)
        elif piece in UDUNITS_DIVISION:
            op = '/'
        elif piece in UDUNITS_MULTIPLICATION:
            op = '*'
        else:
            expr2 = _parse_unit_with_opt_exponent(piece)
            if op == '*' or op is None:
                expr = Product(expr, expr2)
            elif op == '/':
                expr = Quotient(expr, expr2)
            else:
                raise ValueError(f'Invalid operation')
    return expr


def _parse_unit_with_opt_exponent(units: str) -> UnitExpression:
    # At this point we have removed all the multiplication and division signs
    # we should be left with either a NUMBER or an ID
    # but it might have a trailing exponent if it is an ID
    if not units:
        raise ValueError(f"Empty unit string")
    if _is_literal(units):
        return _parse_literal(units)
    elif '**' in units:
        p = units.find('**')
        exp = units[p+2:]
        if exp in ('', '+', '-') or exp[0] not in "+-0123456789" or any(exp_digit not in '0123456789' for exp_digit in exp[1:]):
            raise ValueError('Invalid exponent, no digits')
        return Exponent(SimpleUnit(units[:p].strip()), Integer(exp))
    elif '^' in units:
        p = units.find('^')
        exp = units[p+1:]
        if exp in ('', '+', '-') or exp[0] not in "+-0123456789" or any(exp_digit not in '0123456789' for exp_digit in exp[1:]):
            raise ValueError('Invalid exponent, no digits')
        return Exponent(SimpleUnit(units[:p].strip()), Integer(exp))
    elif units[-1].isdigit() or units[-1] in '+-':
        exp = ""
        while units[-1].isdigit() or units[-1] in '+-':
            exp = f"{units[-1]}{exp}"
            units = units[:-1]
        if exp in ('', '+', '-'):
            raise ValueError('Invalid exponent, no digits')
        return Exponent(SimpleUnit(units), Integer(exp))
    elif units[-1] in UNICODE_EXPONENTS:
        exp = ""
        while units[-1] in UNICODE_EXPONENTS:
            exp = f"{UNICODE_EXPONENTS[units[-1]]}{exp}"
            units = units[:-1]
        if exp in ('', '+', '-'):
            raise ValueError('Invalid exponent, no digits')
        return Exponent(SimpleUnit(units), Integer(exp))
    else:
        return SimpleUnit(units)


def _extract_leading_int(s: str) -> tuple[str, str]:
    idx = 0
    while s[idx].isdigit() or (idx == 0 and (s == '+') or s == '-'):
        idx += 1
    return s[:idx], s[idx:]


def _extract_leading_int_from_exp(s: str) -> tuple[str, str]:
    idx = 0
    exp_int = ''
    while s[idx] in UNICODE_EXPONENTS:
        exp_int += f'{UNICODE_EXPONENTS[s[idx]]}'
        idx += 1
    return exp_int, s[idx:]


def _parse_literal(s: str) -> Literal:
    if '.' in s or 'e' in s or 'E' in s:
        return Real(s)
    else:
        return Integer(s)


def _is_literal(s: str):
    if 'E' in s:
        pieces = s.split('E', maxsplit=1)
        return _is_decimal_number(pieces[0]) and _is_integer_number(pieces[1])
    elif 'e' in s:
        pieces = s.split('e', maxsplit=1)
        return _is_decimal_number(pieces[0]) and _is_integer_number(pieces[1])
    else:
        return _is_decimal_number(s)
    # TODO: timestamp format?


def _is_decimal_number(s: str) -> bool:
    if s == '':
        return False
    if s.startswith("+") or s.startswith("-"):
        s = s[1:]
    if "." in s:
        pieces = s.split(".", maxsplit=1)
        return pieces[0].isdigit() and (pieces[1] == '' or pieces[1].isdigit())
    else:
        return s.isdigit()

def _is_integer_number(s: str) -> bool:
    if s == '':
        return False
    if s.startswith("+") or s.startswith("-"):
        return s[1:].isdigit()
    return s.isdigit()



