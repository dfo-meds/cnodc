import decimal
import math as _math
import typing as t

from medsutil.math import types as mt, _common
from medsutil.math._common import nominal_value, match_convert, ln as _ln, sn_max
import medsutil.math.helpers as help
import medsutil.math.numbers as num
from medsutil.math.types import AnyNumber


def add(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_add(x, y)
    x_n, y_n = match_convert(x, y)
    return x_n + y_n

@num.as_science_number(help.derivative_1, help.derivative_1, units_func=help.units_first)
def sn_add(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) and mt.is_science_number(y):
        y = y.match_units(x)
    return add(nominal_value(x), nominal_value(y))

def sub(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_sub(x, y)
    x_n, y_n = match_convert(x, y)
    return x_n - y_n

@num.as_science_number(help.derivative_1, help.derivative_neg1, units_func=help.units_first)
def sn_sub(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) and mt.is_science_number(y):
        y = y.match_units(x)
    return sub(nominal_value(x), nominal_value(y))

def div(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_div(x, y)
    x_n, y_n = match_convert(x, y)
    return x_n / y_n

@num.as_science_number(help.derivative_numerator, help.derivative_denominator, units_func=help.units_div)
def sn_div(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return div(nominal_value(x), nominal_value(y))

def mul(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_mul(x, y)
    x_n, y_n = match_convert(x, y)
    return x_n * y_n

@num.as_science_number(help.derivative_y, help.derivative_x, units_func=help.units_multiply)
def sn_mul(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return mul(nominal_value(x), nominal_value(y))

def sqrt(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_sqrt(x)
    try:
        return x.sqrt()
    except AttributeError:
        return _math.sqrt(x)

@num.as_science_number(lambda x: 1 / (x ** 0.5), units_func=lambda x: f"({help.units_first(x)}^0.5)")
def sn_sqrt(x: mt.AnyNumber, /):
    return sqrt(nominal_value(x))

def exp(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_exp(x)
    try:
        return x.exp()
    except AttributeError:
        return _math.exp(x)

@num.as_science_number(lambda x: exp(nominal_value(x)), units_func = lambda x: f"(e^{help.units_first(x)})")
def sn_exp(x: mt.AnyNumber, /):
    return exp(nominal_value(x))

def ln(x: mt.AnyNumber):
    if mt.is_science_number(x):
        return sn_ln(x)
    return _ln(x)

@num.as_science_number(lambda x: 1 / x, units_func=lambda x: f"(ln {help.units_first(x)}")
def sn_ln(x: mt.AnyNumber):
    return ln(nominal_value(x))

def log10(x: mt.AnyNumber):
    if mt.is_science_number(x):
        return sn_log10(x)
    try:
        return x.log10()
    except AttributeError:
        return _math.log10(x)

@num.as_science_number(lambda x: 1 / (x * ln(decimal.Decimal(10))), units_func=lambda x: f"(log10 {help.units_first(x)}")
def sn_log10(x: mt.AnyNumber):
    return log10(nominal_value(x))

def log2(x: mt.AnyNumber):
    if mt.is_science_number(x):
        return sn_log2(x)
    try:
        return x.log10() / decimal.Decimal(2).log10()
    except AttributeError:
        return _math.log2(x)

@num.as_science_number(lambda x: 1 / (x * ln(decimal.Decimal(2))), units_func=lambda x: f"(log2 {help.units_first(x)}")
def sn_log2(x: mt.AnyNumber):
    return log2(nominal_value(x))

def log(x: mt.AnyNumber, base: mt.AnyNumber):
    if mt.is_science_number(x) or mt.is_science_number(base):
        return sn_log(x, base)
    x, base = match_convert(x, base)
    try:
        return x.log10() / base.log10()
    except AttributeError:
        return _math.log(x, base)

@num.as_science_number(lambda x, b: 1 / (x * ln(decimal.Decimal(b))), units_func=lambda x, y: f"(log{y} {help.units_first(x)}")
def sn_log(x: mt.AnyNumber, base: mt.AnyNumber):
    return log(nominal_value(x), nominal_value(base))

def pow(x: mt.AnyNumber, y: mt.AnyNumber):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_pow(x, y)
    x, y = match_convert(x, y)
    return x ** y

@num.as_science_number(help.derivative_power_base, help.derivative_power_exponent, units=help.units_power)
def sn_pow(x: mt.AnyNumber, y: mt.AnyNumber):
    return pow(nominal_value(x), nominal_value(y))

def modulo(x: mt.AnyNumber, y: mt.AnyNumber):
    # TODO: handle science number
    n_x, n_y = match_convert(nominal_value(x), nominal_value(y))
    return n_x % n_y

# coefficients must be highest to lowest, last one being x^0 (value is x)
def calculate_polynomial[T: AnyNumber](value: T, *coefficients: mt.AnyNumber) -> T:
    total: mt.AnyNumber | None = None
    for coefficient in coefficients:
        if total is None:
            total = coefficient
        else:
            total = add(mul(total, value), coefficient)
    return t.cast(T, total)


def is_close(x: mt.AnyNumber, y: mt.AnyNumber, relative_tolerance: str | float | int = "1e-09", absolute_tolerance: str | float | int = "0.0") -> bool:
    nv_x = nominal_value(x)
    if isinstance(nv_x, decimal.Decimal):
        rel_tol = decimal.Decimal(relative_tolerance)
        abs_tol = decimal.Decimal(absolute_tolerance)
    else:
        rel_tol = float(relative_tolerance)
        abs_tol = float(absolute_tolerance)
    diff = abs(sub(nv_x, nominal_value(y)))
    if absolute_tolerance is not None and _common.gt(diff, abs_tol):
        return False
    if relative_tolerance is not None and _common.gt(div(diff, sn_max(abs(x), abs(y))), rel_tol):
        return False
    return True
