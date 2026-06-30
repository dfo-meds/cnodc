import decimal
import typing as t
import math as _math
from medsutil.math import types as mt, _common, collapse
from medsutil.math._common import nominal_value, match_convert, match_units, separate_by_type
import medsutil.math.helpers as help
import medsutil.math.numbers as num
from medsutil.math._constants import DEFAULT_REL_TOL, DEFAULT_ABS_TOL
from medsutil.math.types import AnyNumber, MathTypeError, NumberOrIterable


# coefficients must be highest to lowest, last one being x^0 (value is x)
# one variable polynomials only
def calculate_polynomial[T: AnyNumber](value: T, *coefficients: mt.AnyNumber) -> T:
    total: mt.AnyNumber | None = None
    for coefficient in coefficients:
        if total is None:
            total = coefficient
        else:
            total = add(mul(total, value), coefficient)
    return t.cast(T, total)

def test_compatibility(x: mt.NonScienceNumber,
                       sx: mt.NonScienceNumber,
                       y: mt.NonScienceNumber,
                       sy: mt.NonScienceNumber,
                       c: mt.NonScienceNumber = 3) -> bool:
    n_x, n_sx, n_y, n_sy = match_convert(x, sx, y, sy)
    z_score = abs(n_x - n_y) / sqrt((n_sx ** 2) + (n_sy ** 2))
    return z_score < c

def add_in_quadrature(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.AnyNumber:
    # sqrt(x**2 + y**2)
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_add_in_quadrature(x, y)
    x_n, y_n = match_convert(x, y)
    return _common.sqrt((x_n ** 2) + (y_n ** 2))

@num.with_error_propagation(help.derivative_pythagoras_x, help.derivative_pythagoras_y, units_func=help.units_first)
def sn_add_in_quadrature(x: mt.AnyNumber, y: mt.AnyNumber, /):
    x_n, y_n = match_convert(*match_units(x, y))
    return _common.sqrt((x_n ** 2) + (y_n ** 2))

def neg(x: mt.AnyNumber, /) -> mt.AnyNumber:
    return mul(-1, x)

def sn_neg(x: mt.ScienceNumberProtocol, /) -> mt.AnyNumber:
    return sn_mul(-1, x)

def add(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_add(x, y)
    return _common.add(x, y)

@num.with_error_propagation(help.derivative_1, help.derivative_1, units_func=help.units_first)
def sn_add(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.add(*match_units(x, y))

def sub(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.sub(x, y)

@num.with_error_propagation(help.derivative_1, help.derivative_neg1, units_func=help.units_first)
def sn_sub(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.sub(*match_units(x, y))

def div(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_div(x, y)
    return _common.div(x, y)

@num.with_error_propagation(help.derivative_numerator, help.derivative_denominator, units_func=help.units_div)
def sn_div(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.div(x, y)

def mul(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_mul(x, y)
    return _common.mul(x, y)

@num.with_error_propagation(help.derivative_y, help.derivative_x, units_func=help.units_multiply)
def sn_mul(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.mul(x, y)

def sqrt(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_sqrt(x)
    return _common.sqrt(x)

@num.with_error_propagation(lambda x: 1 / (x ** 0.5), units_func=lambda x: f"({help.units_first(x)}^0.5)")
def sn_sqrt(x: mt.AnyNumber, /):
    return _common.sqrt(x)

def exp(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_exp(x)
    return _common.exp(x)

@num.with_error_propagation(lambda x: _common.exp(x), units_func = lambda x: f"(e^{help.units_first(x)})")
def sn_exp(x: mt.AnyNumber, /):
    return _common.exp(x)

def ln(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_ln(x)
    return _common.ln(x)

@num.with_error_propagation(lambda x: 1 / x, units_func=lambda x: f"(ln {help.units_first(x)}")
def sn_ln(x: mt.AnyNumber, /):
    return _common.ln(x)

def log10(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_log10(x)
    return _common.log10(x)

@num.with_error_propagation(lambda x: 1 / (x * ln(decimal.Decimal(10))), units_func=lambda x: f"(log10 {help.units_first(x)}")
def sn_log10(x: mt.AnyNumber, /):
    return _common.log10(x)

def log2(x: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_log2(x)
    return _common.log2(x)

@num.with_error_propagation(lambda x: 1 / (x * ln(decimal.Decimal(2))), units_func=lambda x: f"(log2 {help.units_first(x)}")
def sn_log2(x: mt.AnyNumber, /):
    return _common.log2(nominal_value(x))

def log(x: mt.AnyNumber, base: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(base):
        return sn_log(x, base)
    return _common.log(x, base)

@num.with_error_propagation(lambda x, b: 1 / (x * ln(decimal.Decimal(b))), units_func=lambda x, y: f"(log{y} {help.units_first(x)}")
def sn_log(x: mt.AnyNumber, base: mt.AnyNumber, /):
    return _common.log(x, base)

def pow_(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_pow(x, y)
    return _common.pow(x, y)

@num.with_error_propagation(help.derivative_power_base, help.derivative_power_exponent, units=help.units_power)
def sn_pow(x: mt.AnyNumber, y: mt.AnyNumber, /):
    return _common.pow(x, y)

def modulo(x: mt.AnyNumber, y: mt.AnyNumber, /):
    if mt.is_science_number(x):
        return sn_modulo(x, to_exact_int(y))
    return nominal_value(x) % to_exact_int(y)

# Note: derivative is undefined near integer multiples of y.
# perhaps more care should be taken here later?
# for example, consider 4.01 mod 2 with s=0.1
# the result here is 0.01 with s=0.1
# but 4.01-1s mod 2 = 3.91 mod 2 = 1.91
# this result is represented as -0.09 in our result

# since we've taken mod 2, the result is cyclical and
# if the error causes the result to be close enough to
# zero that it crosses the boundary, we may have issues

# anyone using modulo with scinum should be aware of this!

# however, given (x, s) mod n, if both of these hold:
#   x mod n - 3s >= 0
#   x mod n + 3s < n
# then the result is not impacted by this issue
# (i.e. it's only an issue near 0 or near n)
@num.with_error_propagation(help.derivative_1, None)
def sn_modulo(x: mt.AnyNumber, y: int, /):
    return nominal_value(x) % y

def is_exact_int(x: mt.AnyNumber) -> bool:
    try:
        _ = to_exact_int(x)
        return True
    except MathTypeError:
        return False

def to_exact_int(x: mt.AnyNumber) -> int:
    nv = nominal_value(x)
    try:
        return int(nv.to_integral_exact())
    except AttributeError:
        if nv.is_integer():
            return int(nv)
        raise MathTypeError(f"Invalid number for exact integer: {nv}")

def is_zero(x: mt.AnyNumber) -> bool:
    return _common.is_zero(x)

def copy_sign(take_value: mt.AnyNumber, take_sign: mt.AnyNumber) -> mt.AnyNumber:
    sign_tv = sign(take_value)
    sign_ts = sign(take_sign)
    if (sign_tv == 1 and sign_ts == -1) or (sign_ts == -1 and sign_tv == 1):
        return neg(take_value)
    return take_value

def sign(x: mt.AnyNumber) -> int | None:
    nv = nominal_value(x)
    if _common.is_nan(x):
        return None
    if is_zero(nv): return 0
    elif nv > 0: return 1
    else: return -1

def is_close(x: mt.AnyNumber,
             y: mt.AnyNumber,
             /, *,
             rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
             abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    return _is_close(*match_convert(*match_units(x, y), rel_tol, abs_tol))


def eq(x: mt.AnyNumber,
       y: mt.AnyNumber, /) -> bool:
    if mt.is_science_number(x) and mt.is_science_number(y):
        y = y.convert(x.units)
    return nominal_value(x) == nominal_value(y)


def gt(x: mt.AnyNumber,
       y: mt.AnyNumber,
       /, *,
       rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
       abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    return not lte(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def gte(x: mt.AnyNumber,
        y: mt.AnyNumber,
        /, *,
        rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
        abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    nv_x, nv_y, rt, at = match_convert(*match_units(x, y), rel_tol, abs_tol)
    return nv_x > nv_y or _is_close(nv_x, nv_y, rt, at)

def lt(x: mt.AnyNumber,
       y: mt.AnyNumber,
       /, *,
       rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
       abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    return not gte(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def lte(x: mt.AnyNumber,
        y: mt.AnyNumber,
        /, *,
        rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
        abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    nv_x, nv_y, rt, at = match_convert(*match_units(x, y), rel_tol, abs_tol)
    return nv_x < nv_y or _is_close(nv_x, nv_y, rt, at)


def _is_close(x, y, rel_tol, abs_tol) -> bool:
    abs_diff = abs(x - y)
    abs_x = abs(x)
    abs_y = abs(y)
    if abs_x > abs_y:
        actual_rel_tol = abs_x * rel_tol
    else:
        actual_rel_tol = abs_y * rel_tol
    if actual_rel_tol > abs_tol:
        abs_tol = actual_rel_tol
    return abs_diff <= abs_tol


def between(min_v: mt.AnyNumber,
            x: mt.AnyNumber,
            max_v: mt.AnyNumber,
            /, *,
            include_lower_bound: bool = True,
            include_upper_bound: bool = True,
            rel_tol: mt.NonScienceNumber = DEFAULT_REL_TOL,
            abs_tol: mt.NonScienceNumber = DEFAULT_ABS_TOL) -> bool:
    nv_min, nv_x, nv_max, rt, at = match_convert(*match_units(min_v, x, max_v), rel_tol, abs_tol)
    if include_lower_bound:
        if nv_x < nv_min and not _is_close(nv_x, nv_min, rt, at):
            return False
    elif nv_x < nv_min or _is_close(nv_x, nv_min, rt, at):
        return False
    if include_upper_bound:
        if nv_x > nv_max and not _is_close(nv_x, nv_max, rt, at):
            return False
    elif nv_x > nv_max or _is_close(nv_x, nv_max, rt, at):
        return False
    return True


def sum_(arg1: mt.NumberOrIterable, /, *args: mt.NumberOrIterable) -> mt.AnyNumber:
    sn, dc, ns, flint = separate_by_type(collapse((arg1, args)))
    current_sum = 0
    if len(dc) > 0:
        dc.extend(decimal.Decimal(x) for x in ns)
        dc.append(decimal.Decimal(_math.fsum(flint)))
        current_sum = sum(dc)
    else:
        flint.extend(float(x) for x in ns)
        if len(flint) > 0:
            current_sum = _math.fsum(flint)
    if len(sn) > 0:
        for s in sn:
            current_sum = sn_add(s, current_sum)
    return current_sum


def product(arg1: mt.NumberOrIterable, /, *args: mt.NumberOrIterable) -> mt.AnyNumber:
    sn, dc, ns, flint = separate_by_type(collapse((arg1, args)))
    current_product = 1
    if len(dc) > 0:
        dc.extend(decimal.Decimal(x) for x in ns)
        dc.append(decimal.Decimal(_math.prod(flint)))
        current_product = _math.prod(dc)
    else:
        flint.extend(float(x) for x in ns)
        if len(flint) > 0:
            current_product = _math.prod(flint)
    if len(sn) > 0:
        for s in sn:
            current_product = sn_mul(s, current_product)
    return current_product

def max_(arg1: mt.NumberOrIterable, /, *args: mt.NumberOrIterable) -> mt.AnyNumber:
    sn, dc, ns, flint = separate_by_type(collapse((arg1, args)))
    if len(dc) > 0:
        dc.extend(decimal.Decimal(x) for x in ns)
    else:
        flint.extend(float(x) for x in ns)
    return max(*sn, *dc, *flint)


def min_(arg1: NumberOrIterable, /, *args: NumberOrIterable) -> mt.AnyNumber:
    sn, dc, ns, flint = separate_by_type(collapse((arg1, args)))
    if len(dc) > 0:
        dc.extend(decimal.Decimal(x) for x in ns)
    else:
        flint.extend(float(x) for x in ns)
    return min(*sn, *dc, *flint)
