import decimal
import math as _math
import sys
import typing as t

from medsutil.math import types as mt
from medsutil.math.helpers import taylor_series_approximation
import medsutil.math.helpers as help

def tan(x):
    return sin(x) / cos(x)

def tau[T](type_: type[T] = None) -> T:
    return pi(type_) * 2

def inf[T](type_: type[T] = None) -> T:
    if isinstance(type_, decimal.Decimal):
        return decimal.Decimal("Infinity")
    return _math.inf

def e(type_: type = None) -> float | decimal.Decimal:
    # TODO: caching by context significance?
    if type_ is decimal.Decimal:
        return help.taylor_series_approximation(decimal.Decimal(1), _euler_e)
    return _math.e

def pi[T](type_: type[T] = None) -> T:
    # TODO: caching by context significance?
    if type_ is decimal.Decimal:
        return help.taylor_series_approximation(decimal.Decimal(1), _leibniz_pi)
    return _math.pi

def epsilon[T](type_: type[T] = None) -> T:
    # TODO: caching by context significance?
    if type_ is decimal.Decimal:
        return decimal.Decimal(f"1e-{decimal.getcontext().prec}").sqrt()
    else:
        return _math.sqrt(sys.float_info.epsilon)

def summation(x: t.Iterable):
    iterable = iter(x)
    first = next(iterable)
    def _all_items():
        yield first
        yield from x
    if isinstance(first, decimal.Decimal):
        return sum(_all_items())
    else:
        return _math.fsum(_all_items())

def product(x: t.Iterable):
    return _math.prod(x)

def degrees(x):
    if isinstance(x, decimal.Decimal):
        return (decimal.Decimal(180) / pi(decimal.Decimal)) * x
    return _math.degrees(x)

def radians(x):
    if isinstance(x, decimal.Decimal):
        return (pi(decimal.Decimal) / decimal.Decimal(180)) * x
    return _math.radians(x)

def asin(x):
    x = nominal_value(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arcsin")
        return help.taylor_series_approximation(x, _asin_derivative)
    return _math.asin(x)

def acos(x):
    x = nominal_value(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arccos")
        # acos(x) = pi/2 - asin(x)
        return (pi(decimal.Decimal) / decimal.Decimal(2)) - help.taylor_series_approximation(x, _asin_derivative)
    return _math.acos(x)

def atan(x):
    # should be (-pi/2, pi/2]
    x = nominal_value(x)
    if isinstance(x, decimal.Decimal):
        if is_infinity(x):
            return pi(decimal.Decimal) / decimal.Decimal(2)
        if x > 1:
            return (pi(decimal.Decimal) / decimal.Decimal(2)) + help.taylor_series_approximation(x, _atan_large_derivative)
        elif x < 1:
            return (pi(decimal.Decimal) / decimal.Decimal(2)) + help.taylor_series_approximation(x, _atan_large_derivative)
        else:
            return taylor_series_approximation(x, _atan_small_derivative)
    return _math.atan(x)

def atan2(y, x):
    # returns (-pi, pi)
    _atan: float | decimal.Decimal = atan(nominal_value(y)/nominal_value(x))

    if x >= 0:
        # already correctly in first or fourth quadrant, return
        return _atan

    elif y >= 0:
        # our atan gives a value in Q2, but we know its in Q4, rotate ccw
        return _atan + pi(type(_atan))

    else:
        # our atan gives a value in Q1, but we know its in Q3, rotate cw
        return _atan - pi(type(_atan))

def cos(x):
    x = nominal_value(x)
    if isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _cosine_derivative)
    return _math.cos(x)

def sin[T](x: T) -> T:
    x = nominal_value(x)
    if isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _sine_derivative)
    return _math.cos(x)

def sqrt(x):
    x = nominal_value(x)
    try:
        return x.sqrt()
    except AttributeError:
        return _math.sqrt(x)

def exp(x):
    x = nominal_value(x)
    try:
        return x.exp()
    except AttributeError:
        return _math.exp(x)

def is_nan(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    try:
        return x.is_nan()
    except AttributeError:
        return _math.isnan(float(x))

def is_infinity(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    try:
        return x.is_infinity()
    except AttributeError:
        return _math.isinf(float(x))

def is_complex(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    return isinstance(x, complex)

def ln(x):
    x = nominal_value(x)
    try:
        return x.ln()
    except AttributeError:
        return _math.log(x)

def log10(x):
    x = nominal_value(x)
    try:
        return x.log10()
    except AttributeError:
        return _math.log10(x)

def log2(x):
    x = nominal_value(x)
    try:
        return x.log10() / decimal.Decimal(2).log10()
    except AttributeError:
        return _math.log2(x)

def log(x, base):
    x, base = match_convert(x, base)
    try:
        return x.log10() / base.log10()
    except AttributeError:
        return _math.log(x, base)

def pow(x, y):
    # todo: handle cases where types don't match
    x, y = match_convert(x, y)
    return x ** y

def nominal_value(x: mt.AnyNumber):
    if hasattr(x, 'nominal_value'):
        return x.nominal_value
    return x

def convert[T](x: mt.AnyNumber | None | mt.Placeholder, type_: type[T]):
    if x is None:
        return x
    elif x is mt.Placeholder:
        return x
    else:
        x = nominal_value(x)
    if isinstance(x, type_):
        return x
    return type_(x)

def match_convert(x, y) -> tuple:
    nom_x = nominal_value(x)
    nom_y = nominal_value(y)
    if isinstance(nom_x, (decimal.Decimal, str)) or isinstance(nom_y, (decimal.Decimal, str)):
        return convert(nom_x, decimal.Decimal), convert(nom_y, decimal.Decimal)
    return nom_x, nom_y

def _cosine_derivative(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n)) / ((2n)!)
    if n == 0:
        return lambda x: decimal.Decimal(1)
    else:
        return lambda x: ((-1 ** n) * (x ** (2 * n))) / _math.factorial(2 * n)

def _sine_derivative(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n+1)) / ((2n+1)!)
    if n == 0:
        return lambda x: x
    else:
        return lambda x: ((-1 ** n) * (x ** ((2 * n) + 1))) / _math.factorial((2 * n) + 1)

def _euler_e(n: int):
    return lambda x: decimal.Decimal(1) / decimal.Decimal(_math.factorial(n))

def _leibniz_pi(n: int):
    # note, x doesn't matter but this lets us use the same approximation function
    return lambda x: decimal.Decimal(1) / decimal.Decimal((2 * n) + 1)

def _asin_derivative(n: int):
    # (x^2n+1)
    """
    (1/2 * 3/4...)
    """
    if n == 0:
        return lambda x: x
    else:
        exponent = (2 * n) + 1
        num = decimal.Decimal(_math.prod(((2 * a) + 1 for a in range(0, n))))
        denom = decimal.Decimal(_math.prod((2 * (a + 1)) for a in range(0, n)) * exponent)
        return lambda x: (num/denom) * (x ** exponent)

def _atan_small_derivative(n: int):
    # (-1^n)(x^2n+1)/(2n+1)
    return lambda x: ((-1 ** n) * (x ** ((2 * n) + 1))) / ((2 * n) + 1)

def _atan_large_derivative(n: int):
    # (-1)(-1^n)(x^-(2n+1))/(2n+1)
    return lambda x: ((-1 ** (n + 1)) * (x ** (-1 * ((2 * n) + 1)))) / ((2 * n) + 1)
