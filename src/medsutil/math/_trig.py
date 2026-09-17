import decimal
import math as _math
import typing as t

import medsutil.math.types as mt
import medsutil.math.helpers as help
import medsutil.math.numbers as num
import medsutil.math._constants as mc
import medsutil.math._functions as mb
from medsutil.math import _common, epsilon_sqrt
from medsutil.math.types import MathTypeError


def cos(x):
    if mt.is_science_number(x):
        return sn_cos(x)
    elif isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _cosine_taylor_series)
    else:
        return _math.cos(x)

@num.with_error_propagation(lambda x: sin(_common.nominal_value(x)), units_func=lambda x: "1")
def sn_cos(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return cos(_common.nominal_value(x))

def _cosine_taylor_series(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n)) / ((2n)!)
    if n == 0:
        return lambda x: decimal.Decimal(1)
    else:
        return lambda x: ((-1 if n % 2 == 1 else 1) * (x ** (2 * n))) / _math.factorial(2 * n)



def sin(x):
    if mt.is_science_number(x):
        return sn_sin(x)
    elif isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _sine_taylor_series)
    else:
        return _math.sin(x)

@num.with_error_propagation(lambda x: -1 * cos(_common.nominal_value(x)), units_func=lambda x: "1")
def sn_sin(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return sin(_common.nominal_value(x))

def _sine_taylor_series(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n+1)) / ((2n+1)!)
    if n == 0:
        return lambda x: x
    else:
        return lambda x: ((-1 if n % 2 == 1 else 1) * (x ** ((2 * n) + 1))) / _math.factorial((2 * n) + 1)


def tan(x):
    return mb.div(sin(x), cos(x))

def sn_tan(x):
    return tan(x)


def asin(x):
    if mt.is_science_number(x):
        return sn_asin(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arcsin")
        return help.newton_raphson_approximation(x, x, _asin_newton_raphson)
    return _math.asin(x)

def _asin_derivative(x):
    if x == 1:
        x = x - mc.epsilon(type(x))
    elif x == -1:
        x = x + mc.epsilon(type(x))
    return 1 / ((1 - (x ** 2)) ** 0.5)

@num.with_error_propagation(_asin_derivative, units_func=lambda x: "radians")
def sn_asin(x):
    return asin(_common.nominal_value(x))

def _asin_newton_raphson(guess: decimal.Decimal, x: decimal.Decimal) -> decimal.Decimal:
    g_sin = help.taylor_series_approximation(guess, _sine_taylor_series)
    g_cos = help.taylor_series_approximation(guess, _cosine_taylor_series)
    return (g_sin - x) / g_cos

def _asin_taylor_series(n: int):
    # (x^2n+1)
    """
    (1/2 * 3/4...)
    """
    if n == 0:
        return lambda x: x
    else:
        a = _math.factorial(2 * n)
        b = _math.factorial(n) ** 2
        c = 2 ** (2 * n)
        d = (2 * n) + 1
        coeff = decimal.Decimal((a / (b * c * d)))
        return lambda x: coeff * (x ** d)


def acos(x):
    if mt.is_science_number(x):
        return sn_acos(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arccos")
        # acos(x) = pi/2 - asin(x)
        return (mc.pi(decimal.Decimal) / decimal.Decimal(2)) - help.newton_raphson_approximation(x, x, _asin_newton_raphson)
    return _math.acos(x)

@num.with_error_propagation(_asin_derivative, units_func=lambda x: "radians")
def sn_acos(x):
    return acos(_common.nominal_value(x))



def atan(x):
    # should be (-pi/2, pi/2]
    if mt.is_science_number(x):
        return sn_atan(x)
    if isinstance(x, decimal.Decimal):
        if _common.is_infinity(x):
            return mc.pi(decimal.Decimal) / decimal.Decimal(2)
        else:
            return help.newton_raphson_approximation(x, decimal.Decimal("0"), _atan_newton_raphson)
    return _math.atan(x)

@num.with_error_propagation(lambda x: 1 / (1 + (x ** 2)), units_func=lambda x: "radians")
def sn_atan(x):
    return atan(_common.nominal_value(x))

def _atan_newton_raphson(guess: decimal.Decimal, x: decimal.Decimal) -> decimal.Decimal:
    g_sin = help.taylor_series_approximation(guess, _sine_taylor_series)
    g_cos = help.taylor_series_approximation(guess, _cosine_taylor_series)
    return (g_sin * g_cos) - (g_cos * g_cos * x)


def _atan_small_derivative(n: int):
    # (-1^n)(x^2n+1)/(2n+1)
    return lambda x: ((-1 ** n) * (x ** ((2 * n) + 1))) / ((2 * n) + 1)

def _atan_large_derivative(n: int):
    # (-1)(-1^n)(x^-(2n+1))/(2n+1)
    return lambda x: ((-1 ** (n + 1)) * (x ** (-1 * ((2 * n) + 1)))) / ((2 * n) + 1)

def atan2(y, x):
    # returns (-pi, pi)
    return _fix_quadrant(atan(mb.div(y, x)), y, x)

def _fix_quadrant(_atan, y, x):
    if x >= 0:
        # already correctly in first or fourth quadrant, return
        return _atan

    # check our type to get a compatible PI
    if mt.is_science_number(_atan):
        pi_type = type(_atan.nominal_value)
    else:
        pi_type = type(_atan)

    if y >= 0:
        # our atan gives a value in Q2, but we know its in Q4, rotate ccw
        return mb.add(_atan, mc.pi(pi_type))

    else:
        # our atan gives a value in Q1, but we know its in Q3, rotate cw
        return mb.sub(_atan, mc.pi(pi_type))

def sn_atan2(y, x):
    return _fix_quadrant(sn_atan(y/x), y, x)

def degrees(x):
    if mt.is_science_number(x):
        return sn_degrees(x)
    if isinstance(x, decimal.Decimal):
        return (decimal.Decimal(180) / mc.pi(decimal.Decimal)) * x
    return _math.degrees(x)

@num.with_error_propagation(lambda x: 180 / mc.pi(type(x)), units_func=lambda x: "degrees")
def sn_degrees(x):
    return degrees(_common.nominal_value(x))


def radians(x):
    if mt.is_science_number(x):
        return sn_radians(x)
    if isinstance(x, decimal.Decimal):
        return (mc.pi(decimal.Decimal) / decimal.Decimal(180)) * x
    return _math.radians(x)

@num.with_error_propagation(lambda x: mc.pi(type(x)) / 180, units_func=lambda x: "radians")
def sn_radians(x):
    return radians(_common.nominal_value(x))

