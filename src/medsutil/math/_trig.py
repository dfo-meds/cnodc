import decimal
import math as _math
import typing as t

import medsutil.math.types as mt
import medsutil.math.helpers as help
import medsutil.math.numbers as num
import medsutil.math._constants as mc
import medsutil.math._basics as mb

def cos(x):
    if mt.is_science_number(x):
        return sn_cos(x)
    elif isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _cosine_taylor_series)
    else:
        return _math.cos(x)

@num.as_science_number(lambda x: sin(x), units_func=lambda x: "1")
def sn_cos(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return cos(mb.nominal_value(x))

def _cosine_taylor_series(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n)) / ((2n)!)
    if n == 0:
        return lambda x: decimal.Decimal(1)
    else:
        return lambda x: ((-1 ** n) * (x ** (2 * n))) / _math.factorial(2 * n)



def sin(x):
    if mt.is_science_number(x):
        return sn_sin(x)
    elif isinstance(x, decimal.Decimal):
        return help.taylor_series_approximation(x, _sine_taylor_series)
    else:
        return _math.sin(x)

@num.as_science_number(lambda x: -1 * cos(x), units_func=lambda x: "1")
def sn_sin(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return sin(mb.nominal_value(x))

def _sine_taylor_series(n: int) -> t.Callable[[decimal.Decimal], decimal.Decimal]:
    # (-1^n)(x^(2n+1)) / ((2n+1)!)
    if n == 0:
        return lambda x: x
    else:
        return lambda x: ((-1 ** n) * (x ** ((2 * n) + 1))) / _math.factorial((2 * n) + 1)


def tan(x):
    if mt.is_science_number(x):
        return sn_tan(x)
    return sin(x) / cos(x)

def sn_tan(x):
    return sn_sin(mb.nominal_value(x)) / sn_cos(mb.nominal_value(x))


def asin(x):
    if mt.is_science_number(x):
        return sn_asin(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arcsin")
        return help.taylor_series_approximation(x, _asin_taylor_series)
    return _math.asin(x)

@num.as_science_number(lambda x: 1 / ((1 - (x ** 2)) ** 0.5), units_func=lambda x: "radians")
def sn_asin(x):
    return asin(mb.nominal_value(x))

def _asin_taylor_series(n: int):
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


def acos(x):
    if mt.is_science_number(x):
        return sn_acos(x)
    if isinstance(x, decimal.Decimal):
        if x > 1 or x < -1:
            raise ValueError("Invalid input for arccos")
        # acos(x) = pi/2 - asin(x)
        return (mc.pi(decimal.Decimal) / decimal.Decimal(2)) - help.taylor_series_approximation(x, _asin_taylor_series)
    return _math.acos(x)

@num.as_science_number(lambda x: 1 / ((1 - (x ** 2)) ** 0.5), units_func=lambda x: "radians")
def sn_acos(x):
    return acos(mb.nominal_value(x))



def atan(x):
    # should be (-pi/2, pi/2]
    if mt.is_science_number(x):
        return sn_atan(x)
    if isinstance(x, decimal.Decimal):
        if mb.is_infinity(x):
            return mc.pi(decimal.Decimal) / decimal.Decimal(2)
        if x > 1:
            return (mc.pi(decimal.Decimal) / decimal.Decimal(2)) + help.taylor_series_approximation(x, _atan_large_derivative)
        elif x < 1:
            return (mc.pi(decimal.Decimal) / decimal.Decimal(-2)) + help.taylor_series_approximation(x, _atan_large_derivative)
        else:
            return help.taylor_series_approximation(x, _atan_small_derivative)
    return _math.atan(x)

@num.as_science_number(lambda x: 1 / ((1 + (x ** 2)) ** 0.5), units_func=lambda x: "radians")
def sn_atan(x):
    return atan(mb.nominal_value(x))

def _atan_small_derivative(n: int):
    # (-1^n)(x^2n+1)/(2n+1)
    return lambda x: ((-1 ** n) * (x ** ((2 * n) + 1))) / ((2 * n) + 1)

def _atan_large_derivative(n: int):
    # (-1)(-1^n)(x^-(2n+1))/(2n+1)
    return lambda x: ((-1 ** (n + 1)) * (x ** (-1 * ((2 * n) + 1)))) / ((2 * n) + 1)

def atan2(y, x):
    # returns (-pi, pi)
    return _fix_quadrant(atan(y/x), y, x)

def _fix_quadrant(_atan, y, x):
    if x >= 0:
        # already correctly in first or fourth quadrant, return
        return _atan

    # check our type to get a compatible PI
    if mt.is_science_number(_atan):
        pi_type = _atan.num_type
    else:
        pi_type = type(_atan)

    if y >= 0:
        # our atan gives a value in Q2, but we know its in Q4, rotate ccw
        return _atan + mc.pi(pi_type)

    else:
        # our atan gives a value in Q1, but we know its in Q3, rotate cw
        return _atan - mc.pi(pi_type)

def sn_atan2(y, x):
    return _fix_quadrant(sn_atan(y/x), y, x)

def degrees(x):
    if mt.is_science_number(x):
        return sn_degrees(x)
    if isinstance(x, decimal.Decimal):
        return (decimal.Decimal(180) / mc.pi(decimal.Decimal)) * x
    return _math.degrees(x)

@num.as_science_number(lambda x: 180 / mc.pi(decimal.Decimal), units_func=lambda x: "degrees")
def sn_degrees(x):
    return degrees(mb.nominal_value(x))


def radians(x):
    if mt.is_science_number(x):
        return sn_radians(x)
    if isinstance(x, decimal.Decimal):
        return (mc.pi(decimal.Decimal) / decimal.Decimal(180)) * x
    return _math.radians(x)

@num.as_science_number(lambda x: mc.pi(decimal.Decimal) / 180, units_func=lambda x: "radians")
def sn_radians(x):
    return radians(mb.nominal_value(x))

