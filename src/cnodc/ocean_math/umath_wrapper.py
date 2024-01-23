"""
This module just wraps uncertainties.umath for the most part, but provides
more helpful function signatures for auto-completion.
"""
import typing as t
from uncertainties import UFloat, ufloat
import uncertainties.umath as umath

FLOAT = t.Union[float, UFloat, int]


def adjust_uncertainty(x: FLOAT, inherent_uncertainty: float) -> UFloat:
    if isinstance(x, UFloat):
        if x.std_dev < inherent_uncertainty:
            return ufloat(x.nominal_value, inherent_uncertainty)
        return x
    else:
        return ufloat(x, inherent_uncertainty)

def radians(degrees: FLOAT) -> FLOAT:
    return umath.radians(degrees)


def sin(radians: FLOAT) -> FLOAT:
    return umath.sin(radians)


def cos(radians: FLOAT) -> FLOAT:
    return umath.cos(radians)


def atan2(x: FLOAT, y: FLOAT) -> FLOAT:
    return umath.atan2(x, y)


def sqrt(x: FLOAT) -> FLOAT:
    return umath.sqrt(x)

