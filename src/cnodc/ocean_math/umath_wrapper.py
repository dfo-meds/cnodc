"""
This module just wraps uncertainties.umath for the most part, but provides
more helpful function signatures for auto-completion.
"""
import math
import typing as t
from uncertainties import UFloat, ufloat
import uncertainties.umath as umath

FLOAT = t.Union[float, UFloat, int]


def adjust_uncertainty(x: FLOAT, inherent_uncertainty: float) -> UFloat:
    """Adjust the uncertainty to be at least the given uncertainty value."""
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


def is_close(v: FLOAT,
             expected: FLOAT,
             rel_tol: float = 1e-9,
             abs_tol: float = 0.0,
             _or_less_than: bool = False,
             _or_greater_than: bool = False) -> bool:
    """Check if two values agree within their given standard deviation, accounting for floating point math.

        This process leverages math.isclose() but also accommodates one standard deviation of difference
        between two values. For example 10 +/- 5 and 15 +/- 5 are considered "close" since the one
        standard deviation ranges overlap.

        The flags _or_less_than or _or_greater_than are used below to turn this function into a one-tail check
        instead of the default two-tail.
    """
    # Check the top or bottom difference between the expected and measured values
    if v > expected:
        if _or_greater_than:
            return True
        bv_lower = v.nominal_value - v.std_dev if isinstance(v, UFloat) else v
        expected_upper = expected.nominal_value + expected.std_dev if isinstance(expected, UFloat) else expected
        return bv_lower < expected_upper or math.isclose(bv_lower, expected_upper, rel_tol=rel_tol, abs_tol=abs_tol)
    else:
        if _or_less_than:
            return True
        bv_upper = v.nominal_value + v.std_dev if isinstance(v, UFloat) else v
        expected_lower = expected.nominal_value - expected.std_dev if isinstance(expected, UFloat) else expected
        return bv_upper > expected_lower or math.isclose(bv_upper, expected_lower, rel_tol=rel_tol, abs_tol=abs_tol)


def is_greater_than(*args, **kwargs) -> bool:
    return is_close(*args, _or_greater_than=True, **kwargs)


def is_less_than(*args, **kwargs) -> bool:
    return is_close(*args, _or_less_than=True, **kwargs)


def to_float(f: FLOAT) -> float:
    """Convert a float to"""
    if isinstance(f, UFloat):
        return f.nominal_value
    else:
        return f
