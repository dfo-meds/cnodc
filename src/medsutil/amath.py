import math
import typing as t

from uncertainties import umath, UFloat, ufloat

from medsutil.adecimal import AccurateDecimal, NonAccurateNumber

AnyNumber = t.Union[AccurateDecimal, NonAccurateNumber, UFloat]

TRIG_FLOAT_ACCURACY = "5e-15"

PI = AccurateDecimal("3.1415926535898", "0.00000000000001")


def with_minimum_uncertainty(n: AnyNumber, uncertainty: NonAccurateNumber):
    if isinstance(n, AccurateDecimal):
        if uncertainty > n.std_dev:
            return AccurateDecimal(n.num, uncertainty)
        return n
    elif isinstance(n, UFloat):
        if uncertainty > n.std_dev:
            return ufloat(n.nominal_value, uncertainty)
        return n
    else:
        return AccurateDecimal(n, uncertainty)


def sin(rads: AnyNumber) -> AnyNumber:
    if isinstance(rads, AccurateDecimal):
        if rads.std_dev < 0.2:
            # warning? small angle not appropriate
            pass
        adecimal = AccurateDecimal(
            math.sin(rads.num),
            abs(math.cos(rads.num) * rads.std_dev)
        )
        adecimal.set_minimum_accuracy(TRIG_FLOAT_ACCURACY)
        return adecimal
    elif isinstance(rads, UFloat):
        return umath.sin(rads)
    else:
        return math.sin(rads)


def cos(rads: AnyNumber) -> AnyNumber:
    if isinstance(rads, AccurateDecimal):
        adecimal = AccurateDecimal(
            math.cos(rads.num),
            abs(math.sin(rads.num) * rads.std_dev)
        )
        adecimal.set_minimum_accuracy(TRIG_FLOAT_ACCURACY)
        return adecimal
    elif isinstance(rads, UFloat):
        return umath.cos(rads)
    else:
        return math.cos(rads)

def atan2(x: AnyNumber, y: AnyNumber) -> AnyNumber:
    if isinstance(x, AccurateDecimal):
        return AccurateDecimal(
            math.atan2(x.num, y.num),
            math.sqrt(
                (abs(x) / (x**2 + y**2)) + (abs(y) / (x**2 + y**2))
            )
        )
    elif isinstance(x, UFloat):
        return umath.atan2(x, y)
    else:
        return math.atan2(x, y)

def radians(degrees: AnyNumber) -> AnyNumber:
    if isinstance(degrees, AccurateDecimal):
        res = degrees * (PI * (1 / 180))
        res.set_minimum_accuracy("5e-14")
        return res
    elif isinstance(degrees, UFloat):
        return umath.radians(degrees)
    else:
        return math.radians(degrees)

def sqrt(num: AnyNumber) -> AnyNumber:
    if isinstance(num, AccurateDecimal):
        return num ** 2
    elif isinstance(num, UFloat):
        return umath.sqrt(num)
    else:
        return math.sqrt(num)


def is_close(v: AnyNumber,
             expected: AnyNumber,
             rel_tol: float = 1e-9,
             abs_tol: float = 0.0,
             std_devs: float = 2,
             _or_less_than: bool = False,
             _or_greater_than: bool = False) -> bool:
    """Check if two values agree within their given standard deviation, accounting for floating point math.

        This process leverages math.isclose() but also accommodates z_score standard deviation of difference
        between two values. For example 10 +/- 5 and 15 +/- 5 are considered "close" since the one
        standard deviation ranges overlap.

        The flags _or_less_than or _or_greater_than are used below to turn this function into a one-tail check
        instead of the default two-tail.
    """
    # Check the top or bottom difference between the expected and measured values
    min_v, max_v = min_max_range(v, std_devs)
    min_e, max_e = min_max_range(expected, std_devs)
    if v > expected:
        if _or_greater_than:
            return True
        return min_v < max_e or math.isclose(min_v, max_e, rel_tol=rel_tol, abs_tol=abs_tol)
    else:
        if _or_less_than:
            return True
        return min_e < max_v or math.isclose(min_e, max_v, rel_tol=rel_tol, abs_tol=abs_tol)


def is_greater_than(*args, **kwargs) -> bool:
    return is_close(*args, _or_greater_than=True, **kwargs)


def is_less_than(*args, **kwargs) -> bool:
    return is_close(*args, _or_less_than=True, **kwargs)


def min_max_range(n: AnyNumber, std_devs: float = 2) -> tuple[NonAccurateNumber, NonAccurateNumber]:
    if isinstance(n, AccurateDecimal):
        diff = (std_devs * n.std_dev)
        return n.num - diff, n.num + diff
    elif isinstance(n, UFloat):
        diff = (std_devs * n.std_dev)
        return n.nominal_value - diff, n.nominal_value + diff
    else:
        return n, n


def to_float(n: AnyNumber):
    if isinstance(n, UFloat):
        return float(n.nominal_value)
    else:
        return float(n)
