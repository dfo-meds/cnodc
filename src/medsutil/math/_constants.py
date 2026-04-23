import decimal
import math as _math
import sys
import medsutil.math.helpers as help


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

def _euler_e(n: int):
    return lambda x: decimal.Decimal(1) / decimal.Decimal(_math.factorial(n))

def _leibniz_pi(n: int):
    # note, x doesn't matter but this lets us use the same approximation function
    return lambda x: decimal.Decimal(1) / decimal.Decimal((2 * n) + 1)
