import decimal
import math as _math
import typing as t
from medsutil.math import types as mt


def ln(x):
    try:
        return x.ln()
    except AttributeError:
        return _math.log(x)

def nominal_value(x: mt.AnyNumber) -> mt.BasicNumber:
    if mt.is_science_number(x):
        return x.nominal_value
    else:
        return t.cast(decimal.Decimal | int | float, x)


def is_complex(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    return isinstance(x, complex)


def is_infinity(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    try:
        return x.is_infinity()
    except AttributeError:
        return _math.isinf(float(x))


def is_nan(x: mt.AnyNumber) -> bool:
    x = nominal_value(x)
    try:
        return x.is_nan()
    except AttributeError:
        return _math.isnan(float(x))


def gt(x, y):
    return nominal_value(x) > nominal_value(y)


def lt(x, y):
    return nominal_value(x) < nominal_value(y)


def collapse(iterable: t.Iterable, collapse_types: tuple[type, ...] | None = None) -> t.Iterable:
    ct: tuple[type, ...] = (list, tuple, set) if collapse_types is None else collapse_types
    for x in iterable:
        if isinstance(x, ct):
            yield from collapse(x)
        else:
            yield x


def summation(x: t.Iterable[mt.AnyNumber], /):
    iterable = iter(x)
    first = next(iterable)
    def _all_items():
        yield first
        yield from x
    if isinstance(first, decimal.Decimal):
        return sum(_all_items())
    else:
        return _math.fsum(_all_items())


def product(x: t.Iterable[mt.AnyNumber], /):
    return _math.prod(x)


def convert[T](x: mt.AnyNumber | None | mt.Placeholder, type_: type[T]) -> T | None | mt.Placeholder:
    if x is None:
        return x
    elif x is mt.Placeholder:
        return x
    else:
        x = nominal_value(t.cast(mt.AnyNumber, x))
    if isinstance(x, type_):
        return x
    return type_(x)


def match_convert[T](x: mt.AnyNumber, y: mt.AnyNumber) -> tuple[T, T]:
    nom_x = nominal_value(x)
    nom_y = nominal_value(y)
    if isinstance(nom_x, decimal.Decimal) and not isinstance(nom_y, decimal.Decimal):
        return nom_x, decimal.Decimal(nom_y)
    elif isinstance(nom_y, decimal.Decimal) and not isinstance(nom_x, decimal.Decimal):
        return decimal.Decimal(nom_x), nom_y
    else:
        return nom_x, nom_y


def between(min_: mt.AnyNumber, x: mt.AnyNumber, max_: mt.AnyNumber) -> bool:
    nv_x = nominal_value(x)
    if nv_x < nominal_value(min_):
        return False
    if nv_x > nominal_value(max_):
        return False
    return True


ANY_NUM_OR_ITER = mt.AnyNumber | t.Iterable[mt.AnyNumber]


def sn_max(arg1: ANY_NUM_OR_ITER, /, *args: ANY_NUM_OR_ITER) -> mt.AnyNumber:
    max_: mt.AnyNumber | None = None
    for a in collapse((arg1, args)):
        if max_ is None or gt(a, max_):
            max_ = a
    if max_ is None:
        raise TypeError("Expected at least 1 argument, got 0")
    return max_


def sn_min(arg1: ANY_NUM_OR_ITER, /, *args: ANY_NUM_OR_ITER) -> mt.AnyNumber:
    min_: mt.AnyNumber | None = None
    for a in collapse((arg1, args)):
        if min_ is None or lt(a, min_):
            min_ = a
    if min_ is None:
        raise TypeError("Expected at least 1 argument, got 0")
    return min_
