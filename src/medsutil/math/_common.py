import decimal
import math as _math
import typing as t

from medsutil.math import types as mt


def ln(x: mt.BasicNumber) -> mt.BasicNumber:
    try:
        return x.ln()
    except AttributeError:
        return _math.log(x)


def nominal_value(x: mt.AnyNumber, str_to_float: bool = False) -> mt.BasicNumber:
    if mt.is_science_number(x):
        return x.nominal_value
    elif isinstance(x, str):
        if "." in x or "e" in x or "E" in x:
            return decimal.Decimal(x) if not str_to_float else float(x)
        else:
            return int(x)
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


def convert[T](x: mt.AnyNumber | mt.NumberString | None | type[mt.Placeholder], type_: type[T]) -> T | None | mt.Placeholder:
    if x is None or x is mt.Placeholder:
        return x
    else:
        x = nominal_value(t.cast(mt.AnyNumber, x))
    if isinstance(x, type_):
        return x
    return type_(x)


def match_convert[T](*args: mt.AnyNumber | None | type[mt.Placeholder]) -> tuple[T, ...]:
    nominals = tuple(nominal_value(x) for x in args)
    has_decimals = any(isinstance(x, decimal.Decimal) for x in nominals)
    if has_decimals:
        return tuple(
            convert(n_x, decimal.Decimal)
            if not isinstance(n_x, (int, decimal.Decimal, None, mt.Placeholder)) else
            n_x
            for n_x in nominals
        )
    else:
        return tuple(
            convert(t.cast(mt.NumberString, n_x), float)
            if isinstance(n_x, str) else
            n_x
            for n_x in nominals
        )
