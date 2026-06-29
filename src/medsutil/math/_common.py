import decimal
import math as _math
import typing as t

from medsutil.math import types as mt
from medsutil.math.types import MathTypeError

def is_zero(x) -> bool:
    x = nominal_value(x)
    try:
        return x.is_zero()
    except AttributeError:
        return x == 0


def gt(x, y) -> bool:
    x_n, y_n = match_convert(x, y)
    return x_n > y_n

def lt(x, y) -> bool:
    x_n, y_n = match_convert(x, y)
    return x_n < y_n

def add(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.BasicNumber:
    x_n, y_n = match_convert(x, y)
    return x_n + y_n

def sub(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.BasicNumber:
    x_n, y_n = match_convert(x, y)
    return x_n - y_n

def div(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.BasicNumber:
    x_n, y_n = match_convert(x, y)
    return x_n / y_n

def mul(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.BasicNumber:
    nx, ny = match_convert(x, y)
    return nx * ny

def pow(x: mt.AnyNumber, y: mt.AnyNumber, /) -> mt.BasicNumber:
    nx, ny = match_convert(x, y)
    return nx ** ny

def exp(x: mt.AnyNumber, /) -> mt.BasicNumber:
    x = nominal_value(x)
    try:
        return x.exp()
    except AttributeError:
        return _math.exp(x)

def sqrt(x: mt.AnyNumber, /) -> mt.BasicNumber:
    x = nominal_value(x)
    try:
        return x.sqrt()
    except AttributeError:
        return _math.sqrt(x)

def ln(x: mt.AnyNumber, /) -> mt.BasicNumber:
    x = nominal_value(x)
    try:
        return x.ln()
    except AttributeError:
        return _math.log(x)

def log10(x: mt.AnyNumber, /) -> mt.BasicNumber:
    x = nominal_value(x)
    try:
        return x.log10()
    except AttributeError:
        return _math.log10(x)

def log2(x: mt.AnyNumber, /) -> mt.BasicNumber:
    x = nominal_value(x)
    try:
        l10 = x.log10()
        return l10 / decimal.Decimal(2).log10()
    except AttributeError:
        return _math.log2(x)

def log(x: mt.AnyNumber, base: mt.AnyNumber, /) -> mt.BasicNumber:
    x, base = match_convert(x, base)
    try:
        l10 = x.log10()
        return l10 / base.log10()
    except AttributeError:
        return _math.log(float(x), float(base))


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


def collapse(iterable: t.Iterable) -> t.Iterable:
    for x in iterable:
        if isinstance(x, t.Iterable) and not isinstance(x, str):
            yield from collapse(x)
        else:
            yield x


def nominal_value(x: mt.AnyNumber | mt.NumberString, to_float: bool = True) -> mt.BasicNumber:
    if mt.is_science_number(x):
        x = x.nominal_value
    if isinstance(x, str):
        if "." in x or "e" in x or "E" in x:
            return decimal.Decimal(x) if not to_float else float(x)
        else:
            return int(x)
    elif mt.is_supported(x):
        return x
    else:
        raise MathTypeError(f"{x.__class__.__name__} is not supported")

def convert[T](x: mt.AnyNumber | None | type[mt.Placeholder], to_float: bool = True) -> T | None | mt.Placeholder:
    if x is None or x is mt.Placeholder:
        return x
    nx = nominal_value(t.cast(mt.AnyNumber, x), to_float=to_float)
    try:
        if to_float:
            return float(nx)
        else:
            return decimal.Decimal(nx)
    except (TypeError, ValueError) as ex:
        raise mt.MathTypeError(f"{x.__class__.__name__} is not supported") from ex


def match_units(*args: t.Any) -> tuple[mt.AnyNumber, ...]:
    units = None
    second = False
    for x in args:
        if mt.is_science_number(x):
            if units is not None:
                units = x.units
            else:
                second = True
                break
    if units is None or not second:
        return args
    return tuple(
         x.convert(units) if mt.is_science_number(x) else x
        for x in args
    )


def match_convert[T](*args: mt.AnyNumber | None | type[mt.Placeholder]) -> tuple[T, ...]:
    has_decimals = False
    for x in args:
        if isinstance(x, decimal.Decimal):
            has_decimals = True
            break
        elif mt.is_science_number(x):
            if isinstance(x.nominal_value, decimal.Decimal):
                has_decimals = True
                break
    if has_decimals:
        return tuple(convert(x, to_float=False) for x in args)
    else:
        return tuple(convert(x, to_float=True) for x in args)


def separate_by_type(lst: t.Iterable[mt.AnyNumber]) -> tuple[list[mt.ScienceNumberProtocol], list[decimal.Decimal], list[str], list[int | float]]:
    sn = []
    dc = []
    flint = []
    ns = []
    for x in lst:
        if mt.is_science_number(x):
            sn.append(x)
        elif isinstance(x, str):
            ns.append(x)
        elif isinstance(x, decimal.Decimal):
            dc.append(x)
        else:
            flint.append(x)
    return sn, dc, ns, flint
