import decimal
import math as _math
import typing as t

from medsutil.math import types as mt
import medsutil.math.helpers as help
import medsutil.math.numbers as num



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

def sqrt(x):
    if mt.is_science_number(x):
        return sn_sqrt(x)
    try:
        return x.sqrt()
    except AttributeError:
        return _math.sqrt(x)

@num.as_science_number(lambda x: 1 / (x ** 0.5), units_func=lambda x: f"({help.units_first(x)}^0.5)")
def sn_sqrt(x):
    return sqrt(nominal_value(x))

def exp(x):
    if mt.is_science_number(x):
        return sn_exp(x)
    try:
        return x.exp()
    except AttributeError:
        return _math.exp(x)

@num.as_science_number(lambda x: exp(nominal_value(x)), units_func = lambda x: f"(e^{help.units_first(x)})")
def sn_exp(x):
    return exp(nominal_value(x))

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
    if mt.is_science_number(x):
        return sn_ln(x)
    try:
        return x.ln()
    except AttributeError:
        return _math.log(x)

@num.as_science_number(lambda x: 1 / x, units_func=lambda x: f"(ln {help.units_first(x)}")
def sn_ln(x):
    return ln(nominal_value(x))

def log10(x):
    if mt.is_science_number(x):
        return sn_log10(x)
    try:
        return x.log10()
    except AttributeError:
        return _math.log10(x)

@num.as_science_number(lambda x: 1 / (x * ln(decimal.Decimal(10))), units_func=lambda x: f"(log10 {help.units_first(x)}")
def sn_log10(x):
    return log10(nominal_value(x))

def log2(x):
    if mt.is_science_number(x):
        return sn_log2(x)
    try:
        return x.log10() / decimal.Decimal(2).log10()
    except AttributeError:
        return _math.log2(x)

@num.as_science_number(lambda x: 1 / (x * ln(decimal.Decimal(2))), units_func=lambda x: f"(log2 {help.units_first(x)}")
def sn_log2(x):
    return log2(nominal_value(x))

def log(x, base):
    if mt.is_science_number(x) or mt.is_science_number(base):
        return sn_log(x, base)
    x, base = match_convert(x, base)
    try:
        return x.log10() / base.log10()
    except AttributeError:
        return _math.log(x, base)

@num.as_science_number(lambda x, b: 1 / (x * ln(decimal.Decimal(b))), units_func=lambda x, y: f"(log{y} {help.units_first(x)}")
def sn_log(x, base):
    return log(nominal_value(x), nominal_value(base))

def pow(x, y):
    # todo: handle cases where types don't match
    if mt.is_science_number(x) or mt.is_science_number(y):
        return sn_pow(x, y)
    x, y = match_convert(x, y)
    return x ** y

def sn_pow(x, y):
    return pow(nominal_value(x), nominal_value(y))


def nominal_value(x: mt.AnyNumber):
    if mt.is_science_number(x):
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
