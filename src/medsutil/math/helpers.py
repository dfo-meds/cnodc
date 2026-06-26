import decimal
import medsutil.math._constants as constants
import medsutil.math.types as mt
import typing as t

from medsutil.math import _common


def units_first(*args, **kwargs):
    for x in args:
        if hasattr(x, 'units'):
            return x.units
    for x in kwargs.values():
        if hasattr(x, 'units'):
            return x.units
    return ''

def units_multiply(x, y):
    return f"({units_first(x)})({units_first(y)})"

def units_div(x, y):
    return f"({units_first(x)})(({units_first(y)})^-1)"

def units_div_r(y, x):
    return units_div(x, y)

def units_power(base, exponent):
    return f"({units_first(base)}^({exponent}{units_first(exponent)})"

def units_power_r(exponent, base):
    return units_power(base, exponent)

def derivative_0(*args, **kwargs):
    return 0

def derivative_1(*args, **kwargs):
    return 1

def derivative_neg1(*args, **kwargs):
    return -1

def derivative_x(x, *args, **kwargs):
    return _common.nominal_value(x)

def derivative_y(x, y, *args, **kwargs):
    return _common.nominal_value(y)

def derivative_numerator(x, y, *args, **kwargs):
    return 1 / _common.nominal_value(y)

def derivative_denominator(x, y, *args, **kwargs):
    x, y = _common.match_convert(x, y)
    return (-x) / (y ** 2)

def derivative_pythagoras_x(x, y, *args, **kwargs):
    x, y = _common.match_convert(x, y)
    return x / _common.sqrt((x ** 2) + (y ** 2))

def derivative_pythagoras_y(y, x, *args, **kwargs):
    x, y = _common.match_convert(x, y)
    return x / _common.sqrt((x ** 2) + (y ** 2))

def derivative_power_base(base, exponent):
    base, exponent = _common.match_convert(base, exponent)
    if base > 0 or (exponent % 1 == 0 and (base < 0 or exponent >= 1)):
        return no_complex_or_nan(
            exponent * (base ** (exponent - 1))
        )
    elif base == 0 and exponent == 0:
        return 0
    else:
        raise mt.NotSupportedMathOperation("No derivative can be calculated")

def derivative_power_exponent(base, exponent):
    base, exponent = _common.match_convert(base, exponent)
    if base > 0:
        return no_complex_or_nan(
            _common.ln(base) * (base ** exponent)
        )
    elif base == 0 and exponent == 0:
        return 0
    else:
        raise mt.NotSupportedMathOperation("No derivative can be calculated")

def derivative_modulo(value, modulo):
    value, modulo = _common.match_convert(value, modulo)
    value_type = type(value)
    nd = numerical_derivative(value_type.__mod__, 1)
    return nd(value, modulo)

def derivative_modulo_rev(modulo, value):
    return derivative_modulo(value, modulo)

def numerical_derivative[T](func: t.Callable[..., T], with_respect_to: str | int) -> t.Callable[..., T]:
    is_kwarg = isinstance(with_respect_to, str)
    def _numerical_derivative(*args, **kwargs):
        change_args: list | dict = list(args) if not is_kwarg else kwargs
        base_step_size = constants.epsilon(isinstance(change_args[with_respect_to], decimal.Decimal))
        real_step_size = abs(change_args[with_respect_to] * base_step_size)
        if real_step_size == 0:
            real_step_size = base_step_size
        double_step = real_step_size * 2
        change_args[with_respect_to] += real_step_size
        shift_plus = func(*(args if is_kwarg else change_args), **kwargs)
        change_args[with_respect_to] -= double_step
        shift_minus = func(*(args if is_kwarg else change_args), **kwargs)
        return (shift_plus - shift_minus) / double_step
    return _numerical_derivative

def taylor_series_approximation(x: decimal.Decimal, _build_nth_term: t.Callable[[int], t.Callable[[decimal.Decimal], decimal.Decimal]]) -> decimal.Decimal:
    n = 0
    decimal.getcontext().prec += 2
    last_value = None
    current_value = decimal.Decimal("0")
    while last_value is None or last_value != current_value:
        last_value = current_value
        current_value += _build_nth_term(n)(x)
        n += 1
    decimal.getcontext().prec -= 2
    return current_value

def no_complex_or_nan(x):
    if _common.is_complex(x):
        raise mt.NotSupportedMathOperation("Complex numbers are not supported")
    elif _common.is_nan(x):
        raise mt.NotSupportedMathOperation("NaN found")
    return x







