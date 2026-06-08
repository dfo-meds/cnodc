from medsutil.math.types import (
    AnyNumber,
    is_science_number
)
# Nice easy alias for any number - this covers floats, ints, decimals, and ScientificNumbers
Number = AnyNumber

from medsutil.math._common import (
    nominal_value,
    is_complex,
    is_infinity,
    is_nan,
    gt,
    lt,
    collapse,
    summation,
    product,
    convert,
    match_convert,
    between
)
from medsutil.math._constants import (
    tau,
    inf,
    e,
    pi,
    epsilon,
)
from medsutil.math._functions import (
    sqrt,
    exp,
    ln,
    log10,
    log2,
    log,
    pow,
    add,
    sub,
    mul,
    div,
    calculate_polynomial,
    is_close,
)
from medsutil.math._trig import (
    cos,
    sin,
    tan,
    asin,
    acos,
    atan,
    atan2,
    radians,
    degrees,
)
from medsutil.math.numbers import (
    ScienceNumber
)