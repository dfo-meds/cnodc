"""

Provides math functions that add a few additional key pieces of functionality.

* The ScienceNumber class supports uncertainty and stores units.
* All functions can mix float / decimal / ScienceNumber arguments (the most precise is kept)

"""

from medsutil.math.types import (
    AnyNumber,
    BasicNumber,
    NonScienceNumber,
    NumberString,
    Placeholder,
    is_science_number,
)

# Nice easy alias for any number - this covers floats, ints, decimals, NumberStrings, and ScientificNumbers
Number = AnyNumber

from medsutil.math._common import (
    ceil,
    collapse,
    convert,
    floor,
    is_complex,
    is_infinity,
    is_nan,
    match_convert,
    nominal_value,
    round_to_int,
    round_to_place,
)
from medsutil.math._constants import (
    DEFAULT_ABS_TOL,
    DEFAULT_REL_TOL,
    e,
    epsilon_sqrt,
    inf,
    pi,
    tau,
)
from medsutil.math._functions import (
    add,
    add_in_quadrature,
    between,
    calculate_polynomial,
    copy_sign,
    div,
    eq,
    exp,
    gt,
    gte,
    is_close,
    is_exact_int,
    is_zero,
    ln,
    log,
    log2,
    log10,
    lt,
    lte,
    max_,
    min_,
    modulo,
    mul,
    neg,
    pow_,
    product,
    sign,
    sqrt,
    sub,
    sum_,
    test_compatibility,
    to_exact_int,
)
from medsutil.math._trig import (
    acos,
    asin,
    atan,
    atan2,
    cos,
    degrees,
    radians,
    sin,
    tan,
)
from medsutil.math.numbers import (
    LinearCombination,
    ScienceNumber,
    with_error_propagation,
)
