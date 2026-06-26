from medsutil.math.types import (
    AnyNumber,
    is_science_number,
    BasicNumber,
    NumberString,
    NonScienceNumber,
    Placeholder
)
# Nice easy alias for any number - this covers floats, ints, decimals, NumberStrings, and ScientificNumbers
Number = AnyNumber

from medsutil.math._common import (
    nominal_value,
    is_complex,
    is_infinity,
    is_nan,
    collapse,
    product,
    convert,
    match_convert
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

    pow_,
    add,
    sub,
    mul,
    div,
    neg,

    gt,
    lt,
    between,
    lte,
    gte,
    eq,
    is_close,

    max_,
    min_,
    sum_,
    product,

    calculate_polynomial,
    add_in_quadrature,

    sign,
    copy_sign,
    modulo,

    to_exact_int,
    is_zero,
    is_exact_int,
    test_compatibility,

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
    ScienceNumber,
    LinearCombination,
    with_error_propagation
)