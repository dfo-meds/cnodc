import decimal

import medsutil.math.functions as fn
import medsutil.math.types as mt
import medsutil.math.helpers as help
import medsutil.math.numbers as num


@num.as_science_number(lambda x: -1 * fn.cos(x), units_func=lambda x: "1")
def sin(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return fn.sin(x)

@num.as_science_number(lambda x: fn.sin(x), units_func=lambda x: "1")
def cos(x: mt.AnyNumber) -> mt.ScienceNumberProtocol:
    return fn.cos(x)
