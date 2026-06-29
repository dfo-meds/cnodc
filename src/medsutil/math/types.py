import decimal
import typing as t
import decimal as _decimal

T = t.TypeVar('T')


class NotSupportedMathOperation(ValueError): ...


class Placeholder:
    def __new__(cls):
        return cls


class NumberString(str):

    def __add__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import add
        return add(self, other)

    def __radd__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import add
        return add(self, other)

    def __sub__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import sub
        return sub(self, other)

    def __rsub__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import sub
        return sub(other, self)

    def __mul__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import mul
        return mul(self, other)

    def __rmul__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import mul
        return mul(self, other)

    def __truediv__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import div
        return div(self, other)

    def __rtruediv__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import div
        return div(other, self)

    def __pow__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import pow_
        return pow_(self, other)

    def __float__(self) -> float:
        return float(self)

    def __int__(self) -> int:
        return int(self)

    def __index__(self) -> int:
        return int(self)

    def __trunc__(self) -> int:
        return int(self)

    def __copy__(self):
        return NumberString(self[:])

    def __mod__(self, other) -> ScienceNumberProtocol | BasicNumber:
        from medsutil.math import modulo
        return modulo(self, other)

    def __rmod__(self, other) -> ScienceNumberProtocol | BasicNumber:
        from medsutil.math import modulo
        return modulo(other, self)

    def __deepcopy__(self, _: t.Mapping) -> NumberString:
        return NumberString(self[:])

    def __round__(self, n: int | None = None) -> int | BasicNumber:
        if n is None or n == 0:
            return int(self)
        from medsutil.math import nominal_value
        return round(nominal_value(self), n)

    def __rpow__(self, other) -> BasicNumber | ScienceNumberProtocol:
        from medsutil.math import pow_
        return pow_(other, self)

    def __neg__(self) -> NumberString:
        if len(self) > 0 and self[0] == "-":
            return NumberString(self[1:])
        else:
            return NumberString(f"-{self}")

    def __abs__(self) -> NumberString:
        if len(self) > 0 and self[0] == "-":
            return NumberString(self[1:])
        return self

    def __pos__(self) -> t.Self:
        return self

    def __lt__(self, other) -> bool:
        from medsutil.math import gte
        return not gte(self, other)

    def __gt__(self, other) -> bool:
        from medsutil.math import lte
        return not lte(self, other)

    def to_actual_number(self, as_float: bool = True, as_science_number: bool = False):
        if as_science_number:
            from medsutil.math import ScienceNumber
            if as_float:
                return ScienceNumber.from_float(float(self))
            else:
                return ScienceNumber.from_decimal(decimal.Decimal(self))
        else:
            return float(self) if as_float else decimal.Decimal(self)


@t.runtime_checkable
class ScienceNumberProtocol(t.Protocol):

    @property
    def std_dev(self) -> NonScienceNumber | None: ...

    @property
    def nominal_value(self) -> NonScienceNumber: ...

    @property
    def linear_combo(self) -> LinearCombinationProtocol: ...

    @property
    def units(self) -> str | None: ...

    def __float__(self) -> float: ...
    def __int__(self) -> int: ...
    def __index__(self) -> int: ...
    def __trunc__(self) -> int: ...
    def __round__(self, n: int | None = None) -> int | t.Self: ...
    def __abs__(self) -> t.Self: ...
    def __copy__(self) -> t.Self: ...
    def __deepcopy__(self, _: t.Mapping) -> t.Self: ...
    def __hash__(self) -> int: ...
    def __mod__(self, other: AnyNumber) -> ScienceNumberProtocol: ...

    def convert(self, units: str | None) -> t.Self: ...
    def set_units(self, units: str | None): ...
    def set_min_error_if_worse(self, std_dev: NonScienceNumber, is_relative: bool = False): ...


class LinearCombinationProtocol(t.Protocol):

    @property
    def linear_combo(self) -> ExpandedLinearCombination | UnexpandedLinearCombination: ...

    def expanded(self) -> ExpandedLinearCombination: ...


def is_science_number(x: t.Any) -> t.TypeGuard[ScienceNumberProtocol]:
    return hasattr(x, 'units') and hasattr(x, 'nominal_value') and hasattr(x, 'std_dev')


def is_supported(x: t.Any) -> t.TypeGuard[float | int | _decimal.Decimal]:
    return isinstance(x, (float, int, _decimal.Decimal))


BasicNumber = _decimal.Decimal | int | float
NonScienceNumber = _decimal.Decimal | int | float | NumberString
AnyNumber = _decimal.Decimal | int | float | ScienceNumberProtocol | NumberString

type ExpandedLinearCombination = dict[ScienceNumberProtocol, AnyNumber]
type UnexpandedLinearCombination = list[tuple[AnyNumber, LinearCombinationProtocol]]


class MathTypeError(TypeError): ...


NumberOrIterable = AnyNumber | t.Iterable[AnyNumber]
