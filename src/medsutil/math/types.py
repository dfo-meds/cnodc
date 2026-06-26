import typing as t
import decimal as _decimal

T = t.TypeVar('T')


class NotSupportedMathOperation(ValueError): ...


class Placeholder:
    def __new__(cls):
        return cls


class NumberString(str): ...


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
