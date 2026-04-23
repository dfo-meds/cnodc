import decimal
import functools
import itertools
import typing as t
import medsutil.math.functions as fn
import medsutil.math.types as mt
import medsutil.math.helpers as help
from medsutil.math import functions

SN = t.TypeVar("SN", bound=float | decimal.Decimal)


def as_science_number[SN](*derivative_args, units_func: t.Callable = None, **derivative_kwargs):
    def _wrapper(func: t.Callable[..., SN]):

        derivatives = {}
        def _get_derivative(arg: int | str):
            if arg not in derivatives:
                derivatives[arg] = None
                if isinstance(arg, int):
                    if arg < len(derivative_args):
                        derivatives[arg] = derivative_args[arg]
                elif arg in derivative_kwargs:
                    derivatives[arg] = derivative_kwargs[arg]
                if derivatives[arg] is None:
                    # TODO calculate numerical partial derivatives?
                    derivatives[arg] = "0"
            return derivatives[arg]

        @functools.wraps(func)
        def _call_with_derivatives(*args, **kwargs):
            nominal_value: SN = func(*args, **kwargs)
            linear_part: mt.UnexpandedLinearCombination = list()
            num_type = type(nominal_value)

            for arg_pos, arg_value in itertools.chain(
                ((idx, arg) for idx, arg in enumerate(args)),
                ((name, arg) for name, arg in kwargs.items())
            ):
                if isinstance(arg_value, mt.ScienceNumberProtocol):
                    linear_part.append((_get_derivative(arg_pos)(*args, **kwargs), arg_value.linear_combo))
                    num_type = arg_value.num_type
            units = units_func(*args, **kwargs) if units_func else None
            if not linear_part:
                return ScienceNumber(nominal_value, None, units=units, num_type=num_type)
            else:
                return ScienceNumber(nominal_value, units=units, linear_combo=linear_part, num_type=num_type)

        return _call_with_derivatives
    return _wrapper


class LinearCombination:

    def __init__(self, linear_combo: mt.ExpandedLinearCombination | mt.UnexpandedLinearCombination):
        self._linear_combo = linear_combo

    @property
    def linear_combo(self) -> mt.ExpandedLinearCombination | mt.UnexpandedLinearCombination:
        return self._linear_combo

    def expanded(self) -> mt.ExpandedLinearCombination:
        if isinstance(self._linear_combo, list):
            expansion = {}
            while self._linear_combo:
                main_factor, expr = self._linear_combo.pop()
                if isinstance(expr.linear_combo, dict):
                    for variable, factor in expr.linear_combo.items():
                        coeff = functions.nominal_value(main_factor) * functions.nominal_value(factor)
                        if variable not in expansion:
                            expansion[variable] = coeff
                        else:
                            expansion[variable] += coeff
                else:
                    for factor, sub_combo in expr.linear_combo:
                        self._linear_combo.append((functions.nominal_value(factor) * functions.nominal_value(main_factor), sub_combo))
            self._linear_combo = expansion
        return self._linear_combo


class CastableNumber[SN]:

    def __init__(self, num_type: type[SN] = decimal.Decimal):
        self.num_type: type[SN] = num_type

    @t.overload
    def convert(self, x: None) -> None: ...

    @t.overload
    def convert(self, x: mt.Placeholder) -> mt.Placeholder: ...

    @t.overload
    def convert(self, x: mt.AnyNumber) -> SN: ...

    @t.overload
    def convert(self, x: mt.AnyNumber | None | mt.Placeholder) -> SN | None | mt.Placeholder: ...

    def convert(self, x: mt.AnyNumber | None | mt.Placeholder) -> SN | None | mt.Placeholder:
        return functions.convert(x, self.num_type)


class ScienceNumber[SN](CastableNumber[SN]):

    def __init__(self,
                 value: mt.AnyNumber,
                 std_dev: mt.AnyNumber | None | mt.Placeholder = mt.Placeholder(),
                 units: str | None = None,
                 tag: str | None = None,
                 num_type: type[SN] = decimal.Decimal,
                 linear_combo: LinearCombination | mt.ExpandedLinearCombination | mt.UnexpandedLinearCombination | None = None):
        super().__init__(num_type)
        self._nominal: SN = self.convert(value)
        self._std_dev: SN | None | mt.Placeholder = self.convert(std_dev)
        self._units = units
        self._tag = tag
        if linear_combo is None:
            self._linear_combo = LinearCombination[ScienceNumber]({self: 1})
        elif isinstance(linear_combo, (dict, list)):
            self._linear_combo = LinearCombination(linear_combo)
        else:
            self._linear_combo = linear_combo

    def __str__(self):
        return str(self._nominal)

    def __repr__(self):
        return f"<ScienceNumber:{self._nominal} [sigma:{self.std_dev}] {self._units} >"

    @property
    def linear_combo(self) -> LinearCombination:
        return self._linear_combo

    @property
    def units(self) -> str | None:
        return self._units

    @property
    def nominal_value(self) -> SN:
        return self._nominal

    @property
    def std_dev(self) -> SN | None:
        if self._std_dev is mt.Placeholder and self.linear_combo is not None:
            self._std_dev = fn.sqrt(sum(
                (abs(pow(factor * (var.std_dev or 0), 2)))
                for var, factor in
                self.linear_combo.expanded().items()
            ))
        return self._std_dev

    @as_science_number(lambda x: -1, units_func=help.units_first)
    def __neg__(self) -> ScienceNumber[SN]:
        return self._nominal * self.convert(-1)

    @as_science_number(help.derivative_y, help.derivative_x, units_func=help.units_multiply)
    def __mul__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.nominal_value * self.convert(other)

    @as_science_number(help.derivative_x, help.derivative_y, units_func=help.units_multiply)
    def __rmul__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.nominal_value * self.convert(other)

    @as_science_number(help.derivative_numerator, help.derivative_denominator, units_func=help.units_div)
    def __truediv__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.nominal_value / self.convert(other)

    @as_science_number(help.derivative_denominator, help.derivative_numerator, units_func=help.units_div_r)
    def __rtruediv__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.convert(other) / self.nominal_value

    @as_science_number(help.derivative_power_base, help.derivative_power_exponent, units_func=help.units_power)
    def __pow__(self, power: mt.AnyNumber, modulo: int | None = None) -> ScienceNumber[SN]:
        return self._nominal ** self.convert(power)

    @as_science_number(help.derivative_power_exponent, help.derivative_power_base, units_func=help.units_power_r)
    def __rpow__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.convert(other) ** self._nominal

    def __add__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        try:
            return self._add(other.match_units(self))
        except AttributeError:
            return self._add(other)

    def __radd__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.match_units(other)._add(other)

    @as_science_number(help.derivative_1, help.derivative_1, units_func=help.units_first)
    def _add(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self._nominal + self.convert(other)

    def __mod__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        try:
            return self._mod(other.match_units(self))
        except AttributeError:
            return self._mod(other)

    def __rmod__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.match_units(other)._rmod(other)

    @as_science_number(help.derivative_1, help.derivative_modulo, units_func=help.units_first)
    def _mod(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self._nominal % self.convert(other)

    @as_science_number(help.derivative_modulo_rev, help.derivative_1, units_func=help.units_first)
    def _rmod(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.convert(other) % s

    def __sub__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        try:
            return self._sub(other.match_units(self))
        except AttributeError:
            return self._sub(other)

    @as_science_number(help.derivative_1, help.derivative_neg1, units_func=help.units_first)
    def _sub(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self._nominal - self.convert(other)

    def __rsub__(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.match_units(other)._rsub(other)

    @as_science_number(help.derivative_neg1, help.derivative_1, units_func=help.units_first)
    def _rsub(self, other: mt.AnyNumber) -> ScienceNumber[SN]:
        return self.convert(other) - s

    def __abs__(self) -> ScienceNumber[SN]:
        if self._nominal < 0:
            return self.__neg__()
        return self

    def __pos__(self) -> ScienceNumber[SN]:
        return self

    def __lt__(self, other: mt.AnyNumber) -> bool:
        if isinstance(other, mt.ScienceNumberProtocol):
            other = other.match_units(self)
        return self.nominal_value < self.convert(other)

    def __gt__(self, other: mt.AnyNumber) -> bool:
        if isinstance(other, mt.ScienceNumberProtocol):
            other = other.match_units(self)
        return self.nominal_value > self.convert(other)

    def __eq__(self, other: mt.AnyNumber) -> bool:
        if isinstance(other, mt.ScienceNumberProtocol):
            other = other.match_units(self)
        return self.nominal_value == self.convert(other)

    def __ne__(self, other: mt.AnyNumber) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self._nominal, self._std_dev, self._units, self._tag))

    def __float__(self) -> float:
        return float(self._nominal)

    def __int__(self) -> int:
        return int(self._nominal)

    def __index__(self) -> int:
        return int(self._nominal)

    def __trunc__(self) -> int:
        return int(self._nominal)

    def match_units(self, other: mt.ScienceNumberProtocol[SN]) -> mt.ScienceNumberProtocol[SN]:
        # TODO: if units differ, figure it out
        return other


