import copy
import decimal
import functools
import itertools
import typing as t
import medsutil.math.types as mt
import medsutil.math._functions as basics
import medsutil.math._common as _common
from medsutil.math import types, _constants, nominal_value
from medsutil.math.helpers import numerical_derivative

SN = t.TypeVar("SN", bound=float | decimal.Decimal)


def with_error_propagation(*derivative_args, units_func: t.Callable = None, **derivative_kwargs):
    def _wrapper(func: t.Callable[..., mt.AnyNumber]):
        derivatives = {}
        def _get_derivative(arg: int | str) -> t.Callable[..., mt.AnyNumber]:
            if arg not in derivatives:
                derivatives[arg] = None
                if isinstance(arg, int):
                    if arg < len(derivative_args):
                        derivatives[arg] = derivative_args[arg]
                elif arg in derivative_kwargs:
                    derivatives[arg] = derivative_kwargs[arg]
                if derivatives[arg] is ...:
                    derivatives[arg] = numerical_derivative(func, arg)
                if derivatives[arg] is None:
                    derivatives[arg] = lambda *args, **kwargs: 0
            return derivatives[arg]

        @functools.wraps(func)
        def _call_with_derivatives(*args, **kwargs):
            nv = nominal_value(func(*args, **kwargs))
            linear_part: mt.UnexpandedLinearCombination = list()

            for arg_pos, arg_value in itertools.chain(
                ((idx, arg) for idx, arg in enumerate(args)),
                ((name, arg) for name, arg in kwargs.items())
            ):
                if mt.is_science_number(arg_value):
                    linear_part.append((_get_derivative(arg_pos)(*args, **kwargs), arg_value.linear_combo))
            units = units_func(*args, **kwargs) if units_func else None
            if not linear_part:
                return ScienceNumber(nv, 0, units=units)
            else:
                return ScienceNumber(nv, units=units, linear_combo=linear_part)

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
                        coeff = basics.mul(_common.nominal_value(main_factor), _common.nominal_value(factor))
                        if variable not in expansion:
                            expansion[variable] = coeff
                        else:
                            expansion[variable] += coeff
                else:
                    for factor, sub_combo in expr.linear_combo:
                        self._linear_combo.append((basics.mul(_common.nominal_value(factor), _common.nominal_value(main_factor)), sub_combo))
            self._linear_combo = expansion
        return self._linear_combo



class ScienceNumber:

    def __init__(self,
                 value: mt.NonScienceNumber,
                 std_dev: mt.NonScienceNumber | type[mt.Placeholder] = mt.Placeholder(),
                 min_std_dev: mt.NonScienceNumber | None = None,
                 units: str | None = None,
                 tag: str | None = None,
                 linear_combo: LinearCombination | mt.ExpandedLinearCombination | mt.UnexpandedLinearCombination | None = None):
        self._nominal: mt.NonScienceNumber = value
        self._sigma: mt.NonScienceNumber | type[mt.Placeholder] = std_dev
        self._min_sigma = min_std_dev
        self._units: str | None = units
        self._tag: str | None = tag
        if linear_combo is None:
            self._linear_combo = LinearCombination({self: 1})
        elif isinstance(linear_combo, (dict, list)):
            self._linear_combo = LinearCombination(linear_combo)
        else:
            self._linear_combo = linear_combo

    def __str__(self):
        return str(self._nominal)

    def __repr__(self):
        return f"<ScienceNumber:{self._nominal} [sigma:{self.std_dev}] {self._units} >"

    def is_compatible(self,
                      x: mt.AnyNumber,
                      c: mt.NonScienceNumber = 3) -> bool:
        if mt.is_science_number(x):
            return basics.test_compatibility(
                x.nominal_value,
                x.std_dev or 0,
                self._nominal,
                self.std_dev or 0,
                c
            )
        else:
            return basics.test_compatibility(
                t.cast(mt.NonScienceNumber, x),
                0,
                self._nominal,
                self.std_dev or 0,
                c
            )

    @property
    def linear_combo(self) -> LinearCombination:
        return self._linear_combo

    @property
    def units(self) -> str | None:
        return self._units

    @property
    def nominal_value(self) -> mt.NonScienceNumber:
        return self._nominal

    @property
    def std_dev(self) -> mt.NonScienceNumber:
        if self._sigma is mt.Placeholder and self.linear_combo is not None:
            self._sigma = basics.sqrt(_common.sum_(
                abs(basics.pow_(basics.mul(factor, (var.std_dev or 0)), 2))
                for var, factor in
                self.linear_combo.expanded().items()
            ))
        sd = t.cast(mt.NonScienceNumber, self._sigma)
        if self._min_sigma is not None:
            return t.cast(mt.NonScienceNumber, basics.max_(sd, self._min_sigma))
        else:
            return sd

    def copy_sign(self, other: mt.AnyNumber) -> ScienceNumber:
        return t.cast(ScienceNumber, basics.copy_sign(self, other))

    def sign(self) -> int | None:
        return basics.sign(self._nominal)

    def exp(self) -> ScienceNumber:
        return basics.sn_exp(self)

    def is_finite(self) -> bool:
        return not (self.is_infinite() or self.is_nan())

    def is_infinite(self) -> bool:
        return _common.is_infinity(self._nominal)

    def is_nan(self) -> bool:
        return _common.is_nan(self._nominal)

    def is_zero(self) -> bool:
        return _common.is_zero(self._nominal)

    def to_chopped_value(self, ndigits: int | None = None, force_decimal: bool = False) -> ScienceNumber:
        return ScienceNumber.chop_with_error(
            _common.nominal_value(self._nominal, to_float=not force_decimal),
            ndigits or 0,
            std_dev=mt.Placeholder,
            units=self._units,
            min_std_dev=self._min_sigma,
            tag=self._tag,
            linear_combo=self._linear_combo,
        )

    def to_rounded_value(self, ndigits: int | None = None, force_decimal: bool = False) -> ScienceNumber:
        return ScienceNumber.round_with_error(
            _common.nominal_value(self._nominal, to_float=not force_decimal),
            ndigits or 0,
            std_dev=mt.Placeholder,
            units=self._units,
            min_std_dev=self._min_sigma,
            tag=self._tag,
            linear_combo=self._linear_combo,
        )

    def to_integral(self, force_decimal: bool = False) -> ScienceNumber:
        return self.to_rounded_value(force_decimal=force_decimal)

    def to_integral_value(self, force_decimal: bool = False) -> ScienceNumber:
        return self.to_rounded_value(force_decimal=force_decimal)

    def is_close(self,
                 other: mt.AnyNumber,
                 rel_tol: mt.NonScienceNumber = _constants.DEFAULT_REL_TOL,
                 abs_tol: mt.NonScienceNumber = _constants.DEFAULT_ABS_TOL) -> bool:
        return basics.is_close(self, other, rel_tol=rel_tol, abs_tol=abs_tol)

    def ln(self) -> ScienceNumber:
        return basics.sn_ln(self)

    def log10(self) -> ScienceNumber:
        return basics.sn_log10(self)

    def logb(self) -> ScienceNumber:
        return basics.sn_log2(self)

    def max(self, other: mt.AnyNumber) -> mt.AnyNumber:
        return basics.max_(self, other)

    def min(self, other: mt.AnyNumber) -> mt.AnyNumber:
        return basics.min_(self, other)

    def sqrt(self) -> ScienceNumber:
        return basics.sn_sqrt(self)

    def __neg__(self) -> t.Self:
        return t.cast(t.Self, basics.sn_neg(self))

    def __mul__(self, other: mt.AnyNumber) -> ScienceNumber:
        try:
            return t.cast(ScienceNumber, basics.sn_mul(self, other))
        except types.MathTypeError:
            return NotImplemented

    def __rmul__(self, other: mt.AnyNumber):
        return t.cast(ScienceNumber, basics.sn_mul(other, self))

    def __truediv__(self, other: mt.AnyNumber) -> ScienceNumber:
        try:
            return t.cast(ScienceNumber, basics.sn_div(self, other))
        except types.MathTypeError:
            return NotImplemented

    def __rtruediv__(self, other: mt.AnyNumber) -> ScienceNumber:
        return t.cast(ScienceNumber, basics.sn_div(other, self))

    def __pow__(self, power: mt.AnyNumber, modulo: int | None = None) -> ScienceNumber:
        try:
            p = t.cast(ScienceNumber, basics.sn_pow(self, power))
            if modulo is not None:
                return basics.modulo(p, modulo)
            return p
        except types.MathTypeError:
            return NotImplemented

    def __rpow__(self, other: mt.AnyNumber) -> ScienceNumber:
        return t.cast(ScienceNumber, basics.sn_pow(other, self))

    def __add__(self, other: mt.AnyNumber) -> ScienceNumber:
        try:
            return t.cast(ScienceNumber, basics.sn_add(self, other))
        except types.MathTypeError:
            return NotImplemented

    def __radd__(self, other: mt.AnyNumber) -> ScienceNumber:
        return t.cast(ScienceNumber, basics.sn_add(other, self))

    def __mod__(self, other: mt.AnyNumber) -> ScienceNumber:
        try:
            return t.cast(ScienceNumber, basics.modulo(self, other))
        except types.MathTypeError:
            return NotImplemented

    def __rmod__(self, other: mt.AnyNumber) -> mt.NonScienceNumber:
        try:
            return basics.modulo(other, self)
        except types.MathTypeError:
            return NotImplemented

    def __sub__(self, other: mt.AnyNumber) -> ScienceNumber:
        try:
            return basics.sn_sub(self, other)
        except types.MathTypeError:
            return NotImplemented

    def __rsub__(self, other: mt.AnyNumber) -> ScienceNumber:
        return basics.sn_sub(other, self)

    def __abs__(self) -> t.Self:
        if self._nominal < 0:
            return self.__neg__()
        return self

    def __pos__(self) -> t.Self:
        return self

    def __lt__(self, other: mt.AnyNumber) -> bool:
        return not basics.gte(self, other)

    def __gt__(self, other: mt.AnyNumber) -> bool:
        return not basics.lte(self, other)

    def __eq__(self, other: mt.AnyNumber) -> bool:
        return basics.eq(self, other)

    def __ne__(self, other: mt.AnyNumber) -> bool:
        return not basics.eq(self, other)

    def __hash__(self) -> int:
        return id(self)

    def __float__(self) -> float:
        return float(self._nominal)

    def __int__(self) -> int:
        return int(self._nominal)

    def __index__(self) -> int:
        return int(self._nominal)

    def __trunc__(self) -> int:
        return int(self._nominal)

    def __round__(self, n: int | None = None) -> int | t.Self:
        if n is None or n == 0:
            return int(self._nominal)
        return self.round_with_error(
            _common.nominal_value(self._nominal),
            n,
            units=self._units,
            tag=self._tag,
        )

    def __copy__(self) -> t.Self:
        return self._new_same_class(
            copy.copy(self._nominal),
            std_dev=copy.copy(self.std_dev),
            units=copy.copy(self._units),
            tag=copy.copy(self._tag)
        )

    @classmethod
    def _new_same_class(cls, *args, **kwargs) -> t.Self:
        return cls(*args, **kwargs)

    def __deepcopy__(self, _: t.Mapping) -> t.Self:
        return self.__copy__()  # already a deep copy

    def set_min_error_if_worse(self,
                               std_dev: mt.NonScienceNumber,
                               is_relative: bool = False):
        if is_relative:
            std_dev = basics.mul(std_dev, self._nominal)
        if self._min_sigma is None:
            self._min_sigma = std_dev
        else:
            self._min_sigma = basics.max_(self._min_sigma, std_dev)

    def convert(self, units: str | None) -> t.Self:
        if units is None or self._units is None or units == self._units:
            return self
        from medsutil.units.units import convert
        return convert(self, self._units, units)

    def set_units(self, units: str | None):
        self._units = units

    @classmethod
    def from_decimal(cls,
                     d: decimal.Decimal | str,
                     std_dev: decimal.Decimal | str | None = None,
                     **kwargs) -> ScienceNumber:
        sn = cls(
            decimal.Decimal(d) if isinstance(d, str) else d,
            (decimal.Decimal(std_dev) if isinstance(std_dev, str) else std_dev) if std_dev is not None else mt.Placeholder,
            **kwargs
        )
        return sn

    @classmethod
    def from_float(cls,
                   f: float | int | str,
                   std_dev: float | int | str | None = None,
                   **kwargs) -> ScienceNumber:
        sn = cls(
            float(f) if isinstance(f, str) else f,
            (float(std_dev) if isinstance(std_dev, str) else std_dev) if std_dev is not None else mt.Placeholder,
            min_std_dev=_constants.FLOAT_ERROR)
        return sn

    @classmethod
    def chop_with_error(cls, x: mt.BasicNumber, n: int = 0, **kwargs):
        chopped = int(x * (10 ** n))
        std_err = (10 ** n) / _common.sqrt(3)
        chop_sn = cls(chopped, **kwargs)
        chop_sn.set_min_error_if_worse(std_err)
        return chop_sn

    @classmethod
    def round_with_error(cls, x: mt.BasicNumber, n: int = 0, **kwargs):
        rounded = round(x, n)
        std_err = ((10 ** n) / 2.0) / _common.sqrt(3)
        round_sn = cls(rounded, **kwargs)
        round_sn.set_min_error_if_worse(std_err)
        return round_sn
