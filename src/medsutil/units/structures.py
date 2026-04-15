from __future__ import annotations
import decimal
import typing as t
from pipeman.exceptions import CNODCError

if t.TYPE_CHECKING:
    from medsutil.units import UnitConverter  # pragma: no cover


class UnitError(CNODCError):

    def __init__(self, msg: str, code: t.Optional[int] = None, is_transient: bool = False):
        super().__init__(msg, 'UNITS', code, is_transient)


class Converter:

    def convert(self, input_val):
        raise NotImplementedError  # pragma: no cover

    def scale(self, factor: decimal.Decimal):
        raise NotImplementedError  # pragma: no cover

    def power(self, factor: decimal.Decimal):
        raise NotImplementedError  # pragma: no cover

    def product(self, other_converter):
        raise NotImplementedError  # pragma: no cover

    def shift(self, factor: decimal.Decimal):
        raise NotImplementedError  # pragma: no cover

    def invert(self):
        raise NotImplementedError  # pragma: no cover


class UnitExpression:

    def get_unit_info(self, ref_dict) -> tuple[Converter, dict[str, int]]:
        raise NotImplementedError  # pragma: no cover

    def standardize(self) -> UnitExpression:
        return self

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        raise NotImplementedError  # pragma: no cover


class Literal(UnitExpression):
    pass


class Number(Literal):

    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, Number):
            return other.value == self.value
        return False

    def as_decimal(self):
        return decimal.Decimal(self.value)

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        return LinearFunction(self.as_decimal()), {}

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return str(self.value)


class Real(Number):
    pass


class Integer(Number):
    pass


class SimpleUnit(UnitExpression):

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"[{self.name}]"

    def __eq__(self, other):
        if isinstance(other, SimpleUnit):
            return other.name == self.name
        return False

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        return ref_dict.raw_unit_info(self.name)

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return ref_dict.standardize_simple_unit(self.name)



class Log(UnitExpression):

    def __init__(self, expression: UnitExpression, base: Number):
        self.expression = expression
        self.base = base

    def __eq__(self, other):
        if isinstance(other, Log):
            return other.base == self.base and other.expression == self.expression
        return False

    def __repr__(self):
        return f"(log base {self.base} of {self.expression})"

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return '(log TBD)'  # pragma: no cover

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        raise UnitError("Unit info calculations not yet supported for log units", 3000)


class Offset(UnitExpression):

    def __init__(self, expression: UnitExpression, offset: Literal):
        self.expression = expression
        self.offset = offset

    def __eq__(self, other):
        if isinstance(other, Offset):
            return other.expression == self.expression and other.offset == self.offset
        return False

    def __repr__(self):
        return f"({self.expression} @ {self.offset})"

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return f"({self.expression.udunit_str(ref_dict)} @ {self.offset})"  # pragma: no cover

    def standardize(self) -> UnitExpression:
        return Offset(self.expression.standardize(), self.offset)

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[Converter, dict[str, int]]:
        inner_convert, inner_dims = self.expression.get_unit_info(ref_dict)
        if isinstance(self.offset, Number):
            return inner_convert.shift(self.offset.as_decimal()), inner_dims
        raise UnitError("Shift not supported with other data types yet", 3001)


class Quotient(UnitExpression):

    def __init__(self, left: UnitExpression, right: UnitExpression):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({str(self.left)} / {str(self.right)})"

    def __eq__(self, other):
        if isinstance(other, Quotient):
            return other.left == self.left and other.right == self.right
        return False

    def standardize(self) -> UnitExpression:
        # always convert A/B to A B^-1
        return Product(self.left, Exponent(self.right, Integer("-1"))).standardize()

    def udunit_str(self, ref_dict: UnitConverter) -> str:   # pragma: no cover
        return self.standardize().udunit_str(ref_dict)

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:  # pragma: no cover
        left_factor, left_dims = self.left.get_unit_info(ref_dict)
        right_factor, right_dims = self.right.get_unit_info(ref_dict)
        result_dims = {x: left_dims[x] for x in left_dims}
        for x in right_dims:
            if x in result_dims:
                result_dims[x] -= right_dims[x]
            else:
                result_dims[x] = -1 * right_dims[x]
        return left_factor.product(right_factor.invert()), result_dims


class Product(UnitExpression):

    def __init__(self, left: UnitExpression, right: UnitExpression):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({str(self.left)} * {str(self.right)})"

    def __eq__(self, other):
        if isinstance(other, Product):
            return other.left == self.left and other.right == self.right
        return False

    def standardize(self) -> UnitExpression:
        products = [self.left, self.right]
        raw_products = {}
        others: list[UnitExpression] = []
        constant = None
        while products:
            x = products.pop().standardize()
            if isinstance(x, Product):
                products.append(x.left)
                products.append(x.right)
            elif isinstance(x, Number):
                if constant is None:
                    constant = x.as_decimal()
                else:
                    constant *= x.as_decimal()
            # should be exponents or simple units
            elif isinstance(x, SimpleUnit):
                raw_products[x.name] = Integer("1")
            elif isinstance(x, Exponent) and isinstance(x.base, SimpleUnit):
                raw_products[x.base.name] = x.exponent
            else:  # pragma: no cover (this just handles fallback)
                others.append(x)
        expr = None
        for k in others:  # pragma: no cover (this just handles fallback)
            expr = k if expr is None else Product(k, expr)
        for k in reversed(sorted(raw_products.keys())):
            if raw_products[k].as_decimal() <= 0:
                right = SimpleUnit(k) if raw_products[k].as_decimal() == 1 else Exponent(SimpleUnit(k), raw_products[k])
                expr = right if expr is None else Product(right, expr)
        for k in reversed(sorted(raw_products.keys())):
            if raw_products[k].as_decimal() > 0:
                right = SimpleUnit(k) if raw_products[k].as_decimal() == 1 else Exponent(SimpleUnit(k), raw_products[k])
                expr = right if expr is None else Product(right, expr)
        if constant is not None:
            expr = Real(str(constant)) if expr is None else Product(Real(str(constant)), expr)
        if expr is None:
            raise ValueError('Invalid expression')
        return expr

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return f'{self.left.udunit_str(ref_dict)} {self.right.udunit_str(ref_dict)}'

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:
        left_factor, left_dims = self.left.get_unit_info(ref_dict)
        right_factor, right_dims = self.right.get_unit_info(ref_dict)
        result_dims = {x: left_dims[x] for x in left_dims}
        for x in right_dims:
            if x in result_dims:
                result_dims[x] += right_dims[x]
            else:
                result_dims[x] = right_dims[x]
        return left_factor.product(right_factor), result_dims


class Exponent(UnitExpression):

    def __init__(self, base: UnitExpression, exponent: Integer):
        self.base = base
        self.exponent = exponent

    def __repr__(self):
        return f"({self.base} ** {self.exponent})"

    def standardize(self) -> UnitExpression:
        # (A^n)^m = A^(n*m)
        if isinstance(self.base, Exponent):
            return Exponent(self.base.base, Integer(str(self.exponent.as_decimal() * self.base.exponent.as_decimal()))).standardize()
        # (AB)^n = A^n B^n
        if isinstance(self.base, Product):
            return Product(Exponent(self.base.left, self.exponent), Exponent(self.base.right, self.exponent)).standardize()
        # (A/B)^n = A^n B^-n
        if isinstance(self.base, Quotient):
            return Product(Exponent(self.base.left, self.exponent), Exponent(self.base.right, Integer(str(self.exponent.as_decimal() * -1)))).standardize()
        # (r^n) = r^n
        if isinstance(self.base, Number):
            return Number(str(self.base.as_decimal() ** self.exponent.as_decimal()))
        # A^1 = A
        if self.exponent.value == "1":
            return self.base.standardize()
        return self

    def __eq__(self, other):
        if isinstance(other, Exponent):
            return self.base == other.base and self.exponent == other.exponent
        return False

    def udunit_str(self, ref_dict: UnitConverter) -> str:
        return f'{self.base.udunit_str(ref_dict)}{self.exponent.udunit_str(ref_dict)}'

    def get_unit_info(self, ref_dict: UnitConverter) -> tuple[decimal.Decimal, dict[str, int]]:
        base_factor, base_dims = self.base.get_unit_info(ref_dict)
        exp_num = self.exponent.as_decimal()
        return base_factor.power(exp_num), {x: int(base_dims[x] * exp_num) for x in base_dims}


class LinearFunction(Converter):

    def __init__(self, factor: decimal.Decimal, shift: decimal.Decimal = decimal.Decimal("0")):
        self._scale = factor
        self._shift = shift

    def __repr__(self):
        if self._shift > 0:
            return f'{self._scale}x+{self._shift}'
        elif self._shift < 0:
            return f'{self._scale}x{self._shift}'
        else:
            return f'{self._scale}x'

    def convert(self, input_val):
        return (input_val * self._scale) + self._shift

    def scale(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return LinearFunction(self._scale * factor)

    def power(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return LinearFunction(self._scale ** factor)

    def shift(self, factor: decimal.Decimal):
        if factor == 0:
            return self
        return LinearFunction(self._scale, self._shift + factor)

    def invert(self):
        return LinearFunction(1 / self._scale, (-1 * self._shift) / self._scale)

    def product(self, other_converter):
        if isinstance(other_converter, LinearFunction):
            return LinearFunction(self._scale * other_converter._scale)
        raise UnitError("Cannot take products of other things yet", 3002)


"""
I'd like to come back to these later, but they present complications for analysis.
class NonLinearFunction(Converter):

    def __init__(self, inner_func: Converter, scale: decimal.Decimal = 1, shift: decimal.Decimal = 0, inner_scale: decimal.Decimal = 1, inner_shift: decimal.Decimal = 0):
        self._inner = inner_func
        self._scale = scale
        self._inner_scale = inner_scale
        self._shift = shift
        self._inner_shift = inner_shift

    def convert(self, input_val):
        return (self._scale * self._inner.convert(input_val)) + self._shift

    def scale(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return NonLinearFunction(self._inner, self._scale * factor)

    def power(self, factor: decimal.Decimal):
        if factor == 1:
            return self
        return NonLinearFunction(self._inner, self._scale ** factor)

    def shift(self, factor: decimal.Decimal):
        if factor == 0:
            return self
        return NonLinearFunction(self._inner, self._scale, self._shift + factor)

    def invert(self):
        return NonLinearFunction(self._inner.invert())


class LogFunction(Converter):

    def __init__(self, base):
        self._base = base

    def convert(self, input_val):
        return math.log(input_val, self._base)

    def invert(self):
        return ExpFunction(self._base)


class _NonLinearWrapper(Converter):

    def scale(self, factor: decimal.Decimal):
        return NonLinearFunction(self, factor)

    def shift(self, factor: decimal.Decimal):
        return NonLinearFunction(self, decimal.Decimal(1), factor)

    def power(self, factor: decimal.Decimal):
        return NonLinearFunction(self).power(factor)


class ExpFunction(_NonLinearWrapper):

    def __init__(self, base):
        self._base = base

    def convert(self, input_val):
        return math.pow(self._base, input_val)

    def invert(self):
        return LogFunction(self._base)
"""