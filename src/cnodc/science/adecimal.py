""" Note: this package assumes uncorrelated errors for the moment: look at ufloat for inspiration (but using decimals here) """
import decimal
import math
import typing as t

from uncertainties import UFloat

TRIG_FLOAT_ACCURACY = "0.0000000000000005"

NonAccurateNumber = t.Union[int, float, str, decimal.Decimal]


class AccurateDecimal:

    def __init__(self, num: NonAccurateNumber, std_dev: NonAccurateNumber):
        self.num = decimal.Decimal(num) if not isinstance(num, decimal.Decimal) else num
        self.std_dev = decimal.Decimal(std_dev) if not isinstance(std_dev, decimal.Decimal) else std_dev

    def __float__(self):
        return float(self.num)

    def __int__(self):
        return int(self.num)

    def __abs__(self):
        return AccurateDecimal(self.num if self.num > 0 else -1 * self.num, self.std_dev)

    def __eq__(self, other):
        if isinstance(other, AccurateDecimal):
            return not ((self.num + self.std_dev) < (other.num - other.std_dev) or (self.num - self.std_dev) > (other.num - other.std_dev))
        elif isinstance(other, UFloat):
            return not ((self.num + self.std_dev) < (other.nominal_value - other.std_dev) or (self.num - self.std_dev) > (other.nominal_value - other.std_dev))
        else:
            return not ((self.num + self.std_dev) < other or (self.num - self.std_dev) > other)

    def __gt__(self, other):
        if isinstance(other, AccurateDecimal):
            return (self.num - self.std_dev) > (other.num + other.std_dev)
        elif isinstance(other, UFloat):
            return (self.num - self.std_dev) > (other.nominal_value + other.std_dev)
        else:
            return (self.num - self.std_dev) > other

    def __lt__(self, other):
        if isinstance(other, AccurateDecimal):
            return (self.num + self.std_dev) < (other.num - other.std_dev)
        elif isinstance(other, UFloat):
            return (self.num + self.std_dev) < (other.nominal_value - other.std_dev)
        else:
            return (self.num + self.std_dev) < other

    def __str__(self):
        return f"{self.num:.15f} ± {self.std_dev:.15f}"

    def __add__(self, other):
        if isinstance(other, AccurateDecimal):
            return AccurateDecimal(
                self.num + other.num,
                math.sqrt((self.std_dev ** 2) + (other.std_dev ** 2))
            )
        elif isinstance(other, UFloat):
            return AccurateDecimal(
                self.num + other.nominal_value,
                math.sqrt((self.std_dev ** 2) + (other.std_dev ** 2))
            )
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num + other, self.std_dev)

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        if isinstance(other, AccurateDecimal):
            return AccurateDecimal(
                self.num - other.num,
                math.sqrt((self.std_dev ** 2) + (other.std_dev ** 2))
            )
        elif isinstance(other, UFloat):
            return AccurateDecimal(
                self.num - other.nominal_value,
                math.sqrt((self.std_dev ** 2) + (other.std_dev ** 2))
            )
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num - other, self.std_dev)

    def __rsub__(self, other):
        return self - other

    def __mul__(self, other):
        if isinstance(other, AccurateDecimal):
            product = self.num * other.num
            return AccurateDecimal(
                product,
                (product if product > 0 else -1 * product) * math.sqrt(
                    ((self.std_dev / self.num) ** 2)
                    + ((other.std_dev / other.num) ** 2)
                )
            )
        elif isinstance(other, UFloat):
            product = self.num * other.nominal_value
            return AccurateDecimal(
                product,
                (product if product > 0 else -1 * product) * math.sqrt(
                    ((self.std_dev / self.num) ** 2)
                    + ((other.std_dev / other.nominal_value) ** 2)
                )
            )
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num * other, self.std_dev * other)

    def __rmul__(self, other):
        return self * other

    def set_minimum_accuracy(self, accuracy: NonAccurateNumber):
        new_acc = decimal.Decimal(accuracy) if not isinstance(accuracy, decimal.Decimal) else accuracy
        if new_acc > self.std_dev:
            self.std_dev = new_acc

    def inverse(self):
        recip = 1 / self.num
        rel_error = self.std_dev / self.num
        return AccurateDecimal(recip, rel_error * recip)

    def __truediv__(self, other):
        if isinstance(other, AccurateDecimal):
            return self * other.inverse()
        else:
            return self * (1 / other)

    def __rtruediv__(self, other):
        return self.inverse() * other

    def __pow__(self, power, modulo=None):
        if modulo is not None:
            raise ValueError('modulo not supported yet')
        if isinstance(power, (UFloat, AccurateDecimal)):
            raise ValueError('cannot power by uncertainty yet')
        num = self
        if power < 1:
            power *= -1
            num = num.inverse()
        return AccurateDecimal(num.num ** power, num.std_dev * power)

    def sqrt(self):
        return self ** 0.5

    @staticmethod
    def from_float(f: float, left_sigfigs: t.Optional[int] = None):
        if left_sigfigs is None:
            left_sigfigs = AccurateDecimal.left_sigfigs(str(f))
        return AccurateDecimal(f, f"5e{left_sigfigs-16}")

    @staticmethod
    def from_int(i: int):
        return AccurateDecimal(i, 0)

    @staticmethod
    def from_str(s: str):
        if "E" in s.upper():
            test, _ = s.upper().split("E", maxsplit=1)
        else:
            test = s
        test = test.lstrip("0")
        if "." in test:
            _, test = test.split(".", maxsplit=1)
            return AccurateDecimal(s, f"5e{-1 * (len(test) + 1)}")
        else:
            accuracy = 0
            while test[-1] == "0":
                test = test[:-1]
                accuracy += 1
            if accuracy == 0:
                return AccurateDecimal(s, "5e-1")
            return AccurateDecimal(s, f"5e{accuracy}")

    @staticmethod
    def left_sigfigs(n: str):
        if not n :
            raise ValueError("Not a number")
        if "E" in n.upper():
            left, _ = n.upper().split("E", maxsplit=1)
            p = len(left)
        elif "." in n:
            left, _ = n.split(".", maxsplit=1)
            p = len(left)
        else:
            p = len(n)
        if n[0] == "-":
            p -= 1
        return p
