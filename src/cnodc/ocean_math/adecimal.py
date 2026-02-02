import decimal
import math
import typing as t

TRIG_FLOAT_ACCURACY = "0.0000000000000005"

NonAccurateNumber = t.Union[int, float, str, decimal.Decimal]

class AccurateDecimal:

    def __init__(self, num: NonAccurateNumber, accuracy: NonAccurateNumber):
        self.num = decimal.Decimal(num) if not isinstance(num, decimal.Decimal) else num
        self.accuracy = decimal.Decimal(accuracy) if not isinstance(accuracy, decimal.Decimal) else accuracy

    def __abs__(self):
        return AccurateDecimal(self.num if self.num > 0 else -1 * self.num, self.accuracy)

    def __eq__(self, other):
        if isinstance(other, AccurateDecimal):
            return not ((self.num + self.accuracy) < (other.num - other.accuracy) or (self.num - self.accuracy) > (other.num - other.accuracy))
        else:
            return not ((self.num + self.accuracy) < other or (self.num - self.accuracy) > other)

    def __gt__(self, other):
        if isinstance(other, AccurateDecimal):
            return (self.num - self.accuracy) > (other.num + other.accuracy)
        else:
            return (self.num - self.accuracy) > other

    def __lt__(self, other):
        if isinstance(other, AccurateDecimal):
            return (self.num + self.accuracy) < (other.num - other.accuracy)
        else:
            return (self.num + self.accuracy) < other

    def __str__(self):
        return f"{self.num:.15f} ± {self.accuracy:.15f}"

    def __add__(self, other):
        if isinstance(other, AccurateDecimal):
            return AccurateDecimal(self.num + other.num, self.accuracy + other.accuracy)
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num + other, self.accuracy)

    def __radd__(self, other):
        other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
        return AccurateDecimal(self.num + other, self.accuracy)

    def __sub__(self, other):
        if isinstance(other, AccurateDecimal):
            return AccurateDecimal(self.num - other.num, self.accuracy + other.accuracy)
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num - other, self.accuracy)

    def __rsub__(self, other):
        other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
        return AccurateDecimal(other - self.num, self.accuracy)

    def __mul__(self, other):
        if isinstance(other, AccurateDecimal):
            return AccurateDecimal((self.num * other.num) + (self.accuracy * other.accuracy), (self.num * other.accuracy) + (other.num * self.accuracy))
        else:
            other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
            return AccurateDecimal(self.num * other, self.accuracy * other)

    def __rmul__(self, other):
        other = decimal.Decimal(other) if not isinstance(other, decimal.Decimal) else other
        return AccurateDecimal(other * self.num, self.accuracy * other)

    def set_minimum_accuracy(self, accuracy: NonAccurateNumber):
        new_acc = decimal.Decimal(accuracy) if not isinstance(accuracy, decimal.Decimal) else accuracy
        if new_acc > self.accuracy:
            self.accuracy = new_acc

    def inverse(self):
        return AccurateDecimal(1 / self.num, self.accuracy / (self.num * (self.num * self.accuracy)))

    def __truediv__(self, other):
        if isinstance(other, AccurateDecimal):
            return self * other.inverse()
        else:
            return self * (1/other)

    def __rtruediv__(self, other):
        return AccurateDecimal(other, 0) / self

    def __pow__(self, power, modulo=None):
        if modulo is not None:
            raise ValueError('modulo not supported yet')
        inverse = False
        if power == 0:
            return AccurateDecimal(0, 0)
        elif power < 1:
            inverse = True
            power *= -1
        if isinstance(power, int):
            r = AccurateDecimal(self.num, self.accuracy)
            for x in range(1, power):
                r *= self
            return r.inverse() if inverse else r
        else:
            raise ValueError('decimal power not supported yet')

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


PI = AccurateDecimal("3.1415926535898", "0.00000000000001")

AnyNumber = t.Union[AccurateDecimal, NonAccurateNumber]


def sin(radians: AnyNumber) -> AnyNumber:
    if isinstance(radians, AccurateDecimal):
        adecimal = AccurateDecimal(math.sin(radians.num) * math.cos(radians.accuracy), math.cos(radians.num) * math.sin(radians.accuracy))
        adecimal.set_minimum_accuracy(TRIG_FLOAT_ACCURACY)
        return adecimal
    else:
        return math.sin(radians)

def cos(radians: AnyNumber) -> AnyNumber:
    if isinstance(radians, AccurateDecimal):
        adecimal = AccurateDecimal(math.cos(radians.num) * math.cos(radians.accuracy), math.sin(radians.num) * math.sin(radians.accuracy))
        adecimal.set_minimum_accuracy(TRIG_FLOAT_ACCURACY)
        return adecimal
    else:
        return math.cos(radians)

def radians(degrees: AnyNumber) -> AnyNumber:
    if isinstance(degrees, AccurateDecimal):
        res = degrees * (PI * (1/ 180))
        res.set_minimum_accuracy("5e-14")
        return res
    else:
        return math.radians(degrees)
