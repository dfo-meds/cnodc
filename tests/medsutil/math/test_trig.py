import decimal
import sys

from tests.helpers.base_test_case import BaseTestCase, sub_tests
import math
from medsutil.math import _trig as trig, ScienceNumber

SOME_ANGLES = [
    0,
    math.pi / 2,
    math.pi / 3,
    math.pi / 4,
    math.pi / 6,
    1,
    math.pi / -2,
    math.pi / -3,
    math.pi / -4,
    math.pi / -6,
    (4 * math.pi),
    (9 * math.pi) / 2,
    (13 * math.pi) / 3,
    (17 * math.pi) / 4,
    (25 * math.pi) / 6,
]

SOME_RATIOS = [
    0,
    math.sqrt(3) / 2,
    math.sqrt(2) / 2,
    1/2,
    1,
    math.sqrt(3) / -2,
    math.sqrt(2) / -2,
    -1/2,
    -1
]

class TestTrig(BaseTestCase):

    @sub_tests(SOME_ANGLES)
    def test_cos_float(self, angle):
        self.assertAlmostEqual(math.cos(angle), trig.cos(angle))

    @sub_tests(SOME_ANGLES)
    def test_cos_decimal(self, angle):
        self.assertAlmostEqual(math.cos(angle), float(trig.cos(decimal.Decimal(angle))))

    @sub_tests(SOME_ANGLES)
    def test_cos_scinum(self, angle):
        f = math.cos(angle)
        scinum = trig.cos(ScienceNumber(angle, std_dev=0.01))
        self.assertAlmostEqual(f, float(scinum))
        self.assertAlmostEqual(abs(math.sin(angle)) * 0.01, scinum.std_dev)

    @sub_tests(SOME_ANGLES)
    def test_sin_float(self, angle):
        self.assertAlmostEqual(math.sin(angle), trig.sin(angle))

    @sub_tests(SOME_ANGLES)
    def test_sin_decimal(self, angle):
        self.assertAlmostEqual(math.sin(angle), float(trig.sin(decimal.Decimal(angle))))

    @sub_tests(SOME_ANGLES)
    def test_sin_scinum(self, angle):
        f = math.sin(angle)
        scinum = trig.sin(ScienceNumber(angle, std_dev=0.01))
        self.assertAlmostEqual(f, float(scinum))
        self.assertAlmostEqual(abs(math.cos(angle)) * 0.01, scinum.std_dev)

    @sub_tests(SOME_RATIOS)
    def test_asin_float(self, ratio):
        self.assertAlmostEqual(math.asin(ratio), trig.asin(ratio))

    @sub_tests(SOME_RATIOS)
    def test_asin_decimal(self, ratio):
        self.assertAlmostEqual(math.asin(ratio), float(trig.asin(decimal.Decimal(ratio))))

    @sub_tests(SOME_RATIOS)
    def test_asin_scinum(self, ratio):
        f = math.asin(ratio)
        scinum = trig.asin(ScienceNumber(ratio, std_dev=0.01))
        self.assertAlmostEqual(f, float(scinum))
        if ratio == 1 or ratio == -1:
            # manually calculated - this is the error at one epsilon less than
            # 1 (or greater than -1). Which makes it the closest we could be off
            # by
            self.assertGreater(scinum.std_dev, 474531)
        else:
            self.assertAlmostEqual(1 / math.sqrt(1 - (ratio ** 2)) * 0.01, scinum.std_dev)

    @sub_tests(SOME_RATIOS)
    def test_acos_float(self, ratio):
        self.assertAlmostEqual(math.acos(ratio), trig.acos(ratio))

    @sub_tests(SOME_RATIOS)
    def test_acos_decimal(self, ratio):
        self.assertAlmostEqual(math.acos(ratio), float(trig.acos(decimal.Decimal(ratio))))

    @sub_tests(SOME_RATIOS)
    def test_acos_scinum(self, ratio):
        f = math.acos(ratio)
        scinum = trig.acos(ScienceNumber(ratio, std_dev=0.01))
        self.assertAlmostEqual(f, float(scinum))
        if ratio == 1 or ratio == -1:
            # see above
            self.assertGreater(scinum.std_dev, 474531)
        else:
            self.assertAlmostEqual(1 / math.sqrt(1 - (ratio ** 2)) * 0.01, scinum.std_dev)

    @sub_tests(SOME_RATIOS)
    def test_atan_float(self, ratio):
        self.assertAlmostEqual(math.atan(ratio), trig.atan(ratio))

    @sub_tests(SOME_RATIOS)
    def test_atan_decimal(self, ratio):
        self.assertAlmostEqual(math.atan(ratio), float(trig.atan(decimal.Decimal(ratio))))

    @sub_tests(SOME_RATIOS)
    def test_atan_scinum(self, ratio):
        expected = math.atan(ratio)
        actual = trig.atan(ScienceNumber(ratio, std_dev=0.01))
        self.assertIsInstance(actual, ScienceNumber)
        self.assertAlmostEqual(expected, float(actual))
        self.assertAlmostEqual(actual.std_dev, (1 / (1 + (ratio ** 2))) * 0.01)

    @sub_tests(SOME_ANGLES)
    def test_degrees(self, angle):
        self.assertAlmostEqual(math.degrees(angle), trig.degrees(angle))

    @sub_tests([
        x for x in range(0, 720, 5)
    ])
    def test_radians(self, x):
        self.assertAlmostEqual(math.radians(x), trig.radians(x))

    @sub_tests(SOME_ANGLES)
    def test_degrees_scinum(self, angle):
        expected, actual = math.degrees(angle), trig.degrees(ScienceNumber(angle, std_dev=0.01))
        self.assertIsInstance(actual, ScienceNumber)
        self.assertAlmostEqual(expected, float(actual))
        self.assertAlmostEqual(actual.std_dev, (180 / 3.14159265358) * 0.01)


    @sub_tests([
        x for x in range(0, 720, 15)
    ])
    def test_radians_scinum(self, x):
        expected, actual = math.radians(x), trig.radians(ScienceNumber(x, std_dev=0.0001))
        self.assertIsInstance(actual, ScienceNumber)
        self.assertAlmostEqual(expected, float(actual))
        self.assertAlmostEqual(actual.std_dev, (3.14159265358 / 180) * 0.0001)

    @sub_tests([
        (5, 5),
        (-5, 5),
        (5, -5),
        (-5, -5),
    ])
    def test_atan2(self, x, y):
        self.assertAlmostEqual(math.atan2(x, y), trig.atan2(x, y))

