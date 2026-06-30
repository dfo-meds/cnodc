from medsutil.ocproc2 import SingleElement
from pipeman.programs.qc.dedupe import NODBDuplicateCheck, CompareResult
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestDedupe(BaseTestCase):

    @sub_tests([
        ("abc", "abc"),
        ("a", "a"),
        ("", ""),
    ], [
        ("abc", "abcd"),
        ("abé", "abe"),
        ("", "a"),
        ("a", ""),
    ])
    def test_compare_string(self, a: str, b: str):
        x = NODBDuplicateCheck()
        self.assertIs(x.compare_str_str(a, b), CompareResult.IDENTICAL)

    @sub_tests([
        (1, 1),
        (1.0, 1),
        (1, 1.0),
        (0, 0),
    ], [
        (1, 2),
        (2, 1),
    ])
    def test_compare_floats(self, a: str, b: str):
        x = NODBDuplicateCheck()
        self.assertIs(x.compare_float_float(a, b), CompareResult.IDENTICAL)

    @sub_tests([
        ("1", 1),
        ("0", 0),
        ("0.0", 0),
        ("0.0", 0.0),
    ], [
        ("1", 2),
        ("foo", 5.3),
    ])
    def test_compare_str_float(self, a: str, b: float | int):
        x = NODBDuplicateCheck()
        self.assertIs(x.compare_str_float(a, b), CompareResult.IDENTICAL)


    @sub_tests([
        (SingleElement("2015-01-02T03:04", DatePrecision="minute"), SingleElement("2015-01-02T03:04:05", DatePrecision="second"), CompareResult.B_BETTER),
        (SingleElement("2015-01-02T03:04:05", DatePrecision="second"), SingleElement("2015-01-02T03:04", DatePrecision="minute"), CompareResult.A_BETTER),
        (SingleElement("2015-01-02T03:04:05", DatePrecision="second"), SingleElement("2015-01-02T03:04:05", DatePrecision="second"), CompareResult.IDENTICAL),
        (SingleElement("2015-01-02T03:04:07", DatePrecision="second"), SingleElement("2015-01-02T03:04:05", DatePrecision="second"), CompareResult.DIFFERENT),
        (SingleElement("2015-01-02T03:04+00:00", DatePrecision="minute"), SingleElement("2015-01-02T05:04:05+02:00", DatePrecision="second"), CompareResult.B_BETTER),
        (SingleElement("2015-01-02T03:04:05+00:00", DatePrecision="second"), SingleElement("2015-01-02T05:04+02:00", DatePrecision="minute"), CompareResult.A_BETTER),
        (SingleElement("2015-01-02T03:04:05+00:00", DatePrecision="second"), SingleElement("2015-01-02T05:04:05+02:00", DatePrecision="second"), CompareResult.IDENTICAL),
        (SingleElement("2015-01-02T03:04:05+00:00", DatePrecision="second"), SingleElement("2015-01-02T03:04:05+02:00", DatePrecision="second"), CompareResult.DIFFERENT),
    ])
    def test_compare_datetimes(self, a: SingleElement, b: SingleElement, result: CompareResult):
        x = NODBDuplicateCheck()
        self.assertIs(x.compare_datetimes(a, b), result)

    @sub_tests([
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            CompareResult.IDENTICAL,
        ),
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(5.0, Units="degree_C", Uncertainty=SingleElement(0.05, UncertaintyType="uniform")),
            CompareResult.B_BETTER,
        ),
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(4.5, Units="degree_C", Uncertainty=SingleElement(0.05, UncertaintyType="uniform")),
            CompareResult.B_BETTER,
        ),
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(5.5, Units="degree_C", Uncertainty=SingleElement(0.05, UncertaintyType="uniform")),
            CompareResult.B_BETTER,
        ),
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(278.15, Units="degree_K", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            CompareResult.IDENTICAL,
        ),
        (
            SingleElement(5, Units="degree_C", Uncertainty=SingleElement(0.5, UncertaintyType="uniform")),
            SingleElement(5.6, Units="degree_C", Uncertainty=SingleElement(0.05, UncertaintyType="uniform")),
            CompareResult.DIFFERENT,
        ),
    ])
    def test_compare_parameter(self, a: SingleElement, b: SingleElement, result: CompareResult):
        x = NODBDuplicateCheck()
        self.assertIs(x.compare_parameter(a, b), result)

