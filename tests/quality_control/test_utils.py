from medsutil.ocproc2 import SingleElement
from tests.helpers.base_test_case import BaseTestCase
from medsutil.ocproc2.util import RequiredQuality, is_of_quality
import typing as t

RQ = RequiredQuality

REQUIRED_CHECKS = []
BASE_CHECKS = {
    RQ.NOT_ERRONEOUS: True,
    RQ.NOT_FINAL: True,
    RQ.GOOD_STRUCTURE: True,
    RQ.NOT_DUBIOUS: True,
    RQ.NOT_MISSING: True,
    RQ.HAS_UNITS: False,
    RQ.HAS_VALUE: True,
    RQ.IS_NUMERIC: False,
    RQ.IS_DATETIME: False,
    RQ.IS_INTEGER: False,
    RQ.IS_DURATION: False,
}
QUALITY_TESTS = [
    ({"Quality": 1}, {RQ.NOT_FINAL: False}),
    ({"Quality": 2}, {RQ.NOT_FINAL: False}),
    ({"Quality": 3}, {RQ.NOT_FINAL: False, RQ.NOT_DUBIOUS: False}),
    ({"Quality": 4}, {RQ.NOT_FINAL: False, RQ.NOT_ERRONEOUS: False}),
    ({"Quality": 5}, {RQ.NOT_FINAL: False}),
    ({"Quality": -1}, {RQ.NOT_FINAL: False, RQ.GOOD_STRUCTURE: False}),
    ({"Quality": 9}, {RQ.NOT_FINAL: False, RQ.NOT_MISSING: False}),
    ({"WorkingQuality": 1}, {}),
    ({"WorkingQuality": 2}, {}),
    ({"WorkingQuality": 3}, {RQ.NOT_DUBIOUS: False}),
    ({"WorkingQuality": 4}, {RQ.NOT_ERRONEOUS: False}),
    ({"WorkingQuality": 5}, {}),
    ({"WorkingQuality": -1}, {RQ.GOOD_STRUCTURE: False}),
    ({"WorkingQuality": 9}, {RQ.NOT_MISSING: False}),
]

def _extend_checks(element: str | int | float | None, checks: dict[RQ, bool], metadata: dict[str, t.Any] | None = None):
    for additional_metadata, additional_checks in QUALITY_TESTS:
        real_checks = {}
        real_checks.update(BASE_CHECKS)
        real_checks.update(additional_checks)
        real_checks.update(checks)
        REQUIRED_CHECKS.append((
            SingleElement(element, **(metadata or {}), **additional_metadata),
            [(x, real_checks[x]) for x in real_checks]
        ))

_extend_checks(1, checks={RQ.IS_INTEGER: True, RQ.IS_NUMERIC: True})
_extend_checks(0, checks={RQ.IS_INTEGER: True, RQ.IS_NUMERIC: True})
_extend_checks(1.23, checks={RQ.IS_NUMERIC: True})
_extend_checks("1.23", checks={RQ.IS_NUMERIC: True})
_extend_checks("1e-8", checks={RQ.IS_NUMERIC: True})
_extend_checks(None, checks={RQ.HAS_VALUE: False})
_extend_checks("", checks={RQ.HAS_VALUE: False})
_extend_checks("1e-8", checks={RQ.IS_NUMERIC: True, RQ.HAS_UNITS: True}, metadata={"Units": "m"})
_extend_checks(None, checks={RQ.HAS_VALUE: False, RQ.HAS_UNITS: True}, metadata={"Units": "m"})
_extend_checks("1e-8", checks={RQ.IS_NUMERIC: True}, metadata={"Units": ""})
_extend_checks(None, checks={RQ.HAS_VALUE: False}, metadata={"Units": ""})
_extend_checks("2012-10-11", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00:00", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00:00:00", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00+00:00", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00:00+00:00", checks={RQ.IS_DATETIME: True})
_extend_checks("2012-10-11T00:00:00+00:00", checks={RQ.IS_DATETIME: True})
_extend_checks("PT3H", checks={RQ.IS_DURATION: True})
_extend_checks("PT3M", checks={RQ.IS_DURATION: True})
_extend_checks("PT3S", checks={RQ.IS_DURATION: True})
_extend_checks("P3D", checks={RQ.IS_DURATION: True})


class TestQualityChecks(BaseTestCase):

    def test_quality(self):
        for check_element, checks in REQUIRED_CHECKS:
            for check_rq, result in checks:
                with self.subTest(check_element=check_element, check_rq=check_rq, result=result):
                    msg = "is unexpectedly not" if result else "is unexpectedly"
                    self.assertIs(result, is_of_quality(check_element, check_rq), msg=f"{repr(check_element)} {msg} {repr(check_rq)}")
