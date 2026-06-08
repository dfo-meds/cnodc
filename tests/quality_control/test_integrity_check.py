from medsutil.ocproc2 import SingleElement, ParentRecord, QCResult
from pipeman.programs.nodb.qc.integrity_check import NODBIntegrityCheck
from pipeman.programs.nodb.qc.qc import QCAssertionError, TestContext
from tests.helpers.base_test_case import BaseTestCase


VALID_UNITS = [
    "m",
    "bar",
    "dbar",
    "Pa",
    "km",
    "mm",
    "degree_north",
    "degree_east",
]

INVALID_UNITS = [
    "garbage",
    "@!#$%"
]

class TestIntegrityCheck(BaseTestCase):

    def test_bad_units(self):
        x = NODBIntegrityCheck()
        for unit in VALID_UNITS:
            with self.subTest(good_unit=unit):
                self.assertIsNone(x.units_check(SingleElement(unit), None))
        for unit in INVALID_UNITS:
            with self.subTest(bad_unit=unit):
                with self.assertRaises(QCAssertionError):
                    x.units_check(SingleElement("helloworld"), None)

    def test_record_with_bad_units(self):
        x = NODBIntegrityCheck()
        y = ParentRecord()
        y.coordinates['Latitude'] = SingleElement(56.124, Units='foobar')
        ctx = TestContext(y, {}, None)
        x.run_tests(ctx)
        self.assertEqual(21, y.coordinates['Latitude'].metadata['WorkingQuality'].value)
        self.assertIs(ctx.result, QCResult.MANUAL_REVIEW)

    def test_record_with_good_units(self):
        x = NODBIntegrityCheck()
        y = ParentRecord()
        y.coordinates['Latitude'] = SingleElement(56.124, Units='degree_north')
        y.coordinates['Longitude'] = SingleElement(47.124, Units='degree_east')
        y.coordinates['Time'] = SingleElement("2015-10-11T01:02:03+00:00")
        ctx = TestContext(y, {}, None)
        x.run_tests(ctx)
        for msg in ctx.qc_messages:
            print(msg.to_mapping())
        self.assertIs(ctx.result, QCResult.PASS)