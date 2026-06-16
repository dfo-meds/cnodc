from contextlib import contextmanager

from medsutil import ocproc2
from medsutil.ocproc2 import SingleElement
from pipeman.programs.qc.integrity import NODBIntegrityCheck
from pipeman.programs.qc import QCAssertionError, TestContext, QCSkipCheck
from tests.helpers.base_test_case import BaseTestCase

TEST_ELEMENTS = [

    ("PracticalSalinity", "parameters", 12, "0.001"),
    ("Latitude", "coordinates", 11, "degrees_north"),
    ("XBTHeight", ("metadata:record:parent", "metadata:record:child", "metadata:element"), 10, "m"),
    ("CNODCDuplicateDate", "metadata:record:parent", "2009-08-07", None),
    ("BatteryDescription", "metadata:record:parent", "six batteries", None),
    ("Abstract", "metadata:record:parent", "five golden rings", None),
    ("CreationTime", "metadata:record:parent", "2004-03-02:01:00:00+00:00", None),
    ("BatteryVoltage", ("metadata:record:parent", "metadata:record:child"), 12, "V"),
    ("AnemometerType", "metadata:element", "cup", None),

]

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

GOOD_SRT_COORDINATE_TESTS = [
    ("TSERIES", "Time", "2015-01-02T00:00:00+00:00"),
    ("TSERIES", "TimeOffset", 92),
    ("PROFILE", "Depth", 50),
    ("PROFILE", "Pressure", 9172),
    ("WAVE_SENSORS", "WaveSensor", 1),
    ("SPEC_WAVE", "CentralFrequency", 52),
    ("TSERIES", "Time", "2015-01-02T00:00:00+00:00", "Depth", 50, "Latitude", 64, "Longitude", 42),
    ("PROFILE", "Time", "2015-01-02T00:00:00+00:00", "Depth", 50, "Latitude", 64, "Longitude", 42),
]
BAD_SRT_COORDINATE_TESTS = [
    ("TSERIES",),
    ("PROFILE",),
    ("WAVE_SENSORS",),
    ("SPEC_WAVE",),
    ("TSERIES", "Depth", 50),
    ("PROFILE", "Time", "2015-01-02T00:00:00+00:00"),
    ("WAVE_SENSORS", "Depth", 50),
    ("SPEC_WAVE", "Depth", 50),
]

GOOD_VALUES = [
    (278, "K", "degree_C", 4.85, None, None, None),
    (278, "K", "degree_C", None, 4.85, None, None),
    (278, "K", "degree_C", 4.5, 5, None, None),
    (278, "K", "degree_C", None, None, None, None),
    (278, "K", "K", None, None, "integer", None),
    (278, "K", "K", None, None, "decimal", None),
    (278, "K", "K", None, None, "string", None),
    (278.15, "K", "K", None, None, "decimal", None),
    (278.15, "K", "K", None, None, "string", None),
    ("2015-01-02", None, None, None, None, "date", None),
    ("2015-01-02", None, None, None, None, "dateTimeStamp", None),
    ("2015-01-02T00:00:00+00:00", None, None, None, None, "date", None),
    ("2015-01-02T00:00:00+00:00", None, None, None, None, "dateTimeStamp", None),
    ("2015-01-02T00:00:00+00:00", None, None, None, None, "string", None),
    ("2015-01-02T00:00:00+00:00", None, None, None, None, "string", None),
    ([4, 5], None, None, None, None, "List", None),
    (5, None, None, None, None, "integer", ("0", "1", "2", "3", "4", "5", "9")),
    (5, None, None, None, None, "integer", (0, 1, 2, 3, 4, 5, 9)),
    ("5", None, None, None, None, "integer", ("0", "1", "2", "3", "4", "5", "9")),
    ("5", None, None, None, None, "integer", (0, 1, 2, 3, 4, 5, 9)),
]

BAD_VALUES = [
    ("integrity_invalid_units", 21, 278, "Pa", "degree_C", None, None, None, None),
    ("integrity_lower_than_range", 14, 278, "K", "degree_C", 5, None, None, None),
    ("integrity_greater_than_range", 14, 278, "K", "degree_C", None, 4, None, None),
    ("integrity_invalid_integer", 20, 278.15, "K", "K", None, None, "integer", None),
    ("integrity_invalid_integer", 20, "2015-01-02", None, None, None, None, "integer", None),
    ("integrity_invalid_integer", 20, "2015-01-02T00:00:00+00:00", None, None, None, None, "integer", None),
    ("integrity_invalid_integer", 20, [4, 5], None, None, None, None, "integer", None),
    ("integrity_invalid_decimal", 20, "2015-01-02", None, None, None, None, "decimal", None),
    ("integrity_invalid_decimal", 20, "2015-01-02T00:00:00+00:00", None, None, None, None, "decimal", None),
    ("integrity_invalid_decimal", 20, [4, 5], None, None, None, None, "decimal", None),
    ("integrity_invalid_datetime", 20, [4, 5], None, None, None, None, "date", None),
    ("integrity_invalid_datetime", 20, 278, "K", "K", None, None, "dateTimeStamp", None),
    ("integrity_invalid_datetime", 20, 278.15, "K", "K", None, None, "dateTimeStamp", None),
    ("integrity_invalid_datetime", 20, 278, "K", "K", None, None, "date", None),
    ("integrity_invalid_datetime", 20, 278.15, "K", "K", None, None, "date", None),
    ("integrity_invalid_datetime", 20, [4, 5], None, None, None, None, "dateTimeStamp", None),
    ("integrity_invalid_list", 20, 278, "K", "K", None, None, "List", None),
    ("integrity_invalid_list", 20, 278.15, "K", "K", None, None, "List", None),
    ("integrity_invalid_list", 20, "2015-01-02", None, None, None, None, "List", None),
    ("integrity_invalid_list", 20, "2015-01-02T00:00:00+00:00", None, None, None, None, "List", None),
    ("integrity_invalid_string", 20, [4, 5], None, None, None, None, "string", None),
    ("integrity_value_not_allowed", 14, 12, None, None, None, None, "integer", ("0", "1", "2", "3", "4", "5", "9")),
    ("integrity_value_not_allowed", 14, -5, None, None, None, None, "integer", (0, 1, 2, 3, 4, 5, 9)),
    ("integrity_value_not_allowed", 14, "foobar", None, None, None, None, "integer", ("0", "1", "2", "3", "4", "5", "9")),
    ("integrity_value_not_allowed", 14, "-5", None, None, None, None, "integer", (0, 1, 2, 3, 4, 5, 9)),
]

class TestIntegrityCheck(BaseTestCase):

    @contextmanager
    def assertPassesQC(self):
        try:
            yield {}
        except QCAssertionError as ex:
            raise self.failureException(f"Test unexpectedly failed") from ex

    @contextmanager
    def assertFailsQC(self, error_code: str | None = None, flag_number: int | None = None):
        with self.assertRaises(QCAssertionError) as h:
            yield h
        if error_code is not None:
            self.assertEqual(error_code, h.exception.specific_test_name)
        if flag_number is not None:
            self.assertEqual(flag_number, h.exception.flag_number)

    @contextmanager
    def assertSkipsQC(self):
        with self.assertRaises(QCSkipCheck) as h:
            yield h

    def test_units_check(self):
        x = NODBIntegrityCheck()
        for unit in VALID_UNITS:
            with self.subTest(good_unit=unit):
                with self.assertPassesQC():
                    x.units_check(SingleElement(unit), None)
        for unit in INVALID_UNITS:
            with self.subTest(bad_unit=unit):
                with self.assertFailsQC("integrity_invalid_units", 21):
                    x.units_check(SingleElement("helloworld"), None)

    def test_child_record_coordinate_check(self):
        def _build_record_and_context(test_tuple: tuple):
            r = ocproc2.ParentRecord()
            child_record = ocproc2.ChildRecord()
            for k in range(1, len(test_tuple), 2):
                child_record.coordinates[test_tuple[k]] = test_tuple[k + 1]
            r.subrecords.append_to_record_set(test_tuple[0], 0, child_record)
            ctx = TestContext(r, {}, None)
            ctx.current_record = child_record
            ctx.current_subrecord_type = test_tuple[0]
            return child_record, ctx
        x = NODBIntegrityCheck()
        for good_test in GOOD_SRT_COORDINATE_TESTS:
            with self.subTest(good_test=good_test):
                self.assertIsNone(x.child_record_coordinate_check(*_build_record_and_context(good_test)))
        for bad_test in BAD_SRT_COORDINATE_TESTS:
            with self.subTest(bad_test=bad_test):
                with self.assertFailsQC("integrity_missing_subrecord_coordinate"):
                    x.child_record_coordinate_check(*_build_record_and_context(bad_test))

    def test_working_quality_skipped(self):
        x = NODBIntegrityCheck()
        with self.assertSkipsQC():
            x._verify_element("metadata", "WorkingQuality", ocproc2.SingleElement(2), None)

    def test_strict_flags_invalid_element(self):
        x = NODBIntegrityCheck(strict_mode=True)
        with self.assertFailsQC("integrity_undefined_element", 20):
            x._verify_element("metadata", "_Never_Going_To_Be_An_Element_Name", ocproc2.SingleElement(5), None)

    def test_relaxed_skips_invalid_element(self):
        x = NODBIntegrityCheck(strict_mode=False)
        with self.assertSkipsQC():
            x._verify_element("metadata", "_Never_Going_To_Be_An_Element_Name", ocproc2.SingleElement(5), None)

    def test_group_names(self):
        x = NODBIntegrityCheck()
        r = ocproc2.ParentRecord()
        ctx = TestContext(r, {}, None)
        ctx.current_record = r
        for element_name, ideal_type, value, units in TEST_ELEMENTS:
            for possible_type in ("coordinates", "parameters", "metadata:record:parent", "metadata:record:child", "metadata:element"):
                with self.subTest(element_name=element_name, ideal_type=ideal_type, test_type=possible_type):
                    ctx.current_value = ocproc2.SingleElement(value, Units=units)
                    if ideal_type == possible_type or (isinstance(ideal_type, tuple) and possible_type in ideal_type):
                        with self.assertPassesQC():
                            x._verify_element(
                                possible_type,
                                element_name,
                                ctx.current_value,
                                ctx
                            )
                    else:
                        with self.assertFailsQC("integrity_invalid_group", 20):
                            x._verify_element(possible_type, element_name, ctx.current_value, ctx)

    def test_do_not_allow_multiple_elements(self):
        x = NODBIntegrityCheck(strict_mode=False)
        with self.assertFailsQC():
            r = ocproc2.ParentRecord()
            r.parameters["Temperature"] = ocproc2.SingleElement(5.2, TemperatureScale=ocproc2.MultiElement(
                ("ITS-90", "IPTS-68")))
            x._verify_element("metadata:element", "TemperatureScale", r.parameters["Temperature"].metadata["TemperatureScale"], None)

    def test_good_values(self):
        x = NODBIntegrityCheck(strict_mode=False)
        for value, value_units, preferred_unit, min_value, max_value, data_type, allowed_values in GOOD_VALUES:
            with self.subTest(value=(value, value_units), min=(min_value, preferred_unit), max=(max_value, preferred_unit)):
                with self.assertPassesQC():
                    x._test_element_value(
                        ocproc2.SingleElement(value, Units=value_units),
                        None,
                        preferred_unit=preferred_unit,
                        min_value=min_value,
                        max_value=max_value,
                        data_type=data_type,
                        allowed_values=allowed_values
                    )

    def test_bad_values(self):
        x = NODBIntegrityCheck(strict_mode=False)
        for error_code, flag_number, value, actual_unit, preferred_unit, min_value, max_value, data_type, allowed_values in BAD_VALUES:
            with self.subTest(error_code=error_code, value=(value, actual_unit), min=(min_value, preferred_unit), max=(max_value, preferred_unit), data_type=data_type, allowed_values=allowed_values):
                with self.assertFailsQC(error_code, flag_number):
                    x._test_element_value(
                        ocproc2.SingleElement(value, Units=actual_unit),
                        None,
                        preferred_unit=preferred_unit,
                        min_value=min_value,
                        max_value=max_value,
                        data_type=data_type,
                        allowed_values=allowed_values
                    )


