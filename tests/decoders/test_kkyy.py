import itertools
import json

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, BaseRecord, ElementMap, ChildRecord
from medsutil.ocproc2.codecs import GtsCodec
from tests.helpers.base_test_case import BaseTestCase


class TestFM64X(BaseTestCase):

    def test_decode(self):
        decoder = GtsCodec()
        with open(self.data_file_path("ascii/fm64-x.txt"), "rb") as h:
            ascii_content = h.read()
        records = [x for x in decoder.load(ascii_content, received_date=AwareDateTime(2026, 7, 8, 10, 28, 0, tzinfo="Etc/UTC"))]
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertIsInstance(record, ParentRecord)
        self.assert_element_equals(record.metadata, "GTSHeader", "SSVX06 LFVW 092258")
        self.assert_element_equals(record.metadata, "WMOAsciiCodeForm", "KKYY")
        self.assert_element_equals(record.coordinates, "Time", "2025-02-01T12:23:00+00:00", DatePrecision="minute")
        self.assert_element_equals(record.coordinates, "Latitude", "-12.345", Units="degrees_north", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.coordinates, "Longitude", "-23.456", Units="degrees_east", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))

        self.assert_element_equals(record.parameters, "WindDirection", "120", Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}), WMOWindInstrumentType="1")
        self.assert_element_equals(record.parameters, "WindSpeed", "23", Units="knots", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), WMOWindInstrumentType="1")
        self.assert_element_equals(record.parameters, "AirTemperature", "12.3", Units="degrees_C", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), TemperatureScale="ITS-90")

        self.assertIn("PROFILE", record.subrecords)
        self.assertEqual(2, len(record.subrecords["PROFILE"]))
        self.assertIn(0, record.subrecords["PROFILE"])
        self.assertEqual(8, len(record.subrecords["PROFILE"][0].records))
        for idx, d, t, ps in [
            (0, "25", "4.51", "12.34"),
            (1, "50", "4.42", "12.45"),
            (2, "75", "4.33", "12.56"),
            (3, "100", "4.24", "12.67"),
            (4, "125", "4.15", "12.78"),
            (5, "150", "4.06", "12.89"),
            (6, "175", "3.97", "12.90"),
            (7, "200", "-1.18", "8.01"),
        ]:
            subrecord = record.subrecords["PROFILE"][0].records[idx]
            self.assertIsInstance(subrecord, ChildRecord)
            self.assert_element_equals(subrecord.coordinates, "Depth", d, Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(subrecord.parameters, "Temperature", t, Units="degrees_C", Uncertainty=(0.005, {"UncertaintyType": "uniform"}), TemperatureScale="ITS-90", WMOProfileInstrumentType="123", WMOProfileRecorderType="45")
            self.assert_element_equals(subrecord.parameters, "PracticalSalinity", ps, Units="1e-3", Uncertainty=(0.005, {"UncertaintyType": "uniform"}), WMOProfileInstrumentType="123", WMOProfileRecorderType="45", WMOSalinityDepthMeasurementMethod="2")

        self.assertIn(1, record.subrecords["PROFILE"])
        for idx, d, cd, cs in [
            (0, "30", "120", "5"),
            (1, "60", "130", "10"),
            (2, "90", "140", "15"),
            (3, "120", "320", "154"),
        ]:
            subrecord = record.subrecords["PROFILE"][1].records[idx]
            self.assertIsInstance(subrecord, ChildRecord)
            self.assert_element_equals(subrecord.coordinates, "Depth", d, Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(subrecord.parameters, "CurrentDirection", cd, Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(subrecord.parameters, "CurrentSpeed", cs, Units="cm s-1", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.subrecords["PROFILE"][0].metadata, "DigitizationMethod", "inflection_points")
        self.assert_element_equals(record.parameters, "SeaDepth", "300", Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.metadata, "WMOID", "1200345")
        self.assert_element_equals(record.metadata, "PlatformID", None)





