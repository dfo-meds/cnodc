import itertools
import json

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, BaseRecord, ElementMap, ChildRecord
from medsutil.ocproc2.codecs import GtsCodec
from tests.helpers.base_test_case import BaseTestCase


class TestFM63XI(BaseTestCase):

    def test_decode(self):
        decoder = GtsCodec()
        with open(self.data_file_path("ascii/fm63_xi.txt"), "rb") as h:
            ascii_content = h.read()
        records = [x for x in decoder.load(ascii_content, received_date=AwareDateTime(2026, 7, 8, 10, 28, 0, tzinfo="Etc/UTC"))]
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertIsInstance(record, ParentRecord)
        self.assert_element_equals(record.metadata, "GTSHeader", "SSVX06 LFVW 092258")
        self.assert_element_equals(record.metadata, "WMOAsciiCodeForm", "JJVV")
        self.assert_element_equals(record.coordinates, "Time", "2025-02-01T12:23:00+00:00", DatePrecision="minute")
        self.assert_element_equals(record.coordinates, "Latitude", "-12.345", Units="degrees_north", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.coordinates, "Longitude", "-23.456", Units="degrees_east", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))

        self.assert_element_equals(record.parameters, "WindDirection", "120", Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}), WMOWindInstrumentType="1")
        self.assert_element_equals(record.parameters, "WindSpeed", "23", Units="knots", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), WMOWindInstrumentType="1")
        self.assert_element_equals(record.parameters, "AirTemperature", "12.3", Units="degrees_C", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), TemperatureScale="ITS-90")

        self.assertIn("PROFILE", record.subrecords)
        self.assertEqual(1, len(record.subrecords["PROFILE"]))
        self.assertIn(0, record.subrecords["PROFILE"])
        self.assertEqual(8, len(record.subrecords["PROFILE"][0].records))
        for idx, d, t in [
            (0, "25", "4.5"),
            (1, "50", "4.4"),
            (2, "75", "4.3"),
            (3, "100", "4.2"),
            (4, "125", "4.1"),
            (5, "150", "4.0"),
            (6, "175", "3.9"),
            (7, "200", "-1.1"),
        ]:
            subrecord = record.subrecords["PROFILE"][0].records[idx]
            self.assertIsInstance(subrecord, ChildRecord)
            self.assert_element_equals(subrecord.coordinates, "Depth", d, Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(subrecord.parameters, "Temperature", t, Units="degrees_C", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), TemperatureScale="ITS-90", WMOProfileInstrumentType="123", WMOProfileRecorderType="45")
        self.assert_element_equals(record.subrecords["PROFILE"][0].metadata, "DigitizationMethod", "selected_depths")
        self.assert_element_equals(record.parameters, "SeaDepth", "200", Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.parameters, "CurrentDirection", "340", Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.parameters, "CurrentSpeed", "1.2", Units="knots", Uncertainty=(0.05, {"UncertaintyType": "uniform"}))




