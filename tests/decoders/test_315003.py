import itertools
import json

from medsutil.ocproc2 import ParentRecord, BaseRecord, ElementMap, RecordSet
from medsutil.seawater import TemperatureScale
from tests.helpers.base_test_case import BaseTestCase
from medsutil.ocproc2.codecs.wmo.bufr import _Bufr4Decoder, BufrCDSTables


class TestBufr315003(BaseTestCase):

    def test_decode(self):
        with open(self.data_file_path("bufr/315003.bufr"), "rb") as h:
            bufr_content = h.read()
        decoder = _Bufr4Decoder("test", bufr_content, BufrCDSTables())
        records = [x for x in decoder.convert_to_records()]

        self.assertEqual(1, len(records))
        record = records[0]

        self.assertIsInstance(record, ParentRecord)
        self.assert_element_equals(record.metadata, "WMOID", "1200345")
        self.assert_element_equals(record.metadata, "PlatformModel", "HelloWorld")
        self.assert_element_equals(record.metadata, "PlatformSerial", "123456")
        self.assert_element_equals(record.metadata, "WMOBuoyType", 1)
        self.assert_element_equals(record.metadata, "WMOCommunicationSystem", 2)
        self.assert_element_equals(record.metadata, "WMODataBuoyType", 3)
        self.assert_element_equals(record.metadata, "ProfileNumber", 5)
        self.assert_element_equals(record.coordinates, "Time", "2026-06-19T13:40+00:00", DatePrecision="minute")
        self.assert_element_equals(record.coordinates, "Latitude", 45.12341, Units="degrees_north", Quality=1, Datum="WGS84", Uncertainty=(0.000005, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.coordinates, "Longitude", 91.31215, Units="degrees_east", Quality=1, Datum="WGS84", Uncertainty=(0.000005, {"UncertaintyType": "uniform"}))

        self.assertIn("PROFILE", record.subrecords.record_sets)
        self.assertIn(0, record.subrecords.record_sets['PROFILE'])
        rs = record.subrecords.record_sets["PROFILE"][0]
        self.assertIsInstance(rs, RecordSet)
        self.assert_element_equals(rs.metadata, "ProfileDirection", "up")
        self.assertEqual(20, len(rs.records))

        depth = 2500000
        temp = 305.12
        psal = 13.4
        for sr in rs.records:
            self.assert_element_equals(sr.coordinates, "Pressure", depth, Units="Pa", Quality=1, Uncertainty=(500.0, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(sr.parameters, "Temperature", temp, Units="K", Quality=1, WMOProfileInstrumentType=4, TemperatureScale="ITS-90", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))
            self.assert_element_equals(sr.parameters, "PracticalSalinity", psal, Units="1e-3", Quality=1, WMOProfileInstrumentType=4, Uncertainty=(0.0005, {"UncertaintyType": "uniform"}))
            depth += 2500000
            temp -= 0.5
            psal += 0.1
