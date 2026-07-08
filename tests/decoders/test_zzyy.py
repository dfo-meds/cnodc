import itertools
import json

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, BaseRecord, ElementMap, ChildRecord
from medsutil.ocproc2.codecs import GtsCodec
from tests.helpers.base_test_case import BaseTestCase


class TestFM18XII(BaseTestCase):


    def assert_element_equals(self, em: ElementMap, name: str, value, **kwargs):
        if value is not None:
            self.assertIn(name, em, msg=f"{name} is not present")
        else:
            self.assertNotIn(name, em, msg=f"{name} is unexpectedly present")
        self.assertEqual(em.best(name, default=None), value, msg=f"{name} does not match expected value")
        for kwarg in kwargs:
            value = kwargs[kwarg]
            if isinstance(value, tuple):
                self.assert_element_equals(em[name].metadata, kwarg, value[0], **value[1])
            else:
                self.assert_element_equals(em[name].metadata, kwarg, value)



    def test_decode(self):
        decoder = GtsCodec()
        with open(self.data_file_path("ascii/fm18_xii.txt"), "rb") as h:
            ascii_content = h.read()
        records = [x for x in decoder.load(ascii_content, received_date=AwareDateTime(2026, 7, 8, 10, 28, 0, tzinfo="Etc/UTC"))]
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertIsInstance(record, ParentRecord)
        self.assert_element_equals(record.metadata, "GTSHeader", "SSVX06 LFVW 092258")
        self.assert_element_equals(record.metadata, "WMOAsciiCodeForm", "ZZYY")
        self.assert_element_equals(record.metadata, "WMOID", "12345")
        self.assert_element_equals(record.coordinates, "Time", "2025-02-01T12:23:00+00:00", DatePrecision="minute", Quality="2")
        self.assert_element_equals(record.coordinates, "Latitude", "-12.345", Units="degrees_north", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}), Quality="1")
        self.assert_element_equals(record.coordinates, "Longitude", "-23.456", Units="degrees_east", Uncertainty=(0.0005, {"UncertaintyType": "uniform"}), Quality="1")
        self.assert_element_equals(record.metadata, "WMOQualityLocationClass", "2")
        self.assert_element_equals(record.parameters, "WindDirection", "230", Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}), Quality=None, WMOWindSource="1", WMOAnemometerType="4", SensorDepth=(-10, {"SensorDepthReference": "local_ground_corrected"}))
        self.assert_element_equals(record.parameters, "WindSpeed", "12", Units="m s-1", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), Quality=None, WMOWindSource="1", WMOAnemometerType="4", SensorDepth=(-10, {"SensorDepthReference": "local_ground_corrected"}))
        self.assert_element_equals(record.parameters, "AirTemperature", "4.1", Units="degrees_C", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality="2", TemperatureScale="ITS-90")
        self.assert_element_equals(record.parameters, "RelativeHumidity", "46", Units="1e-2", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), Quality=None)
        self.assert_element_equals(record.parameters, "AirPressure", "912.0", Units="hPa", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality=None)
        self.assert_element_equals(record.parameters, "AirPressureAtSeaLevel", "1010.4", Units="hPa", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality=None)
        self.assert_element_equals(record.parameters, "AirPressureChange", "5.1", Units="hPa", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality=None)
        self.assert_element_equals(record.parameters, "WMOAirPressureCharacteristic", "2")
        self.assert_element_equals(record.parameters, "Temperature", "-4.2", Units="degrees_C", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality="1", TemperatureScale="ITS-90")
        self.assert_element_equals(record.parameters, "WavePeriod", "12.4", Units="s", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality="1")
        self.assert_element_equals(record.parameters, "WaveHeight", "3.2", Units="m", Uncertainty=(0.05, {"UncertaintyType": "uniform"}), Quality="1")
        self.assertIn("PROFILE", record.subrecords)
        self.assertEqual(2, len(record.subrecords["PROFILE"]))
        self.assertIn(0, record.subrecords["PROFILE"])
        self.assertIn(1, record.subrecords["PROFILE"])
        self.assertEqual(4, len(record.subrecords["PROFILE"][0].records))
        for idx, d, t, ps in [
            (0, "25", "4.05", "19.04"),
            (1, "50", "4.01", "19.03"),
            (2, "75", "2.95", "17.05"),
            (3, "100", "-0.04", "6.43"),
        ]:
            subrecord = record.subrecords["PROFILE"][0].records[idx]
            self.assertIsInstance(subrecord, ChildRecord)
            self.assert_element_equals(subrecord.coordinates, "Depth", d, Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), WMOHSPCorrected="1")
            self.assert_element_equals(subrecord.parameters, "Temperature", t, Units="degrees_C", Uncertainty=(0.005, {"UncertaintyType": "uniform"}), TemperatureScale="ITS-90", Quality="1")
            self.assert_element_equals(subrecord.parameters, "PracticalSalinity", ps, Units="1e-3", Uncertainty=(0.005, {"UncertaintyType": "uniform"}), Quality="1", WMOSalinityDepthMeasurementMethod="2")
        self.assertEqual(5, len(record.subrecords["PROFILE"][1].records))
        for idx, d, cd, cs in [
            (0, "30", "160", "215"),
            (1, "60", "180", "225"),
            (2, "90", "190", "230"),
            (3, "120", "320", "130"),
            (4, "150", "310", "120"),
        ]:
            subrecord = record.subrecords["PROFILE"][1].records[idx]
            self.assertIsInstance(subrecord, ChildRecord)
            self.assert_element_equals(subrecord.coordinates, "Depth", d, Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), WMOHSPCorrected="1")
            self.assert_element_equals(subrecord.parameters, "CurrentDirection", cd, Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}), Quality="1", WMOPlatformMotionRemovalMethod="2", WMOCurrentMeasurementDuration="3")
            self.assert_element_equals(subrecord.parameters, "CurrentSpeed", cs, Units="cm s-1", Uncertainty=(0.5, {"UncertaintyType": "uniform"}), Quality="1", WMOPlatformMotionRemovalMethod="2", WMOCurrentMeasurementDuration="3")
        self.assert_element_equals(record.metadata, "WMOQualityPressure", "0")
        self.assert_element_equals(record.metadata, "WMOQualityHousekeeping", "1")
        self.assert_element_equals(record.metadata, "WMOQualityWaterTemperature", "0")
        self.assert_element_equals(record.metadata, "WMOQualityAirTemperature", "0")
        self.assert_element_equals(record.metadata, "WMOQualitySatellite", "1")
        self.assert_element_equals(record.metadata, "WMOQualityLocation", "1")
        self.assert_element_equals(record.metadata, "LastKnownPositionTime", "2025-01-05T11:43:00+00:00", DatePrecision="minute")
        self.assert_element_equals(record.parameters, "HydrostaticPressure", "8231", Units="kPa", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.metadata, "ThermistorCableLength", "143", Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.metadata, "WMODataBuoyType", "32")
        self.assert_element_equals(record.metadata, "WMODrogueType", "15")
        self.assert_element_equals(record.metadata, "PlatformLastKnownSpeed", "51", Units="cm s-1", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.metadata, "PlatformLastKnownDirection", "120", Units="degrees", Uncertainty=(5, {"UncertaintyType": "uniform"}))
        self.assert_element_equals(record.metadata, "BuoyEngineeringStatus", "0123456/")
        self.assert_element_equals(record.metadata, "DrogueCableLength", "5", Units="m", Uncertainty=(0.5, {"UncertaintyType": "uniform"}))




