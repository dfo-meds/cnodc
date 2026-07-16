import itertools
import json

from medsutil.ocproc2 import ParentRecord, BaseRecord, ElementMap
from tests.helpers.base_test_case import BaseTestCase
from medsutil.ocproc2.codecs.wmo.bufr import _Bufr4Decoder, BufrCodeMap


class TestBufr315004(BaseTestCase):

    def test_decode(self):
        with open(self.data_file_path("ocproc2/315004_2.json"), "r") as h:
            good_version = ParentRecord.build_from_mapping(json.load(h))
        with open(self.data_file_path("bufr/315004_2.bufr"), "rb") as h:
            bufr_content = h.read()
        decoder = _Bufr4Decoder("test", bufr_content, BufrCodeMap())
        records = [x for x in decoder.convert_to_records()]
        self.assertEqual(1, len(records))
        self.assertEqual(records[0].metadata["ProfileID"].value, "1")
        self.assertEqual(records[0].metadata["PlatformID"].value, "12")
        self.assertEqual(records[0].metadata["IMONumber"].value, 123)
        self.assertEqual(records[0].metadata["PlatformName"].value, "ShippyMcShip")
        self.assertEqual(records[0].metadata["ShipLineNumber"].value, "1234")
        self.assertEqual(records[0].metadata["ShipTransectNumber"].value, 2)
        self.assertEqual(records[0].metadata["WMOAgencyCode"].value, 3)
        self.assertEqual(records[0].metadata["WMOXBTType"].value, 1)
        self.assertEqual(records[0].metadata["WMOID"].value, "1200345")
        self.assertEqual(records[0].metadata["PlatformSpeed"].value, 1)
        self.assertEqual(records[0].metadata["PlatformSpeed"].units(), "m s-1")
        self.assertEqual(records[0].metadata["PlatformSpeed"].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].metadata["PlatformDirection"].value, 234)
        self.assertEqual(records[0].metadata["PlatformDirection"].units(), "arc_degree")
        self.assertEqual(records[0].metadata["PlatformDirection"].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].metadata["XBTHeight"].value, 2)
        self.assertEqual(records[0].metadata["XBTHeight"].units(), "m")
        self.assertEqual(records[0].metadata["XBTHeight"].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].metadata['InstrumentManufacturingDate'].value, '2013-01-02')
        self.assertEqual(records[0].metadata['SoftwareID'].value, 'madeup')

        self.assertEqual(records[0].parameters['WindSpeed'].value, 1.8)
        self.assertEqual(records[0].parameters['WindSpeed'].units(), 'm s-1')
        self.assertEqual(records[0].parameters['WindSpeed'].metadata['Uncertainty'].value, 0.05)
        self.assertEqual(records[0].parameters['WindSpeed'].metadata['SensorDepth'].value, -0.05)
        self.assertEqual(records[0].parameters['WindSpeed'].metadata['SensorDepth'].units(), 'm')
        self.assertEqual(records[0].parameters['WindSpeed'].metadata['SensorDepth'].metadata['Uncertainty'].value, 0.005)
        self.assertEqual(records[0].parameters['WindSpeed'].metadata['SensorDepth'].metadata['SensorDepthReference'].value, 'local_ground')
        self.assertEqual(records[0].parameters['WindDirection'].value, 123)
        self.assertEqual(records[0].parameters['WindDirection'].units(), 'arc_degree')
        self.assertEqual(records[0].parameters['WindDirection'].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].parameters['WindDirection'].metadata['SensorDepth'].value, -0.05)
        self.assertEqual(records[0].parameters['WindDirection'].metadata['SensorDepth'].units(), 'm')
        self.assertEqual(records[0].parameters['WindDirection'].metadata['SensorDepth'].metadata['Uncertainty'].value, 0.005)
        self.assertEqual(records[0].parameters['WindDirection'].metadata['SensorDepth'].metadata['SensorDepthReference'].value, 'local_ground')

        self.assertEqual(records[0].parameters['AirTemperature'].value, 277.69)
        self.assertEqual(records[0].parameters['AirTemperature'].units(), 'K')
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['Uncertainty'].value, 0.005)
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['SensorDepth'].value, -5)
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['SensorDepth'].units(), 'm')
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['SensorDepth'].metadata['Uncertainty'].value, 0.05)
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['SensorDepth'].metadata['SensorDepthReference'].value, 'water')
        self.assertEqual(records[0].parameters['AirTemperature'].metadata['TemperatureScale'].value, "ITS-90")
        self.assertEqual(records[0].parameters['DewPointTemperature'].value, 301.23)
        self.assertEqual(records[0].parameters['DewPointTemperature'].units(), 'K')
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['TemperatureScale'].value, "ITS-90")
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['Uncertainty'].value, 0.005)
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['SensorDepth'].value, -5)
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['SensorDepth'].units(), 'm')
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['SensorDepth'].metadata['Uncertainty'].value, 0.05)
        self.assertEqual(records[0].parameters['DewPointTemperature'].metadata['SensorDepth'].metadata['SensorDepthReference'].value, 'water')

        self.assertEqual(records[0].parameters['WaveDirection'].value, 12)
        self.assertEqual(records[0].parameters['WaveDirection'].units(), 'arc_degree')
        self.assertEqual(records[0].parameters['WaveDirection'].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].parameters['WavePeriod'].value, 9)
        self.assertEqual(records[0].parameters['WavePeriod'].units(), 's')
        self.assertEqual(records[0].parameters['WavePeriod'].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].parameters['WaveHeight'].value, 0.1)
        self.assertEqual(records[0].parameters['WaveHeight'].units(), 'm')
        self.assertEqual(records[0].parameters['WaveHeight'].metadata['Uncertainty'].value, 0.05)

        self.assertEqual(records[0].parameters['CurrentDirection'].value, 192)
        self.assertEqual(records[0].parameters['CurrentDirection'].units(), 'arc_degree')
        self.assertEqual(records[0].parameters['CurrentDirection'].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].parameters['CurrentDirection'].metadata['WMOCurrentMeasurementDuration'].value, 2)
        self.assertEqual(records[0].parameters['CurrentDirection'].metadata['WMOCurrentMeasurementMethod'].value, 6)
        self.assertEqual(records[0].parameters['CurrentSpeed'].value, 5.43)
        self.assertEqual(records[0].parameters['CurrentSpeed'].units(), 'm s-1')
        self.assertEqual(records[0].parameters['CurrentSpeed'].metadata['Uncertainty'].value, 0.005)
        self.assertEqual(records[0].parameters['CurrentSpeed'].metadata['WMOCurrentMeasurementDuration'].value, 2)
        self.assertEqual(records[0].parameters['CurrentSpeed'].metadata['WMOCurrentMeasurementMethod'].value, 6)

        self.assertEqual(records[0].parameters['SeaDepth'].value, 1234)
        self.assertEqual(records[0].parameters['SeaDepth'].units(), 'm')
        self.assertEqual(records[0].parameters['SeaDepth'].metadata['Uncertainty'].value, 0.5)
        self.assertEqual(records[0].parameters['SeaDepth'].metadata['Quality'].value, 4)

        self.assertEqual(records[0].parameters['Temperature'].value, 274.662)
        self.assertEqual(records[0].parameters['Temperature'].units(), 'K')
        self.assertEqual(records[0].parameters['Temperature'].metadata['Uncertainty'].value, 0.0005)
        self.assertEqual(records[0].parameters['Temperature'].metadata['TemperatureScale'].value, "ITS-90")
        self.assertEqual(records[0].parameters['Temperature'].metadata['WMOTemperatureSalinityMeasurementMethod'].value, 7)
        self.assertEqual(records[0].parameters['Temperature'].metadata['SensorSerial'].value, "12345-12")
        self.assertEqual(records[0].parameters['Temperature'].metadata['SensorDepth'].value, 0.05)
        self.assertEqual(records[0].parameters['Temperature'].metadata['SensorDepth'].metadata['Units'].value, "m")
        self.assertEqual(records[0].parameters['Temperature'].metadata['SensorDepth'].metadata['Uncertainty'].value, 0.005)

        self.assertEqual(records[0].coordinates['Time'].value, '2026-06-19T13:40+00:00')
        self.assertEqual(records[0].coordinates['Latitude'].value, 45.12341)
        self.assertEqual(records[0].coordinates['Latitude'].units(), 'degrees_north')
        self.assertEqual(records[0].coordinates['Latitude'].metadata['Uncertainty'].value, 0.000005)
        self.assertEqual(records[0].coordinates['Latitude'].metadata['Datum'].value, "WGS84")
        self.assertEqual(records[0].coordinates['Longitude'].value, 91.31215)
        self.assertEqual(records[0].coordinates['Longitude'].units(), 'degrees_east')
        self.assertEqual(records[0].coordinates['Longitude'].metadata['Uncertainty'].value, 0.000005)
        self.assertEqual(records[0].coordinates['Longitude'].metadata['Datum'].value, "WGS84")

        self.assertIn("PROFILE", records[0].subrecords.record_sets)
        self.assertIn(0, records[0].subrecords.record_sets['PROFILE'])
        self.assertEqual(20, len(records[0].subrecords.record_sets['PROFILE'][0].records))

        self.assertEqual(records[0].subrecords.record_sets['PROFILE'][0].metadata['DigitizationMethod'].value, 'selected_depths')


        depth = 25
        temp = 305.12
        for sr in records[0].subrecords.record_sets['PROFILE'][0].records:
            self.assertEqual(sr.coordinates['Depth'].value, depth)
            self.assertEqual(sr.coordinates['Depth'].units(), 'm')
            self.assertEqual(sr.coordinates['Depth'].metadata['Quality'].value, 1)
            self.assertEqual(sr.coordinates['Depth'].metadata['Uncertainty'].value, 0.005)

            self.assertEqual(sr.parameters['Temperature'].value, temp)
            self.assertEqual(sr.parameters['Temperature'].units(), 'K')
            self.assertEqual(sr.parameters['Temperature'].metadata['Quality'].value, 1)
            self.assertEqual(sr.parameters['Temperature'].metadata['TemperatureScale'].value, 'ITS-90')
            self.assertEqual(sr.parameters['Temperature'].metadata['SensorSerial'].value, '2345-123')
            self.assertEqual(sr.parameters['Temperature'].metadata['Uncertainty'].value, 0.005)
            depth += 25
            temp -= 0.5