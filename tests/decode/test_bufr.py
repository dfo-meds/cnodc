import json
import unittest as ut
import pathlib
import cnodc.ocproc2 as ocproc2
from cnodc.codecs.gts import GtsCodec


class TestBufrParsing(ut.TestCase):

    def test_315004_1(self):
        f = pathlib.Path(__file__).absolute().parent / 'test_files/315004_1.bufr'
        codec = GtsCodec()
        records = [x for x in codec.load_all(f)]
        self.assertEqual(len(records), 1)
        self.assertIsInstance(records[0], ocproc2.ParentRecord)
        r = records[0]
        self.assertEqual(r.metadata.best_value('BUFRDescriptors'), [315004])
        self.assertEqual(r.metadata.best_value('CNODCInstrumentType'), 'XBT')
        self.assertEqual(r.metadata.best_value('GTSHeader'), 'IOSC01 RJTD 170700')
        self.assertEqual(r.metadata.best_value('StationID'), '7KET')
        self.assertEqual(r.metadata.best_value('ProfileID'), '')
        self.assertEqual(r.metadata.best_value('StationName'), '')
        self.assertEqual(r.metadata.best_value('ShipLineNumber'), '')
        self.assertEqual(r.metadata.best_value('SoftwareID'), '')
        self.assertEqual(r.coordinates.best_value('Time'), '2023-05-16T18:14+00:00')
        self.assertTrue(r.coordinates['Time'].is_iso_datetime())
        self.assertAlmostEqual(r.coordinates.best_value('Latitude'), 33.986)
        self.assertEqual(r.coordinates['Latitude'].metadata.best_value('Units'), 'degree_north')
        self.assertEqual(r.coordinates['Latitude'].metadata.best_value('Uncertainty'), 5e-6)
        self.assertAlmostEqual(r.coordinates.best_value('Longitude'), 137.491)
        self.assertEqual(r.coordinates['Longitude'].metadata.best_value('Units'), 'degree_east')
        self.assertEqual(r.coordinates['Longitude'].metadata.best_value('Uncertainty'), 5e-6)
        self.assertTrue('PROFILE' in r.subrecords)
        self.assertTrue(0 in r.subrecords['PROFILE'])
        self.assertFalse(1 in r.subrecords['PROFILE'])
        rs = r.subrecords['PROFILE'][0]
        levels = rs.records
        self.assertEqual(len(levels), 14)
        self.assertEqual(levels[0].metadata.best_value('WMODigitization'), 0)
        self.assertEqual(levels[0].coordinates.best_value('Depth'), 4)
        self.assertEqual(levels[0].coordinates['Depth'].metadata.best_value('Units'), 'm')
        self.assertEqual(levels[0].coordinates['Depth'].metadata.best_value('Uncertainty'), 0.005)
        self.assertEqual(levels[0].parameters.best_value('Temperature'), 296.15)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best_value('Units'), 'K')
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best_value('Uncertainty'), 0.005)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best_value('WMOProfileInstrumentType'), 212)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best_value('ProfilerSerialNumber'), "")

