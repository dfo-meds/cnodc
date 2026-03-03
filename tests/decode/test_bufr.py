import unittest as ut
import pathlib
import cnodc.ocproc2 as ocproc2
from cnodc.ocproc2.codecs.gts import GtsCodec


class TestBufrParsing(ut.TestCase):

    def test_315004_1(self):
        f = pathlib.Path(__file__).absolute().parent / 'test_files/315004_1.bufr'
        codec = GtsCodec()
        records = [x for x in codec.load_all(f)]
        self.assertEqual(len(records), 1)
        self.assertIsInstance(records[0], ocproc2.ParentRecord)
        r = records[0]
        self.assertEqual(r.metadata.best('BUFRDescriptors'), [315004])
        self.assertEqual(r.metadata.best('CNODCInstrumentType'), 'XBT')
        self.assertEqual(r.metadata.best('GTSHeader'), 'IOSC01 RJTD 170700')
        self.assertEqual(r.metadata.best('StationID'), '7KET')
        self.assertEqual(r.metadata.best('ProfileID'), None)
        self.assertEqual(r.metadata.best('StationName'), None)
        self.assertEqual(r.metadata.best('ShipLineNumber'), None)
        self.assertEqual(r.metadata.best('SoftwareID'), None)
        self.assertEqual(r.coordinates.best('Time'), '2023-05-16T18:14+00:00')
        self.assertTrue(r.coordinates['Time'].is_iso_datetime())
        self.assertAlmostEqual(r.coordinates.best('Latitude'), 33.986)
        self.assertEqual(r.coordinates['Latitude'].metadata.best('Units'), 'degree_north')
        self.assertEqual(r.coordinates['Latitude'].metadata.best('Uncertainty'), 5e-6)
        self.assertAlmostEqual(r.coordinates.best('Longitude'), 137.491)
        self.assertEqual(r.coordinates['Longitude'].metadata.best('Units'), 'degree_east')
        self.assertEqual(r.coordinates['Longitude'].metadata.best('Uncertainty'), 5e-6)
        self.assertTrue('PROFILE' in r.subrecords)
        self.assertTrue(0 in r.subrecords['PROFILE'])
        self.assertFalse(1 in r.subrecords['PROFILE'])
        rs = r.subrecords['PROFILE'][0]
        levels = rs.records
        self.assertEqual(len(levels), 14)
        self.assertEqual(levels[0].metadata.best('WMODigitization'), 0)
        self.assertEqual(levels[0].coordinates.best('Depth'), 4)
        self.assertEqual(levels[0].coordinates['Depth'].metadata.best('Units'), 'm')
        self.assertEqual(levels[0].coordinates['Depth'].metadata.best('Uncertainty'), 0.005)
        self.assertEqual(levels[0].parameters.best('Temperature'), 296.15)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best('Units'), 'K')
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best('Uncertainty'), 0.005)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best('WMOProfileInstrumentType'), 212)
        self.assertEqual(levels[0].parameters['Temperature'].metadata.best('ProfilerSerialNumber'), None)

