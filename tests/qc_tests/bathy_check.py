import unittest as ut
import typing as t
from uncertainties import UFloat, ufloat
from cnodc.bathymetry import BathymetryModel
import cnodc.ocproc2.structures as ocproc2
from cnodc.programs.gtspp.bathymetry_test import GTSPPBathymetryTest


class MockBathymetryModel(BathymetryModel):

    def __init__(self, results: t.Dict[tuple[t.Union[float, UFloat], t.Union[float, UFloat]], t.Union[float, UFloat]]):
        super().__init__('mock')
        self._lookup = results

    def water_depth(self,
                    x: t.Union[float, UFloat],
                    y: t.Union[float, UFloat]) -> t.Union[float, UFloat]:
        if (x, y) in self._lookup:
            return self._lookup[(x, y)]
        else:
            raise ValueError('Invalid coordinates')


class TestBathymetryCheck(ut.TestCase):

    def test_perfect_depth(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        z = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = z
        bathy_model = MockBathymetryModel({(x, y): z})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_above_water(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        z = 50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = z
        bathy_model = MockBathymetryModel({(x, y): z})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.MANUAL_REVIEW)
        self.assertEqual(dr.coordinates['Latitude'].metadata.best_value('WorkingQuality'), 14)
        self.assertEqual(dr.coordinates['Longitude'].metadata.best_value('WorkingQuality'), 14)
        self.assertIn('position_above_sea_level', [m.code for m in dr.qc_tests[0].messages])

    def test_sea_depth_within_10percent_below(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -45
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_sea_depth_within_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -55
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_sea_depth_more_than_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -65
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.MANUAL_REVIEW)
        self.assertEqual(dr.parameters['SeaDepth'].metadata.best_value('WorkingQuality'), 14)
        self.assertIn('sounding_bathymetry_mismatch', [m.code for m in dr.qc_tests[0].messages])

    def test_sea_depth_less_than_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -35
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.MANUAL_REVIEW)
        self.assertEqual(dr.parameters['SeaDepth'].metadata.best_value('WorkingQuality'), 14)
        self.assertIn('sounding_bathymetry_mismatch', [m.code for m in dr.qc_tests[0].messages])

    def test_sea_depth_exactly_10percent_below(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -55
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_sea_depth_exactly_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        dr.parameters['SeaDepth'] = -45
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_depth_exact(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -50
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_depth_below_10percent(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -60
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.MANUAL_REVIEW)
        self.assertEqual(1, len(dr.qc_tests[0].messages))
        self.assertEqual(sr.coordinates['Depth'].metadata.best_value('WorkingQuality'), 14)
        self.assertIn('depth_too_deep', [m.code for m in dr.qc_tests[0].messages])

    def test_depth_within_10percent_below(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -55
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_depth_within_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -45
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_depth_more_than_10percent_above(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -25
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): -50})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_depth_positive_in_range(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = 2
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): ufloat(-5, 10)})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_reference_positive_but_in_error(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -2
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): ufloat(5, 10)})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.PASS)
        self.assertEqual(0, len(dr.qc_tests[0].messages))

    def test_reference_positive_but_not_in_error(self):
        dr = ocproc2.DataRecord()
        x = -45
        y = -50
        dr.coordinates['Latitude'] = y
        dr.coordinates['Longitude'] = x
        sr = ocproc2.DataRecord()
        sr.coordinates['Depth'] = -2
        dr.subrecords.append_record_set('PROFILE', 0, sr)
        bathy_model = MockBathymetryModel({(x, y): ufloat(15, 10)})
        suite = GTSPPBathymetryTest(bathy_model)
        outcome, is_updated = suite.verify_record(dr)
        self.assertTrue(is_updated)
        self.assertEqual(outcome, ocproc2.QCResult.MANUAL_REVIEW)
        self.assertEqual(dr.coordinates['Latitude'].metadata.best_value('WorkingQuality'), 14)
        self.assertEqual(dr.coordinates['Longitude'].metadata.best_value('WorkingQuality'), 14)
        self.assertIn('position_above_sea_level', [m.code for m in dr.qc_tests[0].messages])
