from medsutil.dynamic import dynamic_name
from medsutil.ocproc2 import SingleElement
from pipeman.programs.gtspp.bathymetry import GTSPPBathymetryCheck
from tests.helpers.base_test_case import sub_tests
from tests.helpers.mock_bathymetry import MockBathymetryModel
from tests.helpers.qc_check_base import QCCheckerTestCase


class TestGTSPPBathymetryCheck(QCCheckerTestCase):

    @classmethod
    def setUpClass(cls):
        cls.bathymetry_check = GTSPPBathymetryCheck(
            bathymetry_model_class=dynamic_name(MockBathymetryModel)
        )

    @sub_tests([
        (73.0, 43.2),
        (85.3, 32.5),
        (85.1, 71.3),
        (-73, -54),
        (73.5, 55),
        (99.5, 66)
    ], [
        (81.0, 52.0)
    ])
    def test_not_on_land(self, lon: float, lat: float):
        with self.assertPassesQC():
            self.bathymetry_check.check_not_on_land(
                SingleElement(lat),
                SingleElement(lon)
            )

    @sub_tests([
        (73, 43, -25),
        (73, 43, -60),
        (73, 43, -75),
        (73, 43, -10),
        (73, 43, 0),
        (73, 43, -5),
        (-71, -34, -900),
        (-71, -34, -500),
        (-71, -34, -5),
        (-71, -34, 0),
        (-71, -34, -1000),
        (-71, -34, -1049),
        (-71, -34, -1050),
    ], [
        (-71, -34, -1051),
        (73, 43, -76),
        (-71, -34, -1205951),
        (73, 43, -1955123),
    ])
    def test_not_too_deep(self, lon: float, lat: float, depth: float):
        with self.assertPassesQC():
            self.bathymetry_check.check_not_too_deep(
                SingleElement(depth, Units="m"),
                lat,
                lon
            )