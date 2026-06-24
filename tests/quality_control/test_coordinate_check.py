from medsutil.ocproc2 import SingleElement
from pipeman.programs.gtspp.coordinate import GTSPPCoordinateCheck
from tests.helpers.base_test_case import sub_tests
from tests.helpers.qc_check_base import QCCheckerTestCase


GOOD_COORDINATES = [
    SingleElement(5),
    SingleElement("5"),
    SingleElement(0),
    SingleElement("0"),
    SingleElement(0.0),
    SingleElement("0.0"),
    SingleElement(-90),
    SingleElement("90"),
    SingleElement(5.123),
    SingleElement("5.12351"),
]

BAD_COORDINATES = [
    SingleElement(None),
    SingleElement(""),
]

class TestCoordinateCheck(QCCheckerTestCase):

    @sub_tests(GOOD_COORDINATES, BAD_COORDINATES)
    def test_element_require_value(self, element: SingleElement):
        x = GTSPPCoordinateCheck()
        with self.assertPassesQC():
            x.element_require_value(element)


