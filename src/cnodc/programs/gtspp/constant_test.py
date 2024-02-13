from uncertainties import UFloat, ufloat

from cnodc.bathymetry import BathymetryModel
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext
import cnodc.ocproc2.structures as ocproc2
from cnodc.util import dynamic_object


class GTSPPConstantTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_constant_check', '1.0', test_tags=['GTSPP_2.5'], **kwargs)

    # TODO
