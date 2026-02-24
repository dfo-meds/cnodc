from cnodc.programs.nodb.qc.qc import BaseTestSuite


class GTSPPConstantTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_constant_check', '1.0', test_tags=['GTSPP_2.5'], **kwargs)

    # TODO
