import pathlib
from pipeman.programs.qc.references import ReferenceRangeChecker


class GTSPPParameterRangeTest(ReferenceRangeChecker):

    def __init__(self):
        super().__init__(
            pathlib.Path(__file__).parent.absolute().resolve() / "gtspp_ref_ranges.yaml",
            test_protocol='gtspp',
            test_name='impossible_value_check',
            test_version='1.0',
            test_tags=['GTSPP_2.1', 'GTSPP_2.2'],
        )

