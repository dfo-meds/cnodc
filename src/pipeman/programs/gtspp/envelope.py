import pathlib
from pipeman.programs.qc.references import ReferenceRangeChecker


class GTSPPEnvelopeTest(ReferenceRangeChecker):

    def __init__(self):
        super().__init__(
            pathlib.Path(__file__).absolute().resolve().parent / "gtspp_envelopes.yaml",
            test_name="gtspp_envelopes",
            test_version="1.0",
            test_tags=["GTSPP_2.4"]
        )
