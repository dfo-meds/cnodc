import cnodc.ocproc2 as ocproc2
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest


class GTSPPMandatoryManualReviewTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_mandatory_review', '1.0', test_tags=['GTSPP_5.1'], **kwargs)

    @RecordTest(top_only=True)
    def test_top_record(self, record: ocproc2.ParentRecord, context: TestContext):
        station = self.load_station(context)
        if station.get_metadata('require_review', False):
            context.report_for_review('manual_review_required')
