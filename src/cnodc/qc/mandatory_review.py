import datetime
from cnodc.ocproc2 import DataRecord
import cnodc.nodb.structures as structures
import typing as t
from cnodc.nodb import NODBControllerInstance
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest
import cnodc.ocproc2.structures as ocproc2


class NODBMandatoryManualReviewCheck(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('nodb_mandatory_review', '1.0', **kwargs)

    @RecordTest(top_only=True)
    def test_top_record(self, record: ocproc2.DataRecord, context: TestContext):
        context.report_for_review('manual_review_required')
