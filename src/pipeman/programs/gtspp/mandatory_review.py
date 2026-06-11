from medsutil.ocproc2 import QCResult
from nodb.interface import NODB
from nodb.observations import NODBPlatform
from pipeman.programs.qc.base import DeepDiveChecker, ParentRecordRef, review
from autoinject import injector


class GTSPPMandatoryManualReviewTest(DeepDiveChecker):

    nodb: NODB = None

    @injector.construct
    def __init__(self):
        super().__init__('gtspp_mandatory_review', '1.0', test_tags=['GTSPP_5.1'])

    def parent_record_check(self, ref: ParentRecordRef):
        with self.nodb as db:
            platform = ref.record.metadata.get('CNODCPlatform', None)
            self.require_value(platform)
            nodb_platform = NODBPlatform.find_by_uuid(db, platform.to_string())
            if nodb_platform is not None and nodb_platform.metadata.get("require_review", False):
                self.update_qc_result(QCResult.MANUAL_REVIEW)
