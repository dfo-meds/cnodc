from medsutil.ocproc2 import QCResult
from medsutil import ocproc2
from medsutil.ocproc2.refs import ParentRecordRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import DeepDiveChecker
from tests.helpers.base_test_case import BaseTestCase


class BoringParentRecordCheck(DeepDiveChecker):

    def __init__(self, rq: RequiredQuality):
        super().__init__(test_name="boring_parent", test_version="1.0")
        self._rq = rq

    def parent_record_check(self, ref: ParentRecordRef):
        metadata = ref.setdefault_metadata_ref("Boring")
        with self.review("check", metadata, fail_flag=4, pass_flag=1) as ctx:
            ctx.check_review_already_complete(required_quality=self._rq)
            self.assert_true(metadata.element.value == 5)


class TestBoringParentRecordCheck(BaseTestCase):

    def test_check_boring_fail_empty(self):
        pr = ocproc2.ParentRecord()
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.MANUAL_REVIEW)
        self.assertIn("Boring", pr.metadata)
        self.assertIsNone(pr.metadata["Boring"].value)
        self.assertNotIn("Quality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["WorkingQuality"].value, 4)

    def test_check_boring_fail_invalid(self):
        pr = ocproc2.ParentRecord()
        pr.metadata["Boring"] = 3
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.MANUAL_REVIEW)
        self.assertIn("Boring", pr.metadata)
        self.assertNotIn("Quality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["WorkingQuality"].value, 4)

    def test_check_boring_pass(self):
        pr = ocproc2.ParentRecord()
        pr.metadata["Boring"] = 5
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.PASS)
        self.assertIn("Boring", pr.metadata)
        self.assertNotIn("Quality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["WorkingQuality"].value, 1)

    def test_check_boring_skip_already_erroneous(self):
        pr = ocproc2.ParentRecord()
        pr.metadata["Boring"] = ocproc2.SingleElement(None, Quality=4)
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.SKIP)
        self.assertIn("Boring", pr.metadata)
        self.assertNotIn("WorkingQuality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["Quality"].value, 4)

    def test_check_boring_pass_while_dubious(self):
        pr = ocproc2.ParentRecord()
        pr.metadata["Boring"] = ocproc2.SingleElement(5, Quality=3)
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.PASS)
        self.assertIn("Boring", pr.metadata)
        self.assertIn("WorkingQuality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["WorkingQuality"].value, 3)
        self.assertEqual(pr.metadata["Boring"].metadata["Quality"].value, 3)

    def test_check_boring_fail_while_dubious(self):
        pr = ocproc2.ParentRecord()
        pr.metadata["Boring"] = ocproc2.SingleElement(4, Quality=3)
        checker = BoringParentRecordCheck(RequiredQuality.NOT_ERRONEOUS)
        qc_result = checker.run_record_check(pr)
        self.assertIs(qc_result.result, QCResult.MANUAL_REVIEW)
        self.assertIn("Boring", pr.metadata)
        self.assertIn("WorkingQuality", pr.metadata["Boring"].metadata)
        self.assertEqual(pr.metadata["Boring"].metadata["WorkingQuality"].value, 4)
        self.assertEqual(pr.metadata["Boring"].metadata["Quality"].value, 3)


