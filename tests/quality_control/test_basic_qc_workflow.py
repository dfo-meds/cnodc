from medsutil.ocproc2 import QCResult
from pipeman.programs.nodb.qc.qc import BaseTestSuite, MetadataTest, TestContext
from medsutil import ocproc2
from tests.helpers.base_test_case import BaseTestCase


class BoringTest(BaseTestSuite):

    @MetadataTest("test_boring_1", "Units")
    def check_boring_1(self, value: ocproc2.SingleElement, context):
        self.assert_true(value.to_string() == "m", "oh_no", 4)


class TestBoringTest(BaseTestCase):

    def test_check_boring_1_with_workflow(self):

        # Simulate an original record
        record = ocproc2.ParentRecord()
        record.parameters['LunchLocationX'] = ocproc2.SingleElement("2015-01-02T03:04:05", Units="degrees")

        # Simulate running a QC test on it
        ctx = TestContext(record, {}, None)
        bt = BoringTest("boring", "1.0")
        bt.run_tests(ctx)
        self.assertEqual(ctx.result, QCResult.MANUAL_REVIEW)
        units_meta = record.parameters['LunchLocationX'].metadata['Units'].metadata
        self.assertIn('SystemRecommendedQuality', units_meta)
        self.assertIn('SystemIdentifier', units_meta)
        self.assertEqual(4, units_meta.best('SystemRecommendedQuality', coerce=int, default=None))
        self.assertEqual('test_boring_1', units_meta.best('SystemIdentifier', coerce=str, default=None))

        # Simulate the user's actions
        units_meta['UserProvidedQuality'] = 4
        del units_meta['SystemRecommendedQuality']
        bt.run_tests(ctx)
        self.assertIn('WorkingQuality', units_meta)
        self.assertEqual(4, units_meta.best('WorkingQuality', coerce=int, default=None))
        self.assertNotIn('UserProvidedQuality', units_meta)
        self.assertNotIn('SystemIdentifier', units_meta)
        self.assertNotIn('SystemRecommendedQuality', units_meta)
