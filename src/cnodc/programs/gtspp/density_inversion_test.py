import typing as t
from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest
import cnodc.ocproc2 as ocproc2
from cnodc.ocean_math.seawater import eos80_density_at_depth_t90
import cnodc.ocean_math.ocproc2int as oom


class GTSPPDensityInversionTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_density', '1_0', test_tags=['GTSPP_2.10'], **kwargs)

    @RecordSetTest('PROFILE')
    def density_inversion_test(self, record_set: ocproc2.RecordSet, context: TestContext):
        if len(record_set.records) < 2:
            self.skip_test()
        previous_density, _, _ = oom.calc_density_record(record_set.records[0], context.top_record)
        for i in range(1, len(record_set.records)):
            with context.subrecord_from_current_set_context(i) as ctx:
                current_density, _, _ = oom.calc_density_record(record_set.records[i], context.top_record)
                if current_density is None:
                    continue
                with ctx.two_parameter_context('Temperature', 'PracticalSalinity' if 'PracticalSalinity' in record_set.records[i] else 'AbsoluteSalinity'):
                    self.assert_greater_than('density_inversion_detected', current_density, previous_density)
                previous_density = current_density
