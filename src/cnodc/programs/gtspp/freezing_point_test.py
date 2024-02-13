from cnodc.qc.base import BaseTestSuite, TestContext, SubRecordArray, ProfileLevelTest, RecordSetTest, RecordTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.ocean_math.seawater import eos80_freezing_point_t90


class GTSPPFreezingPointTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_freezing', '1_0', test_tags=['GTSPP_2.6'], **kwargs)

    @RecordTest(subrecord_type='PROFILE')
    def freezing_point_test(self, record: ocproc2.DataRecord, context: TestContext):
        self.precheck_value_in_map(record.parameters, 'PracticalSalinity')
        self.precheck_value_in_map(record.parameters, 'Temperature')
        psal = self.value_in_units(record.parameters.get('PracticalSalinity'), '0.001')
        if psal is None or psal < 26 or psal > 35:
            self.skip_test()
        pressure = self.calculate_pressure_in_dbar(
            record.coordinates.get('Pressure'),
            record.coordinates.get('Depth'),
            context.top_record.coordinates.get('Latitude')
        )
        if pressure is None:
            self.skip_test()
        freezing_point = eos80_freezing_point_t90(psal, pressure)
        with context.parameter_context('Temperature') as ctx2:
            self.test_all_subvalues(ctx2, self._test_freezing_point, fp=freezing_point)

    def _test_freezing_point(self, v: ocproc2.Value, ctx: TestContext, fp: float):
        temp = self.value_in_units(v, '°C', temp_scale='ITS-90')
        if temp > 0:
            return
        self.assert_greater_than('fp_temp_too_low', temp, fp, qc_flag=13)

