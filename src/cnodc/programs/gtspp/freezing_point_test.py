from cnodc.qc.base import BaseTestSuite, TestContext, SubRecordArray, ProfileLevelTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.ocean_math.seawater import eos80_freezing_point_t90


class GTSPPFreezingPointTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_freezing', '1_0', test_tags=['GTSPP_2.6'], **kwargs)

    @ProfileLevelTest()
    def freezing_point_test(self, profile: SubRecordArray, current_level: int, context: TestContext):
        profile.require_good_value('PracticalSalinity', current_level)
        profile.require_good_value('Temperature', current_level)
        # TODO: what if multiple PSAL values are provided? what should we do?
        psal = self.value_in_units(profile.data['PracticalSalinity'][current_level], '0.001')
        if psal is None or psal < 26 or psal > 35:
            self.skip_test()
        pressure = self.calculate_pressure_in_dbar(
            profile.get_data('Pressure', current_level),
            profile.get_data('Depth', current_level),
            context.top_record.coordinates['Latitude']
        )
        if pressure is None:
            self.skip_test()
        freezing_point = eos80_freezing_point_t90(psal, pressure)
        with context.parameter_context('Temperature') as ctx2:
            self.test_all_subvalues(profile.data['Temperature'][current_level], ctx2, self._test_freezing_point, fp=freezing_point)

    def _test_freezing_point(self, v: ocproc2.Value, ctx: TestContext, fp: float):
        temp = self.value_in_units(v, '°C', temp_scale='ITS-90')
        if temp > 0:
            return
        self.assert_greater_than('fp_temp_too_low', temp, fp, qc_flag=13)

