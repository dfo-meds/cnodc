from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest, RecordTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.ocean_math.seawater import eos80_freezing_point_t90
import cnodc.ocean_math.ocproc2int as oom

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
        freezing_point, _, _ = oom.calc_freezing_point(
            pressure=record.coordinates.get('Pressure'),
            depth=record.coordinates.get('Depth'),
            latitude=context.top_record.coordinates.get('Latitude'),
            practical_salinity=record.parameters.get('PracticalSalinity'),
            absolute_salinity=record.parameters.get('AbsoluteSalinity'),
            units='°C',
            temperature_scale='ITS-90'
        )
        if freezing_point is None:
            self.skip_test()
        with context.parameter_context('Temperature') as ctx2:
            self.test_all_subvalues(ctx2, self._test_freezing_point, fp=freezing_point)

    def _test_freezing_point(self, v: ocproc2.Value, ctx: TestContext, fp: float):
        self.precheck_value(v)
        temp = oom.get_temperature(
            temperature=v,
            units='°C',
            temperature_scale='ITS-90',
            obs_date=ctx.top_record.coordinates.get('Time')
        )
        if temp > 0:
            return
        self.assert_greater_than('fp_temp_too_low', temp, fp, qc_flag=13)

