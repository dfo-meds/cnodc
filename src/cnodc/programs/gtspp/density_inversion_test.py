import typing as t
from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.ocean_math.seawater import eos80_density_at_depth_t90


class GTSPPDensityInversionTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_density', '1_0', test_tags=['GTSPP_2.10'], **kwargs)

    @RecordSetTest('PROFILE')
    def density_inversion_test(self, record_set: ocproc2.RecordSet, context: TestContext):
        if len(record_set.records) < 2:
            self.skip_test()
        previous_density = self._calculate_density(record_set.records[0], context)
        for i in range(1, len(record_set.records)):
            with context.subrecord_from_current_set_context(i) as ctx:
                current_density = self._calculate_density(record_set.records[i], ctx)
                if current_density is None:
                    continue
                with ctx.two_parameter_context('Temperature', 'PracticalSalinity'):
                    self.assert_greater_than('density_inversion_detected', current_density, previous_density)
                previous_density = current_density

    def _calculate_density(self, record: ocproc2.DataRecord, context: TestContext) -> t.Optional[float]:
        psal = self.value_in_units(record.parameters.get('PracticalSalinity'), '0.001')
        if psal is None:
            return None
        temp = self.value_in_units(record.parameters.get('Temperature'), '°C', temp_scale='ITS-90')
        if temp is None:
            return None
        pressure_dbar = self.calculate_pressure_in_dbar(
            record.coordinates.get('Pressure'),
            record.coordinates.get('Depth'),
            context.top_record.coordinates.get('Latitude')
        )
        if pressure_dbar is None:
            return None
        return eos80_density_at_depth_t90(psal, temp, pressure_dbar)
