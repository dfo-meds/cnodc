import pathlib
import typing as t
from cnodc.qc.base import BaseTestSuite, TestContext, ProfileTest, SubRecordArray, ProfileLevelTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.ocean_math.seawater import eos80_pressure, eos80_freezing_point_t90, eos80_density_at_depth_t90


class GTSPPDensityInversionTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_density', '1_0', test_tags=['GTSPP_2.10'], **kwargs)

    @ProfileTest()
    def density_inversion_test(self, profile: SubRecordArray, context: TestContext):
        if profile.length < 2:
            self.skip_test()
        if 'Temperature' not in profile.data or 'PracticalSalinity' not in profile.data:
            self.skip_test()
        if 'Depth' not in profile.data and 'Pressure' not in profile.data:
            self.skip_test()
        previous_density = self._calculate_density(profile, 0, context)
        for i in range(1, profile.length):
            density_at_level = self._calculate_density(profile, i, context)
            if density_at_level is None:
                continue
            if previous_density is not None:
                with context.subrecord_from_current_set_context(i) as ctx2:
                    if not self.is_greater_than(density_at_level, previous_density):
                        ctx2.current_record.parameters['Temperature'].metadata['WorkingQuality'] = 13
                        ctx2.current_record.parameters['PracticalSalinity'].metadata['WorkingQuality'] = 13
                        self.report_for_review('density_inversion_detected', ref_value=(density_at_level, previous_density))
            previous_density = density_at_level

    def _calculate_density(self, profile: SubRecordArray, level: int, context: TestContext) -> t.Optional[float]:
        psal = self.value_in_units(profile.data['PracticalSalinity'][level], '0.001')
        if psal is None:
            return None
        temp = self.value_in_units(profile.data['Temperature'][level], '°C', temp_scale='ITS-90')
        if temp is None:
            return None
        pressure_dbar = self.calculate_pressure_in_dbar(
            profile.get_data('Pressure', level),
            profile.get_data('Depth', level),
            context.top_record.coordinates['Latitude']
        )
        if pressure_dbar is None:
            return None
        return eos80_density_at_depth_t90(psal, temp, pressure_dbar)
