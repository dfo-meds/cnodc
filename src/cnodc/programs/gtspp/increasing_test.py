import itertools
import pathlib
import typing as t

import yaml
from uncertainties import UFloat
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext, ProfileTest, SubRecordArray, ProfileLevelTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.units import UnitConverter
from cnodc.ocean_math.seawater import eos80_pressure, eos80_freezing_point_t90, eos80_density_at_depth_t90


class GTSPPIncreasingProfileTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_increasing', '1_0', test_tags=['GTSPP_2.3'], **kwargs)

    @ProfileTest()
    def increasing_depth_test(self, profile: SubRecordArray, ctx: TestContext):
        check_depth = 'Depth' in profile.data
        check_pressure = 'Pressure' in profile.data
        if not check_depth or check_pressure:
            self.skip_test()
        previous_depth = None
        previous_pressure = None
        previous_depth_units = None
        previous_pressure_units = None
        for i, row_ctx in profile.iterate_rows(ctx):
            if i == 0:
                continue
            with row_ctx.self_context():
                if check_depth:
                    with row_ctx.coordinate_context('Depth'):
                        profile.require_good_value('Depth', i, False)
                        current_depth = profile.data['Depth'][i].to_float()
                        if current_depth is not None:
                            current_depth_units = profile.data['Depth'][i].metadata.best_value('Units', None)
                            if previous_depth is not None:
                                if current_depth_units is not None and previous_depth_units is not None and current_depth_units != previous_depth_units:
                                    previous_depth = self.converter.convert(previous_depth, previous_depth_units, current_depth_units)
                                self.assert_greater_than('decreasing_depth', previous_depth, current_depth)
                            previous_depth = current_depth
                            previous_depth_units = current_depth_units
                if check_pressure:
                    with row_ctx.coordinate_context('Pressure'):
                        profile.require_good_value('Pressure', i, False)
                        current_pressure = profile.data['Pressure'][i].to_float()
                        if current_pressure is not None:
                            current_pressure_units = profile.data['Pressure'][i].metadata.best_value('Units', None)
                            if previous_pressure is not None:
                                if current_pressure_units is not None and previous_pressure_units is not None and current_pressure_units != previous_pressure_units:
                                    previous_pressure = self.converter.convert(previous_pressure, previous_pressure_units, current_pressure_units)
                                self.assert_greater_than('decreasing_pressure', previous_pressure, current_pressure)
                            previous_pressure = current_pressure
                            previous_pressure_units = current_pressure_units
