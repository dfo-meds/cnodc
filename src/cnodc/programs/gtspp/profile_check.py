import itertools
import pathlib
import typing as t

import yaml
from uncertainties import UFloat
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext, ProfileTest, SubRecordArray, ProfileLevelTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.units import UnitConverter
from cnodc.ocean_math.seawater import eos80_pressure, eos80_freezing_point, eos80_convert_temperature


class EnvelopeReference:

    def __init__(self, file: t.Union[str, pathlib.Path], converter: UnitConverter):
        self._envelope = []
        self._converter = converter
        with open(file, 'r') as h:
            self._envelope = yaml.safe_load(h) or []
        # TODO: validation

    def find_level(self, depth: ocproc2.AbstractValue) -> dict:
        if depth.is_empty():
            return {}
        depth_units = depth.metadata.best_value('Units', None)
        if depth_units is not None and depth_units != 'm':
            depth = self._converter.convert(depth.to_float(), depth_units, 'm')
        depth_units = 'm'
        for level in self._envelope:
            if 'Depth' not in level:
                continue
            envelope_depth_units = 'm' if 'units' not in level['Depth'] else level['Depth']['units']
            test_against = self._converter.convert(depth, depth_units, envelope_depth_units) if envelope_depth_units != depth_units else depth
            if level['Depth']['minimum'] < test_against <= level['Depth']['maximum']:
                # TODO: consider removing Depth/Pressure?
                return level
        return {}


class GTSPPProfileCheck(BaseTestSuite):

    def __init__(self, envelope_file: t.Union[str, pathlib.Path], **kwargs):
        super().__init__(**kwargs)
        self._envelope_ref = EnvelopeReference(envelope_file, self.converter)

    @ProfileTest()
    def increasing_depth_test(self, profile: SubRecordArray, ctx: TestContext):
        check_depth = 'Depth' in profile.data
        check_pressure = 'Pressure' in profile.data
        if not check_depth or check_pressure:
            return
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

    @ProfileLevelTest("Depth")
    def _envelope_test(self, profile: SubRecordArray, current_level: int, context: TestContext):
        profile.require_good_value('Depth', current_level, False)
        level_references = self._envelope_ref.find_level(profile.data['Depth'][current_level])
        for vname in level_references:
            if vname == 'Depth' or vname == 'Pressure':
                continue
            with context.parameter_context(vname) as v_ctx:
                profile.require_good_value(vname, current_level)
                self.test_all_subvalues(profile.data[vname][current_level], v_ctx, self._envelope_range_check, reference=level_references[vname])

    def _envelope_range_check(self, v: ocproc2.Value, ctx: TestContext, reference: dict):
        units = reference['units'] if 'units' in reference else None
        if 'minimum' in reference:
            self.assert_greater_than('envelope_too_low', v, reference['minimum'], units)
        elif 'maximum' in reference:
            self.assert_less_than('envelope_too_high', v, reference['maximum'], units)

    @ProfileLevelTest()
    def _freezing_point_test(self, profile: SubRecordArray, current_level: int, context: TestContext):
        profile.require_good_value('PracticalSalinity', current_level)
        profile.require_good_value('Temperature', current_level)
        temp = self.value_in_units(profile.data['Temperature'][current_level], '°C', temp_scale='ITS-90')
        # Water doesn't freeze at temperatures above 0 (on Earth at least), so skip the math
        if temp > 0:
            self.skip_test()
        psal = self.value_in_units(profile.data['PracticalSalinity'][current_level], '0.001')
        if psal < 26 or psal > 35:
            self.skip_test()
        pressure = self._get_pressure_dbar(profile, current_level, context)
        if pressure is None:
            self.skip_test()
        freezing_point = eos80_freezing_point(psal, pressure)
        with context.parameter_context('Temperature'):
            self.assert_greater_than('fp_temp_too_low', temp, freezing_point, qc_flag=13)

    def _get_pressure_dbar(self, profile: SubRecordArray, current_level: int, context: TestContext):
        # In Pascals
        if 'Pressure' in profile.data:
            if profile.has_good_value('Pressure', current_level, False):
                return self.value_in_units(profile.data['Pressure'][current_level], 'dbar')
        elif 'Depth' in profile.data:
            if context.top_record.coordinates.has_value('Latitude'):
                if profile.has_good_value('Depth', current_level, False):
                    lat = context.top_record.coordinates['Latitude'].to_float_with_uncertainty()
                    depth_m = self.value_in_units(profile.data['Depth'][current_level], 'm')
                    return eos80_pressure(depth_m, lat)
        return None
