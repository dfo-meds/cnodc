import itertools
import pathlib
import typing as t

import yaml
from uncertainties import UFloat
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext, ProfileTest, SubRecordArray, ProfileLevelTest
import cnodc.ocproc2.structures as ocproc2
from cnodc.units import UnitConverter
from cnodc.ocean_math.seawater import eos80_pressure, eos80_freezing_point_t90, eos80_density_at_depth_t90


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


class GTSPPEnvelopeTest(BaseTestSuite):

    def __init__(self,
                 envelope_file: t.Union[str, pathlib.Path],
                 **kwargs):
        super().__init__('gtspp_envelope', '1_0', test_tags=['GTSPP_2.4'], **kwargs)
        self._envelope_ref = EnvelopeReference(envelope_file, self.converter)

    @ProfileLevelTest("Depth")
    def envelope_test(self, profile: SubRecordArray, current_level: int, context: TestContext):
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
        kwargs = reference['kwargs'] if 'kwargs' in reference else {}
        if 'minimum' in reference:
            self.assert_greater_than('envelope_too_low', v, reference['minimum'], units, **kwargs)
        elif 'maximum' in reference:
            self.assert_less_than('envelope_too_high', v, reference['maximum'], units, **kwargs)
