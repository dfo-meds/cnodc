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


class SpikeReference:

    def __init__(self, file: t.Union[str, pathlib.Path]):
        self.spike_levels = {}
        with open(file, "r") as h:
            self.spike_levels = yaml.safe_load(h) or {}
            # TODO: validation

    def spike_parameters(self) -> t.Iterable[str]:
        yield from (x for x in self.spike_levels.keys() if 'spike' in self.spike_levels[x] or 'gradient' in self.spike_levels[x])

    def spike_top_parameters(self):
        yield from (x for x in self.spike_levels.keys() if 'top_vdown' in self.spike_levels[x] and 'top_vup' in self.spike_levels[x])

    def spike_bottom_parameters(self):
        yield from (x for x in self.spike_levels.keys() if 'bottom_vdown' in self.spike_levels[x] and 'bottom_vup' in self.spike_levels[x])

    def get_spike_threshold(self, parameter_name: str) -> tuple[t.Optional[float], t.Optional[float], t.Optional[str], dict]:
        if parameter_name in self.spike_levels and ('spike' in self.spike_levels[parameter_name] or 'gradient' in self.spike_levels):
            return (
                float(self.spike_levels[parameter_name]['spike']) if 'spike' in self.spike_levels[parameter_name] else None,
                float(self.spike_levels[parameter_name]['gradient']) if 'gradient' in self.spike_levels[parameter_name] else None,
                str(self.spike_levels[parameter_name]['units']),
                self.spike_levels[parameter_name]['kwargs'] if 'kwargs' in self.spike_levels[parameter_name] else {}
            )
        return None, None, None, {}

    def get_top_thresholds(self, parameter_name: str) -> tuple[t.Optional[float], t.Optional[float], t.Optional[str], dict]:
        if parameter_name in self.spike_levels and 'top_vdown' in self.spike_levels[parameter_name] and 'top_vup' in self.spike_levels[parameter_name]:
            return (
                float(self.spike_levels[parameter_name]['top_vdown']),
                float(self.spike_levels[parameter_name]['top_vup']),
                str(self.spike_levels[parameter_name]['units']),
                self.spike_levels[parameter_name]['kwargs'] if 'kwargs' in self.spike_levels[parameter_name] else {}
            )
        return None, None, None, {}

    def get_bottom_thresholds(self, parameter_name: str) -> tuple[t.Optional[float], t.Optional[float], t.Optional[str], dict]:
        if parameter_name in self.spike_levels and 'bottom_vdown' in self.spike_levels[parameter_name] and 'bottom_vup' in self.spike_levels[parameter_name]:
            return (
                float(self.spike_levels[parameter_name]['bottom_vdown']),
                float(self.spike_levels[parameter_name]['bottom_vup']),
                str(self.spike_levels[parameter_name]['units']),
                self.spike_levels[parameter_name]['kwargs'] if 'kwargs' in self.spike_levels[parameter_name] else {}
            )
        return None, None, None, {}


class GTSPPProfileCheck(BaseTestSuite):

    def __init__(self,
                 envelope_file: t.Union[str, pathlib.Path],
                 spike_file: t.Union[str, pathlib.Path],
                 **kwargs):
        super().__init__(**kwargs)
        self._envelope_ref = EnvelopeReference(envelope_file, self.converter)
        self._spike_ref = SpikeReference(spike_file)

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
        kwargs = reference['kwargs'] if 'kwargs' in reference else {}
        if 'minimum' in reference:
            self.assert_greater_than('envelope_too_low', v, reference['minimum'], units, **kwargs)
        elif 'maximum' in reference:
            self.assert_less_than('envelope_too_high', v, reference['maximum'], units, **kwargs)

    @ProfileLevelTest()
    def _freezing_point_test(self, profile: SubRecordArray, current_level: int, context: TestContext):
        profile.require_good_value('PracticalSalinity', current_level)
        profile.require_good_value('Temperature', current_level)
        # TODO: what if multiple PSAL values are provided? what should we do?
        psal = self.value_in_units(profile.data['PracticalSalinity'][current_level], '0.001')
        if psal is None or psal < 26 or psal > 35:
            self.skip_test()
        pressure = self._get_pressure_dbar(profile, current_level, context)
        if pressure is None:
            self.skip_test()
        freezing_point = eos80_freezing_point(psal, pressure)
        with context.parameter_context('Temperature') as ctx2:
            self.test_all_subvalues(profile.data['Temperature'][current_level], ctx2, self._test_freezing_point, fp=freezing_point)

    def _test_freezing_point(self, v: ocproc2.Value, ctx: TestContext, fp: float):
        temp = self.value_in_units(v, '°C', temp_scale='ITS-90')
        if temp > 0:
            return
        self.assert_greater_than('fp_temp_too_low', temp, fp, qc_flag=13)

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

    @ProfileTest()
    def _spike_test(self, profile: SubRecordArray, context: TestContext):
        if profile.length >= 2:
            with context.subrecord_from_current_set_context(0) as ctx_top:
                self._run_top_spike_test(profile, ctx_top)
            with context.subrecord_from_current_set_context(profile.length - 1) as ctx_bottom:
                self._run_bottom_spike_test(profile, ctx_bottom)
        if profile.length >= 3:
            self._run_at_level_spike_tests(profile, context)

    def _run_top_spike_test(self, profile: SubRecordArray, context: TestContext):
        if not (profile.has_good_value('Pressure', 0) or profile.has_good_value('Depth', 0)):
            self.skip_test()
        if not (profile.has_good_value('Pressure', 1) or profile.has_good_value('Depth', 1)):
            self.skip_test()
        test_parameters = {
            pname: self._spike_ref.get_top_thresholds(pname)
            for pname in self._spike_ref.spike_top_parameters()
            if pname in profile.data
        }
        if not test_parameters:
            self.skip_test()
        v1 = self._extract_spike_test_values(profile, 0, {}, test_parameters)
        v2 = self._extract_spike_test_values(profile, 1, {}, test_parameters)
        with context.subrecord_from_current_set_context(0) as ctx2:
            for pname in test_parameters:
                with ctx2.parameter_context(pname) as ctx3:
                    if isinstance(v1, list):
                        for idx in range(0, len(v1)):
                            with ctx3.multivalue_context(idx) as ctx4:
                                self._run_extrema_spike_test_for_parameter(v1[pname][idx], v2[pname], test_parameters[pname])
                    else:
                        self._run_extrema_spike_test_for_parameter(v1[pname], v2[pname], test_parameters[pname])

    def _run_extrema_spike_test_for_parameter(self,
                                              v1: t.Union[list[float], float],
                                              v2: t.Union[list[float], float],
                                              test_ranges: tuple):
        if v1 is None or v2 is None or test_ranges[0] is None or test_ranges[1] is None:
            return
        if isinstance(v1, list):
            if (not v1) or all(x is None for x in v1):
                return
        else:
            v1 = [v1]
        if isinstance(v2, list):
            if (not v2) or all(x is None for x in v2):
                return
        else:
            v2 = [v2]
        check_values = []
        for v1_ in v1:
            for v2_ in v2:
                check_values.append(v1_ - v2_)
        if all(self.is_greater_than(cv, test_ranges[1]) or self.is_less_than(cv, test_ranges[0]) for cv in check_values):
            self.report_for_review('spike_extrema_test_failed', qc_flag=13, ref_value=(check_values, test_ranges[0], test_ranges[1]))

    def _run_bottom_spike_test(self, profile: SubRecordArray, context: TestContext):
        if not (profile.has_good_value('Pressure', -1) or profile.has_good_value('Depth', -1)):
            self.skip_test()
        if not (profile.has_good_value('Pressure', -2) or profile.has_good_value('Depth', -2)):
            self.skip_test()
        test_parameters = {
            pname: self._spike_ref.get_bottom_thresholds(pname)
            for pname in self._spike_ref.spike_bottom_parameters()
            if pname in profile.data
        }
        if not test_parameters:
            self.skip_test()
        v2 = self._extract_spike_test_values(profile, -1, {}, test_parameters)
        v1 = self._extract_spike_test_values(profile, -2, {}, test_parameters)
        with context.subrecord_from_current_set_context(-1) as ctx2:
            for pname in test_parameters:
                with ctx2.parameter_context(pname) as ctx3:
                    if isinstance(v2, list):
                        for idx in range(0, len(v2)):
                            with ctx3.multivalue_context(idx) as ctx4:
                                self._run_extrema_spike_test_for_parameter(v2[pname][idx], v1[pname], test_parameters[pname])
                    else:
                        self._run_extrema_spike_test_for_parameter(v2[pname], v1[pname], test_parameters[pname])

    def _run_at_level_spike_tests(self, profile: SubRecordArray, context: TestContext):
        test_parameters = {
            pname: self._spike_ref.get_spike_threshold(pname)
            for pname in self._spike_ref.spike_parameters()
            if pname in profile.data
        }
        if not test_parameters:
            self.skip_test()
        ref = {}
        for i in range(1, profile.length - 1):
            with context.subrecord_from_current_set_context(i) as ctx2:
                self._run_spike_test_for_level(profile, i, ref, test_parameters, ctx2)

    def _run_spike_test_for_level(self, profile: SubRecordArray, current_level: int, ref: dict, test_parameters: dict, ctx2: TestContext):
        v1 = self._extract_spike_test_values(profile, current_level - 1, ref, test_parameters)
        v2 = self._extract_spike_test_values(profile, current_level, ref, test_parameters)
        v3 = self._extract_spike_test_values(profile, current_level + 1, ref, test_parameters)
        for pname in test_parameters:
            with ctx2.parameter_context(pname) as pctx:
                if isinstance(v2[pname], list):
                    for i in range(0, len(v2[pname])):
                        with pctx.multivalue_context(i) as pctx2:
                            self._run_spike_test_for_parameter(v1[pname], v2[pname][i], v3[pname], test_parameters[pname])
                else:
                    self._run_spike_test_for_parameter(v1[pname], v2[pname], v3[pname], test_parameters[pname])

    def _run_spike_test_for_parameter(self, v1: t.Union[float, list[float]], v2: float, v3: t.Union[float, list[float]], thresholds: tuple):
        if v2 is None or v1 is None or v3 is None or (thresholds[0] is None and thresholds[1] is None):
            self.skip_test()
        if isinstance(v1, list):
            if (not v1) or all(x is None for x in v1):
                self.skip_test()
        else:
            v1 = [v1]
        if isinstance(v3, list):
            if (not v3) or all (x is None for x in v3):
                self.skip_test()
        else:
            v3 = [v3]
        # Check all recorded temperatures above and below (if there are multiple,
        # otherwise this will generate only one result).
        check_spikes = []
        check_gradients = []
        for v1_ in v1:
            for v3_ in v3:
                gradient = abs(v2 - ((v3_ + v1_)/2))
                check_gradients.append(gradient)
                check_spikes.append(gradient - (abs(v1_ - v3_) / 2))
        if thresholds[0] is not None and all(self.is_greater_than(cv, thresholds[0]) for cv in check_spikes):
            self.report_for_review('spike_test_failed', qc_flag=13, ref_value=(check_spikes, thresholds[0]))
        if thresholds[1] is not None and all(self.is_greater_than(cv, thresholds[1]) for cv in check_gradients):
            self.report_for_review('gradient_test_failed', qc_flag=13, ref_value=(check_gradients, thresholds[1]))

    def _extract_spike_test_values(self, profile: SubRecordArray, target_level: int, ref: dict, test_parameters: dict) -> dict[str, t.Union[float, list[float]]]:
        if target_level not in ref:
            if profile.has_good_value('Depth', target_level) or profile.has_good_value('Pressure', target_level):
                ref[target_level] = {
                    pname: self._get_spike_test_values(profile, target_level, pname, test_parameters[pname][-2], **test_parameters[pname][-1])
                    for pname in test_parameters
                }
            else:
                ref[target_level] = None
        if ref[target_level] is None:
            self.skip_test()
        return ref[target_level]

    def _get_spike_test_values(self, profile: SubRecordArray, target_level: int, parameter_name: str, test_units: str, **kwargs) -> t.Union[float, list[float]]:
        if isinstance(profile.data[parameter_name][target_level], ocproc2.MultiValue):
            values = []
            for x in profile.data[parameter_name][target_level].all_values():
                values.append(self.value_in_units(x, test_units, null_dubious=True, null_erroneous=True, **kwargs))
            return values
        else:
            return self.value_in_units(profile.data[parameter_name][target_level], test_units, null_dubious=True, null_erroneous=True, **kwargs)
