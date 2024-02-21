import pathlib
import typing as t
import yaml
from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest
import cnodc.ocproc2 as ocproc2
import cnodc.ocean_math.umath_wrapper as umath


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


class GTSPPSpikeGradientTest(BaseTestSuite):

    def __init__(self,
                 spike_file: t.Union[str, pathlib.Path],
                 run_spike_test: bool = True,
                 run_spike_extrema_test: bool = True,
                 run_gradient_test: bool = True,
                 **kwargs):
        super().__init__('gtspp_spike', '1_0', test_tags=[
            'GTSPP_2.7' if run_spike_test else None,
            'GTSPP_2.8' if run_spike_extrema_test else None,
            'GTSPP_2.9' if run_gradient_test else None,
        ], **kwargs)
        self._spike_ref = SpikeReference(spike_file)
        self.run_spike_test = run_spike_test
        self.run_spike_extrema_test = run_spike_extrema_test
        self.run_gradient_test = run_gradient_test

    @RecordSetTest('PROFILE')
    def _spike_test(self, recordset: ocproc2.RecordSet, context: TestContext):
        if len(recordset.records) >= 2:
            if self.run_spike_extrema_test:
                with context.subrecord_from_current_set_context(0) as ctx_top:
                    self._run_top_spike_test(recordset, ctx_top)
                with context.subrecord_from_current_set_context(len(recordset.records) - 1) as ctx_bottom:
                    self._run_bottom_spike_test(recordset, ctx_bottom)
        if len(recordset.records) >= 3 and (self.run_spike_test or self.run_gradient_test):
            self._run_at_level_spike_tests(recordset, context)

    def check_has_depth_coordinate(self, record: ocproc2.BaseRecord, raise_ex: bool = True) -> bool:
        if self.precheck_value_in_map(record.coordinates, 'Pressure', raise_ex=False):
            return True
        return self.precheck_value_in_map(record.coordinates, 'Depth', raise_ex=raise_ex)

    def _run_top_spike_test(self, recordset: ocproc2.RecordSet, context: TestContext):
        self.check_has_depth_coordinate(recordset.records[0])
        self.check_has_depth_coordinate(recordset.records[1])
        test_parameters = {
            pname: self._spike_ref.get_top_thresholds(pname)
            for pname in self._spike_ref.spike_top_parameters()
        }
        if not test_parameters:
            self.skip_test()
        v1 = self._extract_spike_test_values(recordset, 0, {}, test_parameters)
        v2 = self._extract_spike_test_values(recordset, 1, {}, test_parameters)
        for pname in test_parameters:
            with context.parameter_context(pname) as ctx:
                if isinstance(v1, list):
                    for idx in range(0, len(v1)):
                        with ctx.multivalue_context(idx) as ctx4:
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
        if all(umath.is_greater_than(cv, test_ranges[1]) or umath.is_less_than(cv, test_ranges[0]) for cv in check_values):
            self.report_for_review('spike_extrema_test_failed', qc_flag=13, ref_value=(check_values, test_ranges[0], test_ranges[1]))

    def _run_bottom_spike_test(self, recordset: ocproc2.RecordSet, context: TestContext):
        self.check_has_depth_coordinate(recordset.records[-1])
        self.check_has_depth_coordinate(recordset.records[-2])
        test_parameters = {
            pname: self._spike_ref.get_bottom_thresholds(pname)
            for pname in self._spike_ref.spike_bottom_parameters()
        }
        if not test_parameters:
            self.skip_test()
        v2 = self._extract_spike_test_values(recordset, -1, {}, test_parameters)
        v1 = self._extract_spike_test_values(recordset, -2, {}, test_parameters)
        for pname in test_parameters:
            with context.parameter_context(pname) as ctx3:
                if isinstance(v2, list):
                    for idx in range(0, len(v2)):
                        with ctx3.multivalue_context(idx) as ctx4:
                            self._run_extrema_spike_test_for_parameter(v2[pname][idx], v1[pname], test_parameters[pname])
                else:
                    self._run_extrema_spike_test_for_parameter(v2[pname], v1[pname], test_parameters[pname])

    def _run_at_level_spike_tests(self, recordset: ocproc2.RecordSet, context: TestContext):
        test_parameters = {
            pname: self._spike_ref.get_spike_threshold(pname)
            for pname in self._spike_ref.spike_parameters()
        }
        if not test_parameters:
            self.skip_test()
        ref = {}
        for i in range(1, len(recordset.records) - 1):
            with context.subrecord_from_current_set_context(i) as ctx2:
                self._run_spike_test_for_level(recordset, i, ref, test_parameters, ctx2)

    def _run_spike_test_for_level(self, recordset: ocproc2.RecordSet, current_level: int, ref: dict, test_parameters: dict, ctx2: TestContext):
        v1 = self._extract_spike_test_values(recordset, current_level - 1, ref, test_parameters)
        v2 = self._extract_spike_test_values(recordset, current_level, ref, test_parameters)
        v3 = self._extract_spike_test_values(recordset, current_level + 1, ref, test_parameters)
        for pname in test_parameters:
            with ctx2.parameter_context(pname) as pctx:
                if isinstance(v2[pname], list):
                    for i in range(0, len(v2[pname])):
                        with pctx.multivalue_context(i) as pctx2:
                            self._run_spike_test_for_parameter(v1[pname], v2[pname][i], v3[pname], test_parameters[pname])
                else:
                    self._run_spike_test_for_parameter(v1[pname], v2[pname], v3[pname], test_parameters[pname])

    def _run_spike_test_for_parameter(self, v1: t.Union[umath.FLOAT, list[umath.FLOAT], None], v2: umath.FLOAT, v3: t.Union[umath.FLOAT, list[umath.FLOAT], None], thresholds: tuple):
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
        if self.run_spike_test and thresholds[0] is not None and all(umath.is_greater_than(cv, thresholds[0]) for cv in check_spikes):
            self.report_for_review('spike_test_failed', qc_flag=13, ref_value=(check_spikes, thresholds[0]))
        if self.run_gradient_test and thresholds[1] is not None and all(umath.is_greater_than(cv, thresholds[1]) for cv in check_gradients):
            self.report_for_review('gradient_test_failed', qc_flag=13, ref_value=(check_gradients, thresholds[1]))

    def _extract_spike_test_values(self, recordset: ocproc2.RecordSet, target_level: int, ref: dict, test_parameters: dict) -> dict[str, t.Union[float, list[float]]]:
        if target_level not in ref:
            if not self.check_has_depth_coordinate(recordset.records[target_level], False):
                ref[target_level] = None
            else:
                ref[target_level] = {
                    pname: self._get_spike_test_values(recordset, target_level, pname, test_parameters[pname][-2], **test_parameters[pname][-1])
                    for pname in test_parameters
                }
        if ref[target_level] is None:
            self.skip_test()
        return ref[target_level]

    def _get_spike_test_values(self, recordset: ocproc2.RecordSet, target_level: int, parameter_name: str, *args, **kwargs) -> t.Union[float, list[float], None]:
        record = recordset.records[target_level]
        if parameter_name in record.coordinates:
            return self.all_values_in_units(record.coordinates[parameter_name], *args, **kwargs) or None
        elif parameter_name in record.metadata:
            return self.all_values_in_units(record.metadata[parameter_name], *args, **kwargs) or None
        elif parameter_name in record.parameters:
            return self.all_values_in_units(record.parameters[parameter_name], *args, **kwargs) or None
        return None
