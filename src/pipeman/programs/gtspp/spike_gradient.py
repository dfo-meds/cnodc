import itertools
import pathlib
import typing as t
import yaml

import medsutil.ocproc2 as ocproc2
import medsutil.math as amath
from medsutil.ocproc2.refs import ChildRecordRef, ElementRef, SingleElementRef, MultiElementRef, RecordSetRef
from medsutil.ocproc2.util import high_quality_depth_pressure, RequiredQuality
from pipeman.programs.qc.base import ProfileChecker
import medsutil.ocproc_math as omath


class SpikeReference:

    def __init__(self, file: str | pathlib.Path):
        self.spike_levels = {}
        with open(file, "r") as h:
            self.spike_levels = yaml.safe_load(h) or {}
            # TODO: validation

    def spike_parameters(self) -> t.Iterable[str]:
        yield from (x for x in self.spike_levels.keys() if 'spike' in self.spike_levels[x] or 'gradient' in self.spike_levels[x])

    def spike_top_parameters(self) -> t.Iterable[str]:
        yield from (x for x in self.spike_levels.keys() if 'top_vdown' in self.spike_levels[x] and 'top_vup' in self.spike_levels[x])

    def spike_bottom_parameters(self) -> t.Iterable[str]:
        yield from (x for x in self.spike_levels.keys() if 'bottom_vdown' in self.spike_levels[x] and 'bottom_vup' in self.spike_levels[x])

    def spike_top_info(self) -> dict[str, tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]]:
        return {
            param: self.get_top_thresholds(param)
            for param in self.spike_top_parameters()
        }

    def spike_info(self) -> dict[str, tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]]:
        return {
            param: self.get_spike_thresholds(param)
            for param in self.spike_parameters()
        }

    def spike_bottom_info(self) -> dict[str, tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]]:
        return {
            param: self.get_bottom_thresholds(param)
            for param in self.spike_bottom_parameters()
        }

    def get_spike_thresholds(self, parameter_name: str) -> tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]:
        if parameter_name in self.spike_levels and ('spike' in self.spike_levels[parameter_name] or 'gradient' in self.spike_levels):
            return (
                amath.NumberString(self.spike_levels[parameter_name]['spike']) if 'spike' in self.spike_levels[parameter_name] else None,
                amath.NumberString(self.spike_levels[parameter_name]['gradient']) if 'gradient' in self.spike_levels[parameter_name] else None,
                str(self.spike_levels[parameter_name]['units']),
            )
        return None, None, None

    def get_top_thresholds(self, parameter_name: str) -> tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]:
        if parameter_name in self.spike_levels and 'top_vdown' in self.spike_levels[parameter_name] and 'top_vup' in self.spike_levels[parameter_name]:
            return (
                amath.NumberString(self.spike_levels[parameter_name]['top_vdown']),
                amath.NumberString(self.spike_levels[parameter_name]['top_vup']),
                str(self.spike_levels[parameter_name]['units']),
            )
        return None, None, None

    def get_bottom_thresholds(self, parameter_name: str) -> tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]:
        if parameter_name in self.spike_levels and 'bottom_vdown' in self.spike_levels[parameter_name] and 'bottom_vup' in self.spike_levels[parameter_name]:
            return (
                amath.NumberString(self.spike_levels[parameter_name]['bottom_vdown']),
                amath.NumberString(self.spike_levels[parameter_name]['bottom_vup']),
                str(self.spike_levels[parameter_name]['units']),
            )
        return None, None, None


class GTSPPSpikeGradientTest(ProfileChecker):

    def __init__(self,
                 spike_file: t.Union[str, pathlib.Path],
                 run_spike_test: bool = True,
                 run_spike_extrema_test: bool = True,
                 run_gradient_test: bool = True):
        super().__init__(
            test_name='gtspp_spike',
            test_version='1.0',
            test_tags=[
                'GTSPP_2.7' if run_spike_test else None,
                'GTSPP_2.8' if run_spike_extrema_test else None,
                'GTSPP_2.9' if run_gradient_test else None,
            ]
        )
        self._spike_ref = SpikeReference(spike_file)
        self._run_spike_test = run_spike_test
        self._run_spike_extrema_test = run_spike_extrema_test
        self._run_gradient_test = run_gradient_test

    def profile_check(self, profile: list[ChildRecordRef], recordset_ref: RecordSetRef):
        if len(profile) < 2:
            return
        if self._run_spike_extrema_test:
            with self.skip_review_blocker():
                self._top_spike_test(profile[0], profile[1])
            with self.skip_review_blocker():
                self._bottom_spike_test(profile[-2], profile[-1])
        if len(profile) > 2 and (self._run_spike_test or self._run_gradient_test):
            parameters = self._spike_ref.spike_info()
            if parameters:
                for idx in range(1, len(profile) - 1):
                    with self.skip_review_blocker():
                        self._middle_spike_test(profile[idx-1], profile[idx], profile[idx+1], parameters)

    def depth(self, record: ocproc2.ChildRecord):
        depth, _ = high_quality_depth_pressure(
            record.coordinates.get("Pressure", None),
            record.coordinates.get("Depth", None),
            self.current_latitude
        )
        return depth

    def _top_spike_test(self, top: ChildRecordRef, second_top: ChildRecordRef):
        parameters = self._spike_ref.spike_top_info()
        if not parameters:
            self.skip_review("no_parameters")
        self._extrema_spike_check(top, second_top, parameters)

    def _bottom_spike_test(self, second_bottom: ChildRecordRef, bottom: ChildRecordRef):
        parameters = self._spike_ref.spike_bottom_info()
        if not parameters:
            self.skip_review("no_parameters")
        self._extrema_spike_check(second_bottom, bottom, parameters, True)

    def _extrema_spike_check(self, v1: ChildRecordRef, v2: ChildRecordRef, parameters: dict[str, tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]], review_v2: bool = False):
        for v1_ref, v2_ref in self.extract_all_keyed_parameters(v1, v2, include_parameters=parameters.keys()):
            if v1_ref is None and v2_ref is None:
                continue
            pname = v1_ref.element_name if v1_ref is not None else v2_ref.element_name
            self._extrema_spike_check_for_single_parameter(v1_ref, v2_ref, *parameters[pname], review_v2=review_v2)

    def _extrema_spike_check_for_single_parameter(self,
                                                  v1_ref: SingleElementRef | None,
                                                  v2_ref: SingleElementRef | None,
                                                  min_value: amath.AnyNumber | None,
                                                  max_value: amath.AnyNumber | None,
                                                  units: str | None,
                                                  review_v2: bool = False):
        review_ref = v2_ref if review_v2 else v1_ref
        if review_ref is None:
            return
        with self.review("extrema_spike", review_ref, pass_flag=1, fail_flag=3) as ctx:
            ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.NOT_DUBIOUS)
            if v1_ref is None or v2_ref is None:
                self.skip_review("no_paired_ref")
                return
            if min_value is None and max_value is None:
                self.skip_review("no_min_or_max")
                return
            v1, v2 = self.extract_parameter_values(v1_ref, v2_ref, units=units)
            if v1 is None or v2 is None:
                self.skip_review("no_paired_value")
                return
            diff = amath.sub(v1, v2)
            if max_value is not None:
                self.assert_less_or_close(diff, max_value)
            if min_value is not None:
                self.assert_greater_or_close(diff, min_value)

    def _middle_spike_test(self, previous: ChildRecordRef, current: ChildRecordRef, next_: ChildRecordRef, parameters: dict[str, tuple[amath.AnyNumber | None, amath.AnyNumber | None, str | None]]) -> None:
        for previous_ref, current_ref, next_ref in self.extract_all_keyed_parameters(previous, current, next_, include_parameters=parameters.keys()):
            if current_ref is None:
                continue
            self._spike_gradient_check_for_single_parameter(previous_ref, current_ref, next_ref, *parameters[current_ref.element_name])

    def _spike_gradient_check_for_single_parameter(self,
                                                   previous: SingleElementRef | None,
                                                   current: SingleElementRef,
                                                   next_: SingleElementRef | None,
                                                   spike_threshold: amath.AnyNumber | None,
                                                   gradient_threshold: amath.AnyNumber | None,
                                                   units: str | None):

        with self.review("spike_gradient_check", current, pass_flag=1, fail_flag=3) as ctx:
            ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.NOT_DUBIOUS)
            if previous is None or next_ is None:
                self.skip_review("missing_before_or_after_value")
                return
            v1, v2, v3 = self.extract_parameter_values(previous, current, next_, units=units)
            if v1 is None or v3 is None or v2 is None:
                self.skip_review("missing_nearby_values")
                return
            gradient_value = abs(amath.sub(v2, amath.div(amath.add(v3, v1), 2)))
            if self._run_gradient_test and gradient_threshold is not None:
                self.assert_less_or_close(gradient_value, gradient_threshold, msg="gradient_threshold_exceeded")
            if self._run_spike_test and spike_threshold is not None:
                spike_value = amath.sub(gradient_value, abs(amath.div(amath.sub(v1, v3), 2)))
                self.assert_less_or_close(spike_value, spike_threshold, msg="spike_threshold_exceeded")
