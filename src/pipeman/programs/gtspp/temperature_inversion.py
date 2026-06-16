import typing as t

import medsutil.math as amath
import medsutil.ocproc_math as omath
from medsutil.ocproc2.refs import ChildRecordRef, RecordSetRef, SingleElementRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import ProfileChecker


class GTSPPTemperatureInversionTest(ProfileChecker):

    TRACK_COORDINATES = True

    def __init__(self):
        super().__init__(
            test_name='gtspp_temp_inversion',
            test_version='1.0',
            test_tags=['GTSPP_2.12']
        )
        self._maxima_threshold = amath.NumberString("0.1")
        self._minima_threshold = amath.NumberString("-0.1")
        self._min_depth = amath.NumberString("75")
        self._min_temperature = amath.NumberString("4")
        self._depth_gap = amath.NumberString("50")

    def profile_check(self, profile: list[ChildRecordRef], recordset_ref: RecordSetRef):
        if len(profile) < 4:
            self.skip_review("four or more records required")
        for temperatures in self.extract_all_keyed_parameters(*profile, include_parameters=("Temperature",)):
            self._inversion_check(temperatures, profile)

    def _inversion_check(self, temperature_refs: tuple[SingleElementRef | None, ...], profile: list[ChildRecordRef]):
        with self.review_all("inversion_check", [x for x in temperature_refs if x is not None], pass_flag=1, fail_flag=3):
            minima: list[amath.AnyNumber] = []
            maxima: list[amath.AnyNumber] = []
            for idx in range(2, len(temperature_refs)):
                if temperature_refs[idx - 2] is None or temperature_refs[idx - 1] is None or temperature_refs[idx] is None:
                    continue
                temp1, temp2, temp3 = self.extract_parameter_values(temperature_refs[idx-2], temperature_refs[idx-1], temperature_refs[idx], units="degrees_C")
                if temp1 is None or temp2 is None or temp3 is None:
                    continue
                self._update_coordinates(profile[idx-1])
                if self.current_depth is None:
                    continue
                self._inversion_check_at_level(temp1, temp2, temp3, t.cast(amath.AnyNumber, self.current_depth), minima, maxima)

    def _inversion_check_at_level(self,
                                  temp1: amath.AnyNumber,
                                  temp2: amath.AnyNumber,
                                  temp3: amath.AnyNumber,
                                  current_depth: amath.AnyNumber,
                                  minima: list[amath.AnyNumber],
                                  maxima: list[amath.AnyNumber]):
        average_temp_13 = amath.div(amath.add(temp1, temp3), 2)
        temp_2_diff = amath.sub(temp2, average_temp_13)

        if amath.gt(temp_2_diff, self._maxima_threshold):
            maxima.append(current_depth)
            for minimum in minima:
                if amath.gt(minimum, current_depth) and amath.lt(abs(amath.sub(minimum, current_depth)), self._depth_gap):
                    self.report_qc_error("invalid_temperature_inversion")


        if amath.lt(temp_2_diff, self._minima_threshold):
            minima.append(current_depth)
            for maximum in maxima:
                if amath.gt(maximum, current_depth) and amath.lt(abs(amath.sub(maximum, current_depth)), self._depth_gap):
                    self.report_qc_error("invalid_temperature_inversion")
