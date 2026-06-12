import typing as t

import medsutil.math as amath
import medsutil.ocproc2 as ocproc2
import medsutil.ocproc_math as omath
from medsutil.ocproc2.refs import ChildRecordRef, SingleElementRef
from medsutil.ocproc2.util import RequiredQuality, Quality
from pipeman.programs.qc.base import ProfileChecker


DEPTH_TEMPERATURES = tuple[amath.AnyNumber, list[amath.AnyNumber]]


class GTSPPTemperatureInversionTest(ProfileChecker):

    TRACK_COORDINATES = True

    def __init__(self):
        super().__init__(
            test_name='gtspp_temp_inversion',
            test_version='1.0',
            test_tags=['GTSPP_2.12']
        )
        self._maxima_threshold = 0.1
        self._minima_threshold = -0.1
        self._min_depth = 75
        self._min_temperature = 4
        self._depth_gap = 50

    def profile_check(self, profile: list[ChildRecordRef]):
        if len(profile) < 4:
            self.skip_review("four or more records required")
        self.profile_memory['inversion_test_failed'] = None
        super().profile_check(profile)

    def level_check(self, ref: ChildRecordRef):
        if self.current_depth is None:
            self.skip_review("no_valid_depth")
        if amath.lt(self.current_depth, self._min_depth):
            self.skip_review("depth_too_shallow")

        temp_ref = ref.parameter_ref("Temperature")
        if temp_ref is None:
            self.skip_review("missing_temperature")

        if not self.profile_memory["inversion_test_failed"]:
            level_temps = []
            for temp_single in self.extract_good_values(temp_ref, RequiredQuality.GOOD_VALUE_WITH_UNITS):
                temp = omath.get_temperature(
                    temperature=temp_single.element,
                    obs_date=self.current_time,
                )
                if temp is not None:
                    level_temps.append(temp)

            if not level_temps:
                self.skip_review("no_valid_temperatures")
            elif any(amath.lt(temp, self._min_temperature) for temp in level_temps):
                self.skip_review("temperature_too_cold")
            else:
                self._temp_inversion_precheck((t.cast(amath.AnyNumber, self.current_depth), level_temps))

        result = self.profile_memory["inversion_test_failed"]

        # test if True or False so we properly count the good tests, just skip if None
        if result is not None:
            for temp_single in temp_ref.single_element_refs():
                with self.review("bad_inversion", temp_single, pass_flag=1, fail_flag=3) as ctx:
                    ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.NOT_DUBIOUS | RequiredQuality.HAS_VALUE)
                    if self.profile_memory["inversion_test_failed"]:
                        self.report_qc_error("inversion_test_failed")

    def _temp_inversion_precheck(self, current: DEPTH_TEMPERATURES):
        pmem = self.profile_memory

        one_ago = pmem.get("one_ago", None)
        if one_ago is None:
            pmem["one_ago"] = current
            return

        two_ago = pmem.get("two_ago", None)
        if two_ago is not None:
            if pmem["inversion_test_failed"] is None:
                pmem["inversion_test_failed"] = False
            self._temp_inversion_check(
                t.cast(DEPTH_TEMPERATURES, two_ago),
                t.cast(DEPTH_TEMPERATURES, one_ago),
                current
            )

        pmem["two_ago"] = one_ago
        pmem["one_ago"] = current

    def _temp_inversion_check(self,
                              depth_temp_1: DEPTH_TEMPERATURES,
                              depth_temp_2: DEPTH_TEMPERATURES,
                              depth_temp_3: DEPTH_TEMPERATURES):
        for index in range(0, min(len(depth_temp_1[1]), len(depth_temp_2[1]), len(depth_temp_3[1]))):
            _, temp1 = depth_temp_1[1][index]
            element2, temp2 = depth_temp_2[1][index]
            _, temp3 = depth_temp_3[1][index]
            current_depth = depth_temp_2[0]

            average_temp_13 = amath.div(amath.add(temp1, temp2), 2)
            temp_2_diff = amath.sub(temp2, average_temp_13)

            if amath.gt(temp_2_diff, self._maxima_threshold) and not amath.is_close(temp_2_diff, self._maxima_threshold):
                pmem = self.profile_memory
                if 'maxima' not in pmem:
                    pmem['maxima'] = []
                pmem['maxima'].append(current_depth)

                if 'minima' in pmem:
                    for minima in pmem['minima']:
                        if amath.gt(minima, current_depth) and amath.lt(abs(minima - current_depth), self._depth_gap):
                            pmem['inversion_test_failed'] = True


            if amath.lt(temp_2_diff, self._minima_threshold) and not amath.is_close(temp_2_diff, self._minima_threshold):
                pmem = self.profile_memory
                if 'minima' not in pmem:
                    pmem['minima'] = []
                pmem['minima'].append(depth_temp_2[0])

                if 'maxima' in pmem:
                    for maxima in pmem['maxima']:
                        if amath.gt(maxima, current_depth) and amath.lt(abs(maxima - current_depth), self._depth_gap):
                            pmem['inversion_test_failed'] = True
