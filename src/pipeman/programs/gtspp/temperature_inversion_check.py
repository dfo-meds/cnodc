import typing as t

import medsutil.math as amath
import medsutil.ocproc_math as omath
from medsutil.ocproc2.refs import ChildRecordRef
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

    def profile_check(self, profile: list[ChildRecordRef]):
        if len(profile) < 4:
            self.skip_review("four or more records required")
        self.profile_memory['inversion_test_failed'] = set()
        super().profile_check(profile)

    def level_check(self, ref: ChildRecordRef):
        self.assert_is_not_none(self.current_depth, msg="no_valid_depth")
        current_depth = t.cast(amath.AnyNumber, self.current_depth)
        self.assert_less(current_depth, self._min_depth, msg="depth_too_shallow")

        temp_ref = ref.parameter_ref("Temperature")
        self.assert_is_not_none(temp_ref, msg="missing_temperature")

        if not self.profile_memory["inversion_test_failed"]:
            for temp_single in self.extract_good_values(temp_ref, RequiredQuality.GOOD_VALUE_WITH_UNITS):
                temp = omath.get_temperature(
                    temperature=temp_single.element,
                    obs_date=self.current_time,
                )
                if temp is None:
                    continue
                if amath.lte(temp, self._min_temperature):
                    continue
                self._temp_inversion_precheck(
                    current_depth,
                    temp_single.element.metadata.best("SensorRank", coerce=int, default=0),
                    temp
                )

        result = self.profile_memory["inversion_test_failed"]

        # test if True or False so we properly count the good tests, just skip if None
        for temp_single in temp_ref.single_element_refs():
            sensor_rank = temp_single.element.metadata.best("SensorRank", default=0, coerce=int)
            if sensor_rank not in result:
                continue
            with self.review("bad_inversion", temp_single, pass_flag=1, fail_flag=3) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.NOT_DUBIOUS | RequiredQuality.HAS_VALUE)
                if self.profile_memory["inversion_test_failed"][sensor_rank]:
                    self.report_qc_error("inversion_test_failed")

    def _temp_inversion_precheck(self, depth: amath.AnyNumber, instrument_rank: int, temperature: amath.AnyNumber):
        if instrument_rank not in self.profile_memory:
            self.profile_memory[instrument_rank] = pmem = {}
        else:
            pmem = self.profile_memory[instrument_rank]

        one_ago = pmem.get("one_ago", None)
        if one_ago is None:
            pmem["one_ago"] = (depth, temperature)
            return

        two_ago = pmem.get("two_ago", None)
        if two_ago is not None:
            if pmem["inversion_test_failed"] is None:
                pmem["inversion_test_failed"] = False
            self._temp_inversion_check(
                pmem,
                instrument_rank,
                t.cast(tuple[amath.AnyNumber, amath.AnyNumber], two_ago),
                t.cast(tuple[amath.AnyNumber, amath.AnyNumber], one_ago),
                (depth, temperature)
            )

        pmem["two_ago"] = one_ago
        pmem["one_ago"] = (depth, temperature)

    def _temp_inversion_check(self,
                              pmem: dict,
                              instrument_rank: int,
                              depth_temp_1: tuple[amath.AnyNumber, amath.AnyNumber],
                              depth_temp_2: tuple[amath.AnyNumber, amath.AnyNumber],
                              depth_temp_3: tuple[amath.AnyNumber, amath.AnyNumber]):
        _, temp1 = depth_temp_1[1]
        element2, temp2 = depth_temp_2[1]
        _, temp3 = depth_temp_3[1]
        current_depth = depth_temp_2[0]

        average_temp_13 = (temp1 + temp2) / 2
        temp_2_diff = temp2 - average_temp_13

        if amath.gt(temp_2_diff, self._maxima_threshold):
            if 'maxima' not in pmem:
                pmem['maxima'] = []
            pmem['maxima'].append(current_depth)

            if 'minima' in pmem:
                for minima in pmem['minima']:
                    if amath.gt(minima, current_depth) and amath.lt(abs(amath.sub(minima, current_depth)), self._depth_gap):
                        self.profile_memory["inversion_test_failed"].add(instrument_rank)


        if amath.lt(temp_2_diff, self._minima_threshold):
            if 'minima' not in pmem:
                pmem['minima'] = []
            pmem['minima'].append(depth_temp_2[0])

            if 'maxima' in pmem:
                for maxima in pmem['maxima']:
                    if amath.gt(maxima, current_depth) and amath.lt(abs(amath.sub(maxima, current_depth)), self._depth_gap):
                        self.profile_memory["inversion_test_failed"].add(instrument_rank)
