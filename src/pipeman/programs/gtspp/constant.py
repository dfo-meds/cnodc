import itertools

from medsutil.ocproc2 import SingleElement
from medsutil.ocproc2.refs import ChildRecordRef, RecordSetRef, MultiElementRef, SingleElementRef, ElementType
import medsutil.math as amath
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import ProfileChecker


class GTSPPConstantTest(ProfileChecker):

    def __init__(self, **kwargs):
        super().__init__(
            test_name='gtspp_constant_check',
            test_version='1.0',
            test_tags=['GTSPP_2.5'],
            **kwargs
        )

    def profile_check(self, profile: list[ChildRecordRef], recordset_ref: RecordSetRef):
        digitization_method = recordset_ref.recordset.metadata.best("DigitizationMethod", coerce=str, default=None)
        if digitization_method == "selected_depths":
            self._constant_check_selected(profile)
        elif digitization_method == "inflection_points":
            self._constant_check_inflection(profile)
        else:
            self.skip_review("unknown_digitization_method")

    def _constant_check_inflection(self, profile: list[ChildRecordRef]):
        for idx in range(1, len(profile) - 1):
            for keyed_pairs in self.extract_all_keyed_parameters(profile[idx-1], profile[idx], profile[idx+1]):
                if any(x is None for x in keyed_pairs):
                    continue
                self._constant_check_inflection_single_parameter(*keyed_pairs)

    def _constant_check_inflection_single_parameter(self,
                                                    previous: SingleElementRef,
                                                    current: SingleElementRef,
                                                    next_: SingleElementRef):
        with self.review("constant_for_inflection_points", current, fail_flag=4, pass_flag=1) as ctx:
            ctx.check_review_already_complete()
            v1, v2, v3 = self.extract_parameter_values(previous, current, next_)
            if v1 is not None and v2 is not None and v3 is not None:
                if amath.is_close(v1, v2) and amath.is_close(v2, v3) and amath.is_close(v1, v3):
                    self.report_qc_error("constant_detected")
            else:
                self.skip_review("missing_nearby_value")

    def _constant_check_selected(self, profile: list[ChildRecordRef]):
        for all_parameters in self.extract_all_keyed_parameters(*profile):
            all_values = self.extract_parameter_values(*all_parameters)
            with self.review_all("constant_for_selected_levels", [x for x in all_parameters if x is not None], fail_flag=3, pass_flag=1):
                for i in range(1, len(all_values)):
                    if all_values[i-1] is None:
                        continue
                    if all_values[i] is None:
                        continue
                    self.assert_not_close(all_values[i-1], all_values[i], msg="constant_value_found")







