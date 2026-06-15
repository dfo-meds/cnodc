from medsutil.ocproc2.util import Quality, RequiredQuality
from pipeman.programs.qc.base import ProfileChecker
from medsutil.ocproc2.refs import ChildRecordRef, ElementRef
import medsutil.math as amath


class GTSPPIncreasingChecker(ProfileChecker):

    def __init__(self):
        super().__init__(
            test_name='gtspp_increasing',
            test_version='1.0',
            test_tags=['GTSPP_2.3']
        )

    def level_check(self, ref: ChildRecordRef):
        depth = ref.coordinate_ref("Depth")
        if depth is not None:
            self._increasing_check(depth)
        pressure = ref.coordinate_ref("Pressure")
        if pressure is not None:
            self._increasing_check(pressure)

    def _increasing_check(self, ref: ElementRef):
        for single_element in ref.single_element_refs():
            sensor_rank = single_element.element.metadata.best("SensorRank", coerce=int, default=0)
            last_value: amath.AnyNumber | None = None
            units: str | None = None
            if sensor_rank in self.profile_memory:
                if ref.element_name in self.profile_memory[sensor_rank]:
                    last_value, units = self.profile_memory[sensor_rank][ref.element_name]
            else:
                self.profile_memory[sensor_rank] = {}
            with self.review("is_deeper", single_element, fail_flag=Quality.ERRONEOUS, pass_flag=Quality.GOOD) as ctx:
                self.require_quality(single_element.element, RequiredQuality.GOOD_OR_DUBIOUS_VALUE | RequiredQuality.HAS_UNITS)

                if units is None:
                    units = single_element.element.metadata.best("Units", coerce=str)
                level_value = single_element.element.to_numeric(units)
                self.profile_memory[sensor_rank][ref.element_name] = (level_value, units)

                ctx.check_review_already_complete()

                if last_value is not None:
                    self.assert_greater_or_close(level_value, last_value, msg="not_deeper")
