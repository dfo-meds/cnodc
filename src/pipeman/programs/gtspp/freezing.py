import typing as t

from medsutil import seawater
from medsutil.ocproc2.util import RequiredQuality, Quality
from medsutil.seawater import TemperatureScale
from pipeman.programs.qc.base import DeepDiveChecker, review, QCSkipReview, \
    QualityController
from medsutil.ocproc2.refs import AnyRef, ElementRef, SingleElementRef, RecordRef

import medsutil.ocproc2 as ocproc2
import medsutil.ocproc_math as omath
import medsutil.math as amath


class GTSPPFreezingPointTest(DeepDiveChecker):

    TRACK_COORDINATES = True

    def __init__(self, aggressive_mode: bool = False):
        super().__init__('gtspp_freezing', '1.0', test_tags=['GTSPP_2.6'])
        self._aggressive_mode = aggressive_mode

    def record_check(self, ref: RecordRef):
        temp_ref = ref.parameter_ref("Temperature")
        if temp_ref is not None:
            freezing_points = [x for x in self.get_freezing_points(ref)]
            if freezing_points:
                freezing_point = max(freezing_points) if self._aggressive_mode else min(freezing_points)
                for temp_sref in temp_ref.single_element_refs():
                    self.freezing_point_test(
                        temp_sref,
                        freezing_point
                    )

    def get_freezing_points(self, ref: RecordRef) -> t.Iterable[amath.AnyNumber]:
        # in degrees_C
        sal_ref = ref.parameter_ref("PracticalSalinity")
        lat = self.current_latitude
        pressure = self.current_pressure
        for sal_sref in self.extract_good_values(sal_ref, required_quality=RequiredQuality.GOOD_VALUE_WITH_UNITS):
            fp = omath.get_freezing_point_from_psal(
                sal_sref.element,
                pressure_dbar=pressure,
                latitude_dd=lat,
                units="degrees_C",
                temperature_scale=TemperatureScale.TS_1990
            )
            if fp is not None:
                yield fp

    @review("above_freezing", fail_flag=Quality.DUBIOUS, pass_flag=Quality.GOOD, required_quality=RequiredQuality.GOOD_VALUE_WITH_UNITS)
    def freezing_point_test(self, ref: SingleElementRef, freezing_point: amath.AnyNumber):
        temp = omath.get_temperature(
            temperature=ref.element,
            obs_date=self.current_time,
            units="degrees_C",
            temperature_scale=TemperatureScale.TS_1990
        )
        self.assert_greater_or_close(t.cast(amath.AnyNumber, temp), freezing_point)
