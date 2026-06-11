import typing as t

from medsutil import seawater
from medsutil.seawater import TemperatureScale
from pipeman.programs.qc.base import DeepDiveChecker, RecordRef, AnyRef, ElementRef, review, QCSkipReview, \
    SingleElementRef

import medsutil.ocproc2 as ocproc2
import medsutil.ocproc_math as omath
import medsutil.math as amath


class GTSPPFreezingPointTest(DeepDiveChecker):

    TRACK_COORDINATES = True

    def __init__(self, **kwargs):
        super().__init__('gtspp_freezing', '1.0', test_tags=['GTSPP_2.6'])

    def record_check(self, ref: RecordRef):
        freezing_points = [x for x in self.get_freezing_points(ref)]
        # need at least one salinity to check against
        if not freezing_points:
            return
        temp_ref = self.get_record_parameter_ref(ref, "Temperature")
        if temp_ref is not None:
            for temp_sref in self.iterate_on_single_elements(temp_ref):
                self.freezing_point_test(
                    temp_sref,
                    freezing_points
                )

    def get_freezing_points(self, ref: RecordRef) -> t.Iterable[amath.AnyNumber]:
        # in degrees_C
        sal_ref = self.get_record_parameter_ref(ref, "PracticalSalinity")
        lat = self.current_latitude
        pressure = self.current_pressure
        for sal_sref in self.extract_good_values(sal_ref):
            fp = omath.get_freezing_point_from_psal(
                sal_sref.element,
                pressure_dbar=pressure,
                latitude_dd=lat,
                units="degrees_C",
                temperature_scale=TemperatureScale.TS_1990
            )
            if fp is not None:
                yield fp

    @review("above_freezing", fail_flag=3, skip_dubious=True, pass_flag=1)
    def freezing_point_test(self, ref: SingleElementRef, freezing_points: list[amath.AnyNumber]):
        temp = omath.get_temperature(
            temperature=ref.element,
            temperature_scale=TemperatureScale.TS_1990,
            units="degrees_C",
            obs_date=self.current_time
        )
        for fp in freezing_points:
            if amath.
        self.assert_greater_or_close(temp,)

    def _test_freezing_point(self, v: ocproc2.AbstractElement, ctx: TestContext, fp: float):
        self.should_test_value(v)

        if temp > 0:
            return
        self.assert_greater_than('fp_temp_too_low', temp, fp, qc_flag=13)

