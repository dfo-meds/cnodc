from medsutil.ocproc2 import ChildRecord
from medsutil.ocproc2.refs import ChildRecordRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import ProfileChecker
import medsutil.ocproc2 as ocproc2
import medsutil.ocproc_math as omath


class GTSPPDensityInversionTest(ProfileChecker):

    def __init__(self):
        super().__init__(
            test_name='gtspp_density',
            test_verison='1.0',
            test_tags=['GTSPP_2.10']
        )

    def profile_check(self, profile: list[ChildRecordRef]):
        if len(profile) < 2:
            return
        self.profile_memory['last'] = {}
        super().profile_check(profile)

    def level_check(self, record: ChildRecordRef):
        temp_ref = record.parameter_ref("Temperature")
        salinity_ref = record.parameter_ref("Salinity")
        if temp_ref is None or salinity_ref is None:
            return
        for temp_sref in temp_ref.single_element_refs():
            t_sensor_rank = temp_sref.element.metadata.best("SensorRank", coerce=int, default=0)
            for psal_sref in salinity_ref.single_element_refs():
                p_sensor_rank = temp_sref.element.metadata.best("SensorRank", coerce=int, default=0)
                key = (t_sensor_rank, p_sensor_rank)
                with self.review_all("density_inversion", [temp_sref, psal_sref]) as ctx:
                    self.require_quality(temp_sref.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)
                    self.require_quality(psal_sref.element, RequiredQuality.GOOD_VALUE_WITH_UNITS)
                    density = omath.get_density(temp_sref.element, psal_sref.element, self.current_pressure, self.current_depth, self.current_latitude, self.current_time)
                    if density is None:
                        self.skip_review("no density")
                    elif key not in self.profile_memory['last']:
                        self.profile_memory['last'][key] = density
                        self.skip_review("first level")
                    else:
                        ctx.check_review_already_complete()
                        self.assert_greater_or_close(
                            density, self.profile_memory['last'][key],
                        )
