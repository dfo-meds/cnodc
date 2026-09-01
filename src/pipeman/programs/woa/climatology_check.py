import enum
import typing as t

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2.refs import ElementType, SingleElementRef
from medsutil.ocproc2.util import Quality
from pipeman.programs.qc.base import DeepDiveChecker
from pipeman.programs.woa.grid import WorldOceanAtlasOneDegree


class WOATimeResolution(enum.Enum):
    ANNUAL = "annual"
    SEASONAL = "seasonal"
    MONTHLY = "monthly"


class WorldOceanAtlasClimatologyCheck(DeepDiveChecker):

    LIMIT_ELEMENT_TYPES = ElementType.PARAMETERS
    TRACK_COORDINATES = True

    ELEMENT_TO_VAR = {
        "Temperature": "temp",
        "PracticalSalinity": "salinity",
    }

    def __init__(self,
                 temporal_resolution: WOATimeResolution,
                 std_dev_range: int = 3,
                 **kwargs):
        super().__init__(**kwargs)
        self._std_dev_range = std_dev_range
        self._resolution = temporal_resolution
        self._grids = {}

    def get_grid(self, var: str, time_period: str) -> WorldOceanAtlasOneDegree:
        key = (var, time_period)
        if key not in self._grids:
            self._grids[key] = WorldOceanAtlasOneDegree.build_from_file(var, time_period)
        return self._grids[key]

    def get_atlas(self, element_name: str, time: AwareDateTime | None) -> WorldOceanAtlasOneDegree | None:
        if time is None:
            return None
        if element_name not in self.ELEMENT_TO_VAR:
            return None
        if self._resolution is WOATimeResolution.ANNUAL:
            time_period = "annual"
        elif self._resolution is WOATimeResolution.SEASONAL:
            month = time.month
            if 1 <= month <= 3:
                time_period = "winter"
            elif 4 <= month <= 6:
                time_period = "spring"
            elif 7 <= month <= 9:
                time_period = "summer"
            else:
                time_period = "fall"
        else:
            time_period = time.strftime("%b").lower()
        return self.get_grid(self.ELEMENT_TO_VAR[element_name], time_period)

    def single_element_check(self, ref: SingleElementRef):
        with self.review("woa_climatology_check", ref, fail_flag=Quality.DUBIOUS) as ctx:
            ctx.check_review_already_complete()
            if self.current_latitude is None or self.current_longitude is None or self.current_depth is None or self.current_time is None:
                self.skip_review("uncertain_position")
            self._atlas_check(ref, t.cast(float, self.current_longitude), t.cast(float, self.current_latitude), t.cast(float, self.current_depth))

    def _atlas_check(self, ref: SingleElementRef, lon: float, lat: float, depth: float):
        if not ref.element.is_numeric():
            self.skip_review("non_numeric")
        if not ref.element.units():
            self.skip_review("no_units")
        atlas = self.get_atlas(ref.element_name, self.current_time)
        if atlas is None:
            self.skip_review("no_atlas")
        ref_value = atlas.get_value(lon, lat, depth)
        if ref_value is None:
            self.skip_review("no_ref_value")
        self.assert_compatible(ref.element.to_scinum(), ref_value, k=self._std_dev_range, msg="not_in_woa_reference_range")








