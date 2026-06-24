from uncertainties import UFloat, ufloat

from medsutil.bathymetry import BathymetryModel
import medsutil.ocproc2 as ocproc2
from medsutil.dynamic import dynamic_object
from medsutil.ocproc2.refs import RecordRef, SingleElementRef, ParentRecordRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import DeepDiveChecker
import medsutil.math as amath


class GTSPPBathymetryCheck(DeepDiveChecker):

    def __init__(self,
                 bathymetry_model_class: str,
                 bathymetry_model_kwargs: dict = None,
                 absolute_bottom_tolerance: float = 50,
                 run_on_land_test: bool = True,
                 run_sounding_test: bool = True,
                 run_bottom_test: bool = True,
                 searcher_cls: type | None = None):
        super().__init__(
            test_name='gtspp_bathy_check',
            test_version='1.0',
            test_tags=[
                'GTSPP_1.4' if run_on_land_test else None,
                'GTSPP_1.6' if run_sounding_test else None,
                'GTSPP_2.11' if run_bottom_test else None
            ],
            searcher_cls=searcher_cls
        )
        if bathymetry_model_kwargs:
            self._bathymetry_model: BathymetryModel = dynamic_object(bathymetry_model_class)(**bathymetry_model_kwargs)
        else:
            self._bathymetry_model: BathymetryModel = dynamic_object(bathymetry_model_class)()
        self.run_on_land_test = run_on_land_test
        self.run_sounding_test = run_sounding_test
        self.run_bottom_test = run_bottom_test
        self._absolute_bottom_tolerance = absolute_bottom_tolerance

    def parent_record_check(self, ref: ParentRecordRef):
        if self.run_bottom_test or self.run_sounding_test or self.run_on_land_test:
            self.add_note(f"Bathymetry checked against: {self._bathymetry_model.ref_name}")

    def check_should_skip(self) -> bool:
        if 'should_skip' not in self.record_memory:
            self.record_memory['should_skip'] = False
            pid = self.current_record.record.metadata.best("CNODCPlatform", coerce=str, default=None)
            if pid is not None:
                platform = self.searcher.find_by_uuid(pid)
                if platform is not None:
                    if platform.skip_on_land_check:
                        self.record_memory['should_skip'] = True
        return self.record_memory['should_skip']

    def record_check(self, ref: RecordRef):
        if self.run_on_land_test and not self.check_should_skip():
            lats = ref.coordinate_ref("Latitude")
            lons = ref.coordinate_ref("Longitude")
            if lats is not None and lons is not None:
                for lat, lon in self.group_by_sensor_rank(lats, lons):
                    if lat is not None and lon is not None:
                        with self.review_all("position_check", [lat, lon], pass_flag=1, fail_flag=4) as ctx:
                            ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                            self.check_not_on_land(lat.element, lon.element)
        if self.run_bottom_test or self.run_sounding_test:
            self._update_coordinates(ref)

    def single_element_check(self, ref: SingleElementRef):

        lat, lon = self.current_latitude, self.current_longitude
        if lat is None or lon is None: return

        # require valid element name
        if self.run_bottom_test and ref.element_name == "Depth":
            with self.review("depth_check", ref, pass_flag=1, fail_flag=3) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                self.check_not_too_deep(ref.element, lat, lon)
        elif self.run_sounding_test and ref.element_name == "SeaDepth":
            with self.review("sea_depth_check", ref, pass_flag=1, fail_flag=3) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                self.check_not_too_deep(ref.element, lat, lon)

    def check_not_too_deep(self, element: ocproc2.SingleElement, lat: amath.AnyNumber, lon: amath.AnyNumber):
        max_depth_m = self._bathymetry_model.water_depth(lat, lon)
        if max_depth_m is None:
            self.skip_review("invalid_bathymetry_location")
        else:
            depth_value = element.to_numeric("m")
            self.assert_less_or_close(depth_value, amath.add(max_depth_m, self._absolute_bottom_tolerance), msg="too_deep")

    def check_not_on_land(self, lat: ocproc2.SingleElement, lon: ocproc2.SingleElement):
        lat_value = lat.to_numeric("degrees_north")
        lon_value = lon.to_numeric("degrees_east")
        max_depth = self._bathymetry_model.water_depth(lat_value, lon_value)
        if max_depth is None:
            self.skip_review("invalid_bathymetry_location")
        else:
            self.assert_less_or_close(max_depth, 0, msg="is_on_land")

