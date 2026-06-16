from uncertainties import UFloat, ufloat

from medsutil.bathymetry import BathymetryModel
import medsutil.ocproc2 as ocproc2
from medsutil.dynamic import dynamic_object
from medsutil.ocproc2.refs import RecordRef, SingleElementRef, ParentRecordRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import DeepDiveChecker
import medsutil.math as amath


class GTSPPBathymetryTest(DeepDiveChecker):

    def __init__(self,
                 bathymetry_model_class: str,
                 bathymetry_model_kwargs: dict = None,
                 run_on_land_test: bool = True,
                 run_sounding_test: bool = True,
                 run_bottom_test: bool = True):
        super().__init__(
            test_name='gtspp_bathy_check',
            test_version='1.0',
            test_tags=[
                'GTSPP_1.4' if run_on_land_test else None,
                'GTSPP_1.6' if run_sounding_test else None,
                'GTSPP_2.11' if run_bottom_test else None
            ]
        )
        if bathymetry_model_kwargs:
            self._bathymetry_model: BathymetryModel = dynamic_object(bathymetry_model_class)(**bathymetry_model_kwargs)
        else:
            self._bathymetry_model: BathymetryModel = dynamic_object(bathymetry_model_class)()
        self.run_on_land_test = run_on_land_test
        self.run_sounding_test = run_sounding_test
        self.run_bottom_test = run_bottom_test

    def parent_record_check(self, ref: ParentRecordRef):
        if self.run_bottom_test or self.run_sounding_test or self.run_on_land_test:
            self.add_note(f"Bathymetry checked against: {self._bathymetry_model.ref_name}")

    def record_check(self, ref: RecordRef):
        if self.run_on_land_test:
            lats = ref.coordinate_ref("Latitude")
            lons = ref.coordinate_ref("Longitude")
            # TODO: load station and check if on_land check is needed
            if lats is not None and lons is not None:
                for lat, lon in self.extract_keyed_parameters(lats, lons):
                    if lat is not None and lon is not None:
                        with self.review_all("position_check", [lat, lon], pass_flag=1, fail_flag=4) as ctx:
                            ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                            lat_value = lat.element.to_numeric("degrees_north")
                            lon_value = lon.element.to_numeric("degrees_east")
                            max_depth = self._get_max_valid_depth_meters(lat_value, lon_value)
                            if max_depth is None:
                                self.skip_review("invalid_bathymetry_location")
                            else:
                                self.assert_less_or_close(max_depth, 0, msg="on_land")

        if self.run_bottom_test or self.run_sounding_test:
            self._update_coordinates(ref)
            lat, lon = self.current_latitude, self.current_longitude
            if lat is not None and lon is not None:
                max_valid_depth = self._get_max_valid_depth_meters(lat, lon)
                if max_valid_depth is not None:
                    depths = ref.coordinate_ref("Depth")
                    if depths is not None:
                        for depth in depths.single_element_refs():
                            with self.review("sounding_check", depth, pass_flag=1, fail_flag=4) as ctx:
                                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                                depth_value = depth.element.to_numeric("m")
                                self.assert_less_or_close(depth_value, max_valid_depth, msg="too_deep")

    def _get_max_valid_depth_meters(self, lat: amath.AnyNumber, lon: amath.AnyNumber) -> amath.AnyNumber | None:
        return self._bathymetry_model.water_depth(lat, lon)
