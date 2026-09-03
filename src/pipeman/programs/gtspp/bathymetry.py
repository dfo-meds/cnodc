import medsutil.ocproc2 as ocproc2

from medsutil.fastgrid import FastGeoGrid
from medsutil.math import ScienceNumber
from medsutil.ocproc2.refs import RecordRef, SingleElementRef
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.bathymetry.glb import GreatLakesBathymetry

from pipeman.programs.bathymetry.base import BathymetryModel
from pipeman.programs.qc.base import DeepDiveChecker
import medsutil.math as amath

from zirconium import ApplicationConfig
from autoinject import injector
import typing as t
import pathlib

class GTSPPBathymetryCheck(DeepDiveChecker):

    config: ApplicationConfig = None

    @injector.construct
    def __init__(self,
                 absolute_bottom_tolerance: float = 50,
                 run_on_land_test: bool = True,
                 run_sounding_test: bool = True,
                 run_bottom_test: bool = True,
                 searcher_cls: type | None = None):
        super().__init__(
            test_protocol='gtspp',
            test_name='bathymetry_check',
            test_version='1.0',
            test_tags=[
                'GTSPP_1.4' if run_on_land_test else None,
                'GTSPP_1.6' if run_sounding_test else None,
                'GTSPP_2.11' if run_bottom_test else None
            ],
            searcher_cls=searcher_cls
        )
        self._bathymetry_models: list[tuple[str, BathymetryModel]] = [
            ("ncei_great_lakes", GreatLakesBathymetry(t.cast(pathlib.Path, self.config.as_path("references", "great_lakes_bathymetry")))),
            # TODO: GEBCO
        ]
        self.run_on_land_test = run_on_land_test
        self.run_sounding_test = run_sounding_test
        self.run_bottom_test = run_bottom_test
        self._absolute_bottom_tolerance = absolute_bottom_tolerance

    def check_should_skip_on_land(self) -> bool:
        if 'should_skip_on_land' not in self.record_memory:
            self.record_memory['should_skip_on_land'] = False
            pid = self.current_record.record.metadata.best("CNODCPlatform", coerce=str, default=None)
            if pid is not None:
                platform = self.searcher.load_platform(pid)
                if platform is not None and platform.skip_on_land_check:
                    self.record_memory['should_skip_on_land'] = True
        return self.record_memory['should_skip_on_land']

    def record_check(self, ref: RecordRef):
        if self.run_on_land_test and not self.check_should_skip_on_land():
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

    def get_water_depth(self, lat: float, lon: float) -> ScienceNumber | None:
        for bathy_model_name, bathy_model in self._bathymetry_models:
            water_depth = bathy_model.get_bathymetry(lon, lat)
            if water_depth is not None:
                if "bathy_models" not in self.record_memory:
                    self.record_memory["bathy_models"] = set()
                if bathy_model_name not in self.record_memory["bathy_models"]:
                    self.record_memory["bathy_models"].add(bathy_model_name)
                    self.add_note(f"Bathymetry model used: {bathy_model_name} at x={lon} y={lat}")
                return water_depth
        return None

    def single_element_check(self, ref: SingleElementRef):

        lat, lon = self.current_latitude, self.current_longitude
        if lat is None or lon is None: return

        water_depth = self.get_water_depth(float(lat), float(lon))

        # require valid element name
        if self.run_bottom_test and ref.element_name == "Depth":
            with self.review("depth_check", ref, pass_flag=1, fail_flag=3) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                self.check_not_too_deep(ref.element, water_depth)

        elif self.run_sounding_test and ref.element_name == "SeaDepth":
            with self.review("sea_depth_check", ref, pass_flag=1, fail_flag=3) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.GOOD_NUMERIC | RequiredQuality.HAS_UNITS)
                self.check_not_too_deep(ref.element, water_depth)

    def check_not_too_deep(self, element: ocproc2.SingleElement, max_depth_m: ScienceNumber | None):
        if max_depth_m is None:
            self.skip_review("invalid_bathymetry_location")
        else:
            depth_value = element.to_scinum().convert("m")
            self.assert_less_or_close(depth_value, amath.add(max_depth_m, self._absolute_bottom_tolerance), msg="too_deep")

    def check_not_on_land(self, lat: ocproc2.SingleElement, lon: ocproc2.SingleElement):
        lat_value = lat.to_numeric("degrees_north")
        lon_value = lon.to_numeric("degrees_east")
        max_depth = self.get_water_depth(lat_value, lon_value)
        if max_depth is None:
            self.skip_review("invalid_bathymetry_location")
        else:
            self.assert_less_or_close(max_depth, 0, msg="is_on_land")

