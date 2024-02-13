from uncertainties import UFloat, ufloat

from cnodc.bathymetry import BathymetryModel
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext
import cnodc.ocproc2.structures as ocproc2
from cnodc.util import dynamic_object


class GTSPPBathymetryTest(BaseTestSuite):

    def __init__(self,
                 bathymetry_model_class: str,
                 bathymetry_model_kwargs: dict,
                 minimum_relative_uncertainty: float = 0.1,
                 run_on_land_test: bool = True,
                 run_sounding_test: bool = True,
                 run_bottom_test: bool = True,
                 **kwargs):
        super().__init__(
            'gtspp_bathy_check',
            '1.0',
            test_tags=[
                'GTSPP_1.4' if run_on_land_test else None,
                'GTSPP_1.6' if run_sounding_test else None,
                'GTSPP_2.11' if run_bottom_test else None
            ],
            **kwargs)
        bathymetry_model_kwargs = bathymetry_model_kwargs or {}
        self._min_uncertainty = minimum_relative_uncertainty
        self._bathymetry_model: BathymetryModel = dynamic_object(bathymetry_model_class)(**bathymetry_model_kwargs)
        self.run_on_land_test = run_on_land_test
        self.run_sounding_test = run_sounding_test
        self.run_bottom_test = run_bottom_test

    @RecordTest()
    def check_position(self, record: ocproc2.DataRecord, context: TestContext):
        self.precheck_value_in_map(record.coordinates, 'Latitude', allow_dubious=True)
        self.precheck_value_in_map(record.coordinates, 'Longitude', allow_dubious=True)
        station = self.load_station(context)
        if station is not None and station.get_metadata('skip_bathymetry_check', False):
            return
        x = record.coordinates['Longitude'].to_float_with_uncertainty()
        y = record.coordinates['Latitude'].to_float_with_uncertainty()
        z = self._bathymetry_model.water_depth(x, y)
        if z is None:
            self.record_note(f'Bathymetry [{self._bathymetry_model.ref_name}] does not support coordinates ({x}, {y})', context)
            return
        self.record_note(f"Bathymetry [{self._bathymetry_model.ref_name}] reports water depth at ({x}, {y}) to be {z} m", context)
        # This is the most conservative check possible (almost certainly above sea level)
        if self.run_on_land_test:
            with context.two_coordinate_context('Latitude', 'Longitude') as ctx:
                self.assert_less_than('position_above_sea_level', z, 1e-6)
        elif self.run_bottom_test or self.run_sounding_test:
            # When checking the sounding, we allow values up to 10% deeper to allow for
            # tidal and other effects. However, if the uncertainty is larger than 10% of
            # the value, then we use the uncertainty instead
            if isinstance(z, UFloat):
                if z.std_dev < self._min_uncertainty * abs(z.nominal_value):
                    z = ufloat(z.nominal_value, self._min_uncertainty * abs(z.nominal_value))
            else:
                z = ufloat(z, self._min_uncertainty * abs(z))
            self._check_for_soundings(record, context, z)

    def _check_for_soundings(self, record: ocproc2.DataRecord, context, z: UFloat):
        if self.run_sounding_test and 'SeaDepth' in record.parameters:
            with context.parameter_context('SeaDepth') as ctx:
                self.test_all_subvalues(ctx, self._check_sounding, z=z)
        if self.run_bottom_test and 'Depth' in record.coordinates:
            with context.coordinate_context('Depth') as ctx:
                self.test_all_subvalues(ctx, self._check_depth, z=z)
        self.test_all_subrecords_without_coordinates(context, self._check_for_soundings, z=z)

    def _check_sounding(self, v: ocproc2.Value, ctx: TestContext, z: UFloat):
        self.precheck_value(v)
        self.assert_close_to('sounding_bathymetry_mismatch', self.value_in_units(v, 'm'), z)

    def _check_depth(self, v: ocproc2.Value, ctx: TestContext, z: UFloat):
        self.precheck_value(v)
        self.assert_greater_than('depth_too_deep', self.value_in_units(v), z)