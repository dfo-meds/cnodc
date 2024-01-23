from uncertainties import UFloat, ufloat

from cnodc.bathymetry import BathymetryModel
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext
import cnodc.ocproc2.structures as ocproc2
from cnodc.util import dynamic_object


class NODBBathymetryCheck(BaseTestSuite):

    def __init__(self,
                 bathymetry_model_class: str,
                 bathymetry_model_kwargs: dict,
                 minimum_relative_uncertainty: float = 0.1,
                 **kwargs):
        super().__init__('nodb_bathy_check', '1.0', **kwargs)
        bathymetry_model_kwargs = bathymetry_model_kwargs or {}
        self._min_uncertainty = minimum_relative_uncertainty
        self._bathymetry_model = dynamic_object(bathymetry_model_class)(**bathymetry_model_kwargs)

    @RecordTest()
    def check_position(self, record: ocproc2.DataRecord, context: TestContext):
        self.require_good_value(record.coordinates, 'Latitude', True)
        self.require_good_value(record.coordinates, 'Longitude', True)
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
        if not self.is_less_than(z, 1e-6):
            record.coordinates['Latitude'].metadata['WorkingQuality'] = 14
            record.coordinates['Longitude'].metadata['WorkingQuality'] = 14
            context.report_for_review('position_above_sea_level', ref_value=str(z))
        else:
            # When checking the sounding, we allow values up to 10% deeper to allow for
            # tidal and other effects. However, if the uncertainty is larger than 10% of
            # the value, then we use the uncertainty instead
            if isinstance(z, UFloat):
                if z.std_dev < self._min_uncertainty * abs(z.nominal_value):
                    z = ufloat(z.nominal_value, self._min_uncertainty * abs(z.nominal_value))
            else:
                z = ufloat(z, self._min_uncertainty * abs(z))
            self._check_for_soundings(record, z, context)

    def _check_for_soundings(self, record: ocproc2.DataRecord, z: UFloat, context):
        if 'SeaDepth' in record.parameters:
            self.test_all_subvalues(record.parameters['SeaDepth'], context, self._check_sounding, z=z)
        if 'Depth' in record.coordinates:
            self.test_all_subvalues(record.coordinates['Depth'], context, self._check_depth, z=z)
        for sr, sr_ctx in self.iterate_on_subrecords(record, context):
            if sr.coordinates.has_value('Latitude') or sr.coordinates.has_value('Longitude'):
                continue
            with sr_ctx.self_context():
                self._check_for_soundings(sr, z, sr_ctx)

    def _check_sounding(self, v: ocproc2.Value, ctx: TestContext, z: UFloat):
        self.assert_close_to('sounding_bathymetry_mismatch', v, z, "m")

    def _check_depth(self, v: ocproc2.Value, ctx: TestContext, z: UFloat):
        self.assert_greater_than('depth_too_deep', v, z, "m")
