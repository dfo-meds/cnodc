from uncertainties import UFloat, ufloat

from cnodc.bathymetry import BathymetryModel
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext
import cnodc.ocproc2.structures as ocproc2
from cnodc.util import dynamic_object


class NODBBathymetryCheck(BaseTestSuite):

    def __init__(self, bathymetry_model_class: str, bathymetry_model_kwargs: dict, **kwargs):
        super().__init__('nodb_bathy_check', '1.0', **kwargs)
        bathymetry_model_kwargs = bathymetry_model_kwargs or {}
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
        # This is the most conservative check possible (almost certainly above sea level)
        self.record_note(f"Bathymetry [{self._bathymetry_model.ref_name}] reports water depth at ({x}, {y}) to be {z} m", context)
        if not self.is_less_than(z, 1e-6):
            record.coordinates['Latitude'].metadata['WorkingQuality'] = 14
            record.coordinates['Longitude'].metadata['WorkingQuality'] = 14
            context.report_for_review('position_above_sea_level', ref_value=str(z))
        else:
            # When checking the sounding, we allow values up to 10% deeper to allow for
            # tidal and other effects. However, if the uncertainty is larger than 10% of
            # the value, then we use the uncertainty instead
            if isinstance(z, UFloat):
                if z.std_dev < 0.1 * abs(z.nominal_value):
                    z = ufloat(z.nominal_value, 0.1 * abs(z.nominal_value))
            else:
                z = ufloat(z, 0.1 * abs(z))
            self._check_for_soundings(record, z, context)

    def _check_for_soundings(self, record: ocproc2.DataRecord, z: UFloat, context):
        base_path = context.current_path
        if 'SeaDepth' in record.parameters:
            context.current_path = [*base_path, 'SeaDepth']
            for av in record.parameters['SeaDepth'].all_values():
                self._check_sounding(av, z, context)
        if 'Depth' in record.coordinates:
            context.current_path = [*base_path, 'Depth']
            self._check_depth(record.coordinates['Depth'], z, context)
        for srt in record.subrecords:
            for srs_idx in record.subrecords[srt]:
                for sr_idx, sr in enumerate(record.subrecords[srt][srs_idx].records):
                    # These records will eventually be checked by the RecordTest() above and can be skipped.
                    if sr.coordinates.has_value('Latitude') or sr.coordinates.has_value('Longitude'):
                        continue
                    context.current_path = [*base_path, f'{srt}/{srs_idx}/{sr_idx}']
                    self._check_for_soundings(sr, z, context)

    def _check_sounding(self, v: ocproc2.Value, z: UFloat, context):
        if not self.is_close(v, z, "m"):
            v.metadata['WorkingQuality'] = 14
            context.report_for_review('sounding_bathymetry_mismatch', ref_value=str(z))

    def _check_depth(self, v: ocproc2.Value, z: UFloat, context):
        if not self.is_greater_than(v, z, "m"):
            v.metadata['WorkingQuality'] = 14
            context.report_for_review('depth_too_deep', ref_value=str(z))
