import pathlib

import shapely
import zrlog
from uncertainties import UFloat

import cnodc.ocproc2.structures as ocproc2
import typing as t
import yaml
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest, QCTest
from cnodc.ocean_math.geodesy import upoint_to_geometry
from cnodc.units import UnitConverter


class NODBParameterReference:

    def __init__(self, config_file: pathlib.Path, converter: UnitConverter):
        self._converter = converter
        self._file_path = config_file
        self._config = {}
        self._log = zrlog.get_logger("cnodc.param_reference")
        if not self._file_path.exists():
            raise ValueError(f"Invalid configuration file: {self._file_path}")
        with open(self._file_path, "r") as h:
            self._config = yaml.safe_load(h) or {}
        if 'GLOBAL' not in self._config:
            self._log.warning(f"Parameter check missing GLOBAL section in f{self._file_path}")
            self._config['GLOBAL'] = {}
        elif not isinstance(self._config['GLOBAL'], dict):
            raise ValueError('Global section is not a dictionary')
        else:
            self._validate_range_entries(self._config['GLOBAL'])
        if 'REGIONAL' not in self._config:
            self._config['REGIONAL'] = {}
        elif not isinstance(self._config['REGIONAL'], dict):
            raise ValueError('Regional section is not a dictionary')
        else:
            for key in list(self._config['REGIONAL'].keys()):
                if '_BoundingBox' not in self._config['REGIONAL'][key]:
                    raise ValueError(f'Invalid regional section {key} in {self._file_path}, missing bounding box')
                self._config['REGIONAL'][key]['_BoundingBox'] = shapely.from_wkt(self._config['REGIONAL'][key]['_BoundingBox'])
                if not isinstance(self._config['REGIONAL'][key]['_BoundingBox'], shapely.Polygon):
                    raise ValueError(f"Bounding box for {key} in {self._file_path} must be a polygon")
                self._validate_range_entries(self._config['REGIONAL'][key], True)

    def _validate_range_entries(self, entries: dict, skip_bounding_box: bool = False):
        for x in entries:
            if x == '_BoundingBox' and skip_bounding_box:
                continue
            if not isinstance(x, dict):
                raise ValueError(f'Entry {x} in parameter list must be a dictionary')
            has_min = 'minimum' in x
            has_max = 'maximum' in x
            if not (has_min or has_max):
                self._log.warning(f"Entry {x} does not define a minimum or maximum")
                continue
            if has_min:
                x['minimum'] = float(x['minimum'])
            if has_max:
                x['maximum'] = float(x['maximum'])
            if 'units' in x and x['units'] and not self._converter.is_valid_unit(x['units']):
                raise ValueError(f'Entry {x} has an invalid unit string')

    def build_parameter_references(self, lat: t.Union[float, UFloat], lon: t.Union[float, UFloat]) -> tuple[dict, set]:
        regions = set()
        base = {x: self._config['GLOBAL'][x] for x in self._config['GLOBAL']}
        geom = upoint_to_geometry(latitude=lat, longitude=lon)
        for region_key in self._config['REGIONAL']:
            if geom.intersects(self._config['REGIONAL'][region_key]['_BoundingBox']):
                base.update({x: self._config['REGIONAL'][region_key][x] for x in self._config['REGIONAL'][region_key] if x != '_BoundingBox'})
                regions.add(region_key)
        return base, regions


class NODBParameterCheck(BaseTestSuite):

    def __init__(self, config_file: t.Union[pathlib.Path, str], **kwargs):
        super().__init__(
            'nodb_parameter_check',
            '1.0',
            test_tags=['GTSPP_2.1', 'GTSPP_2.2'],
            **kwargs
        )
        self._ref = NODBParameterReference(pathlib.Path(config_file) if not isinstance(config_file, pathlib.Path) else config_file, self.converter)

    @RecordTest()
    def test_parameter_ranges(self, record: ocproc2.DataRecord, context: TestContext):
        self.require_good_value(record.coordinates, 'Latitude', True)
        self.require_good_value(record.coordinates, 'Longitude', True)
        references, regions = self._ref.build_parameter_references(
            record.coordinates['Latitude'].to_float_with_uncertainty(),
            record.coordinates['Longitude'].to_float_with_uncertainty()
        )
        if not references:
            return
        self.record_note(f"Parameter regions identified as [{';'.join(regions)}]", context, False)
        self._test_against_reference_and_loop(record, context, references)

    def _test_against_reference_and_loop(self, record: ocproc2.DataRecord, context: TestContext, references: dict):
        self._test_against_reference(record, context, references)
        for sr, sr_ctx in self.iterate_on_subrecords(record, context):
            if sr.coordinates.has_value('Latitude') or sr.coordinates.has_value('Longitude'):
                continue
            with sr_ctx.self_context() as ctx:
                self._test_against_reference_and_loop(sr, ctx, references)

    def _test_against_reference(self, record: ocproc2.DataRecord, context: TestContext, references: dict):
        for x in references:
            if x in record.coordinates:
                with context.coordinate_context(x):
                    self.test_all_subvalues(record.coordinates[x], context, self._test_reference_range, reference=references[x])
            elif x in record.parameters:
                with context.parameter_context(x):
                    self.test_all_subvalues(record.parameters[x], context, self._test_reference_range, reference=references[x])
            elif x in record.metadata:
                with context.metadata_context(x):
                    self.test_all_subvalues(record.metadata[x], context, self._test_reference_range, reference=references[x])

    def _test_reference_range(self, v: ocproc2.Value, ctx: TestContext, reference: dict):
        ref_units = reference['units'] if 'units' in reference else None
        kwargs = reference['kwargs'] if 'kwargs' in reference else {}
        if v.is_empty():
            return
        if 'minimum' in reference:
            self.assert_greater_than('parameter_too_low', v, reference['minimum'], ref_units, **kwargs)
        if 'maximum' in reference:
            self.assert_less_than('parameter_too_high', v, reference['maximum'], ref_units, **kwargs)
