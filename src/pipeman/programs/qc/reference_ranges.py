import decimal
import pathlib
import typing as t
from dataclasses import dataclass

import shapely
import yaml
import zrlog

from medsutil import math as amath, geodesy
from medsutil.exceptions import CodedError
from medsutil.units import UnitConverter
from pipeman.programs.qc.base import ReferenceRange, QualityChecker, ElementType, RecordRef, SingleElementRef, review


@dataclass
class ParameterReference:
    range: ReferenceRange
    min_depth: amath.AnyNumber | None = None
    max_depth: amath.AnyNumber | None = None


class ParameterReferenceError(CodedError): CODE_SPACE = "PARAM-REF"


class ParameterReferences:

    def __init__(self, config_file: str | pathlib.Path, converter: UnitConverter):
        self._converter = converter
        self._file_path = pathlib.Path(config_file) if not isinstance(config_file, pathlib.Path) else config_file
        self._global_references: dict[str, list[ParameterReference]] = {}
        self._regional_references: dict[str, tuple[shapely.Polygon, dict[str, list[ParameterReference]]]] = {}
        self._log = zrlog.get_logger("cnodc.param_reference")
        self._load_file()

    def _load_file(self):

        if not self._file_path.exists():
            raise ParameterReferenceError(f"Invalid configuration file: {self._file_path}", 1000)

        with open(self._file_path, "r") as h:
            config = yaml.safe_load(h) or {}

        if 'GLOBAL' not in config or not config['GLOBAL']:
            self._log.warning(f"Parameter check missing GLOBAL section in f{self._file_path}")
        elif not isinstance(config['GLOBAL'], dict):
            raise ParameterReferenceError('Global section is not a dictionary', 1001)
        else:
            self._load_entries(config['GLOBAL'], self._global_references)

        if 'REGIONAL' not in config or not config["REGIONAL"]:
            pass
        elif not isinstance(config['REGIONAL'], dict):
            raise ValueError('Regional section is not a dictionary', 1002)
        else:
            for region_name, region_dict in config["REGIONAL"].items():
                if region_name not in self._regional_references:
                    if '_BoundingBox' not in region_dict:
                        raise ParameterReferenceError(f"Missing regional bounding box for [{region_name}]", 1003)
                    self._regional_references[region_name] = (
                        self._load_bounding_box(region_dict['_BoundingBox']),
                        {}
                    )
                self._load_entries(config['REGIONAL'][region_name], self._regional_references[region_name][1])

    def _load_entries(self, entries: dict[str, dict[str, t.Any]], target: dict[str, list[ParameterReference]]):
        for k, v in entries.items():
            if k != '_BoundingBox':
                if isinstance(v, dict):
                    ref_range = self._load_entry(k, v)
                    if ref_range is not None:
                        if k not in target:
                            target[k] = []
                        target[k].append(ref_range)
                elif isinstance(v, list):
                    for sub_entry in v:
                        if not isinstance(v, dict):
                            raise ParameterReferenceError(f'Entry {k} in parameter list must be a dictionary', 1201)
                        ref_range = self._load_entry(k, sub_entry)
                        if ref_range is not None:
                            if k not in target:
                                target[k] = []
                            target[k].append(ref_range)
                else:
                    raise ParameterReferenceError(f'Entry {k} in parameter list must be a dictionary', 1200)

    def _load_bounding_box(self, bounding_box: str | list[str]) -> shapely.Polygon:
        try:
            if isinstance(bounding_box, str):
                return t.cast(shapely.Polygon, shapely.from_wkt(bounding_box))
            else:
                pg = "POLYGON((" + ",".join(coord_pair.strip().replace(",", " ").replace("  ", " ") for coord_pair in bounding_box) + "))"
                return t.cast(shapely.Polygon, shapely.from_wkt(pg))
        except Exception as ex:
            raise ParameterReferenceError("Invalid bounding box", 1300) from ex

    def _load_entry(self, k: str, entry: dict[str, t.Any]) -> ParameterReference | None:
        if not (entry.get('minimum', None) is not None or entry.get('maximum', None) is not None):
            self._log.warning(f"Entry {k} does not define a minimum or maximum")
            return None
        if 'units' in entry:
            if entry["units"] and not self._converter.is_valid_unit(str(entry["units"])):
                raise ParameterReferenceError(f"Entry {k} has an invalid unit string")
        else:
            self._log.warning(f"Entry {k} does not define its units")
        min_depth = None
        max_depth = None
        if 'min_depth' in entry and entry['min_depth']:
            min_depth = decimal.Decimal(entry['min_depth'])
        if 'max_depth' in entry and entry['max_depth']:
            max_depth = decimal.Decimal(entry['max_depth'])
        return ParameterReference(
            min_depth=min_depth,
            max_depth=max_depth,
            range=ReferenceRange(**{k: v for k, v in entry if k not in ("min_depth", "max_depth")})
        )

    def build_parameter_references(self,
                                   lat: amath.AnyNumber,
                                   lon: amath.AnyNumber,
                                   depth: amath.AnyNumber | None) -> tuple[set[str], dict[str, ReferenceRange]]:
        regions: set[str] = set()
        base: dict[str, list[ParameterReference]] = {}
        geom = geodesy.buffer_coordinates(lat, lon)
        for region_key, regional_info in self._regional_references.items():
            regional_boundary, regional_refs = regional_info
            if regional_boundary.intersects(geom):
                base.update(regional_refs)
                regions.add(region_key)
        filtered = {}
        for k, v in base.items():
            new_v = self._find_first_for_depth(depth, v)
            if new_v is not None:
                filtered[k] = v
        return regions, filtered

    def _find_first_for_depth(self, depth: amath.AnyNumber | None, ranges: list[
        ParameterReference]) -> ReferenceRange | None:
        for p_ref in ranges:
            if depth is not None:
                if p_ref.min_depth is not None and amath.lt(p_ref.min_depth, depth) and not amath.is_close(p_ref.min_depth, depth):
                    continue
                if p_ref.max_depth is not None and amath.gt(p_ref.max_depth, depth) and not amath.is_close(p_ref.max_depth, depth):
                    continue
            elif p_ref.min_depth is not None or p_ref.max_depth is not None:
                continue
            return p_ref.range
        return None


class ReferenceRangeChecker(QualityChecker):

    def __init__(self, config_file: str | pathlib.Path, **kwargs):
        super().__init__(**kwargs)
        self._ref = ParameterReferences(
            config_file,
            self.converter
        )

    def run(self):
        self.crawl_record(self.current_record,
            record_cb=self.parameter_range_check_for_record,
            limit_element_types=ElementType.PARAMETERS,
            track_coordinates=True,
        )

    def parameter_range_check_for_record(self, ref: RecordRef):
        regions, ref_ranges = self._get_parameter_ranges(self.current_latitude, self.current_longitude, self.current_depth)
        if ref_ranges:
            for p_ref in self.iterate_on_record_single_elements(ref, ElementType.PARAMETERS):
                if p_ref.element_name in ref_ranges:
                    self.parameter_range_check_for_element(p_ref, ref_ranges[p_ref.element_name])

    @review("in_reference_range", fail_flag=4, pass_flag=1)
    def parameter_range_check_for_element(self, ref: SingleElementRef, reference_range: ReferenceRange):
        self.assert_in_reference_range(ref.element, reference_range, msg="outside_reference_range")

    def _get_parameter_ranges(self,
                              lat: amath.AnyNumber | None,
                              lon: amath.AnyNumber | None,
                              depth: amath.AnyNumber | None) -> tuple[set[str], dict[str, ReferenceRange]]:
        if lat is None or lon is None:
            return set(), dict()
        key = (lat, lon, depth)
        if 'ranges' not in self.record_memory:
            self.record_memory['ranges'] = ranges = {}
        else:
            ranges = self.record_memory['ranges']
        if key not in ranges:
            ranges[key] = self._ref.build_parameter_references(lat, lon, depth)
        return ranges[key]
