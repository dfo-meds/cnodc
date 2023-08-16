import datetime

from cnodc.ocproc2 import ReferenceTables, DataValue, DataValueMap
from cnodc.nodb import NODBObservation
from .common import QCSkip, QCError, QCReview, QCDelay, qc_test
from autoinject import injector
from shapely import Polygon, Point

from ..ocproc2.structures import NODBQCFlag, DataRecord


class ReferenceValue:

    def __init__(self, name: str, units: str, min_val: float, max_val: float, region: Polygon = None, min_depth: float = None, max_depth: float = None):
        self.name = name
        self.units = units
        self.min_val = min_val
        self.max_val = max_val
        self.min_depth = min_depth
        self.max_depth = max_depth
        self.region = region

    def in_region(self, lat, long, depth):
        if self.region is not None:
            if lat is None or long is None:
                return False
            if not self.region.contains(Point(long, lat)):
                return False
        if self.min_depth is not None or self.max_depth is not None:
            if depth is None:
                return False
            if self.min_depth is not None and depth < self.min_depth:
                return False
            if self.max_depth is not None and depth > self.max_depth:
                return False
        return True


class ValueRangeReferenceTable:

    tables: ReferenceTables = None

    @injector.construct
    def __init__(self):
        self._reference_ranges: list[ReferenceValue] = [
            ReferenceValue("LAT", "degrees", -90, 90),
            ReferenceValue("LON", "degrees", -180, 180),
        ]

    def get_reference_ranges(self, name, units=None, lat=None, long=None, depth=None):
        for ref in self._reference_ranges:
            if name != ref.name:
                continue
            if units is not None and ref.units is None:
                continue
            if units is None and ref.units is not None:
                continue
            if not self.tables.can_convert(units, ref.units):
                continue
            if not ref.in_region(lat, long, depth):
                continue
            yield ref


@qc_test('CSC', 'Coordinate Sanity Check')
def coordinate_sanity_check(obs: NODBObservation):
    record = obs.decode_data_record()
    if record:
        passed = validate_property(obs, 'LAT', record.coordinates, -90, 90, True)
        passed = validate_property(obs, 'LON', record.coordinates, -180, 180, True) and passed
        passed = validate_property(obs, 'OBS_TIME', record.coordinates, None, datetime.datetime.utcnow(), True) and passed
        if not passed:
            raise QCReview(f"Sanity check failed for one or more of latitude, longitude, and observation time", None)
        if 'LAT' in record.coordinates:
            obs.latitude = record.coordinates['LAT'].value()
        else:
            obs.latitude = None
        if 'LON' in record.coordinates:
            obs.longitude = record.coordinates['LON'].value()
        else:
            obs.longitude = None
        if 'OBS_TIME' in record.coordinates and record.coordinates['OBS_TIME'].value() is not None:
            obs.obs_time = datetime.datetime.fromisoformat(record.coordinates['OBS_TIME'].value())
        else:
            obs.obs_time = None


def validate_property(obs: NODBObservation, property_short_name, property_map: DataValueMap, min_value, max_value, required: bool = True, as_date_time: bool = False):
    if property_short_name not in property_map:
        if required:
            property_map[property_short_name] = DataValue(None, None, {"NODB_QC": "R"})
            obs.add_qc_error_for_review(f"{property_short_name}_missing")
            return False
        return True
    prop = property_map[property_short_name]
    if prop.qc_done():
        return True
    value = prop.value()
    if value is None:
        if required:
            prop.set_nodb_flag(NODBQCFlag.FOR_REVIEW)
            obs.add_qc_error_for_review(f"{property_short_name}_empty")
            return False
        return True
    if as_date_time:
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError as ex:
            prop.set_nodb_flag(NODBQCFlag.FOR_REVIEW)
            obs.add_qc_error_for_review(f"{property_short_name}_not_valid_iso")
    if min_value is not None and value < min_value:
        prop.set_nodb_flag(NODBQCFlag.FOR_REVIEW)
        obs.add_qc_error_for_review(f"{property_short_name}_too_low")
        return False
    if max_value is not None and value > max_value:
        prop.set_nodb_flag(NODBQCFlag.FOR_REVIEW)
        obs.add_qc_error_for_review(f"{property_short_name}_too_high")
        return False
    return True
