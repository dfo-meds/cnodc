import datetime
import itertools
from uncertainties import UFloat
from cnodc.nodb import NODBControllerInstance
from cnodc.ocean_math.geodesy import uhaversine
from cnodc.qc.base import BaseTestSuite, BatchTest, TestContext, QCSkipTest
import cnodc.ocproc2.structures as ocproc2
import cnodc.nodb.structures as structures
import enum
import typing as t
from autoinject import injector
import cnodc.ocean_math.umath_wrapper as umath


class DuplicateCheckResult(enum.Enum):

    IDENTICAL = 'I'
    A_IS_SUPERSET = 'A'
    B_IS_SUPERSET = 'B'
    DISJOINT_MATCH = 'D'
    PROBABLE_MATCH = 'P'
    IMPROBABLE_MATCH = '?'
    NO_MATCH = 'N'


class ValueCompareResult(enum.Enum):

    IDENTICAL = 'I'
    A_BETTER = 'A'
    B_BETTER = 'B'
    CONFLICT = 'X'
    COMPATIBLE = 'C'


class BatchCompareResult(enum.Enum):

    NO_MATCH = 'N'
    A_IS_DUPE = 'A'
    B_IS_DUPE = 'B'
    BOTH_MAYBE_DUPE = 'M'


@injector.injectable_global
class RecordSearcher:

    def find_records(self,
                     db: NODBControllerInstance,
                     station_uuid: str,
                     min_time: datetime.datetime,
                     max_time: datetime.datetime,
                     min_lat: float,
                     max_lat: float,
                     min_lon: float,
                     max_lon: float) -> t.Iterable[t.Union[structures.NODBWorkingRecord, structures.NODBObservationData]]:
        # TODO
        pass


class NODBDuplicateCheck(BaseTestSuite):

    db_records: RecordSearcher = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__('nodb_dupe_check', '1.0', **kwargs)
        self.time_window = 15 * 60   # seconds
        self.distance_window = 5000  # m
        self._probable_threshold = 0.8  # fraction
        self._improbable_threshold = 0.2  # fraction
        one_degree_lat = uhaversine((0, 0), (90, 0)).nominal_value / 90.0
        self._lat_range = self.distance_window / one_degree_lat

    @BatchTest()
    def dupe_check(self, batch: dict[str, TestContext]):
        batch_keys = []
        # Build a list of records in this batch that have a valid Latitude, Longitude, Time, and CNODCStation
        for key in batch:
            try:
                self.precheck_value_in_map(batch[key].top_record.coordinates, 'Latitude')
                self.precheck_value_in_map(batch[key].top_record.coordinates, 'Longitude')
                self.precheck_value_in_map(batch[key].top_record.coordinates, 'Time')
                self.precheck_value_in_map(batch[key].top_record.metadata, 'CNODCStation')
                batch_keys.append(key)
            except QCSkipTest:
                continue
        skip_keys = set()
        n_batch_keys = len(batch_keys)
        for i in range(0, n_batch_keys - 1):
            for j in range(i + 1, n_batch_keys):
                if j in skip_keys:
                    continue
                result = self._check_batch_duplicate(batch[batch_keys[i]], batch[batch_keys[j]])
                if result == BatchCompareResult.A_IS_DUPE:
                    skip_keys.add(batch_keys[i])
                    break
                elif result == BatchCompareResult.B_IS_DUPE:
                    skip_keys.add(batch_keys[j])
        for key in batch_keys:
            if key in skip_keys:
                continue
            self._check_db_duplicates(batch[key])

    def _check_db_duplicates(self, context_a: TestContext):
        # TODO
        pass

    def _stream_db_records(self, context_a: TestContext) -> t.Iterable[t.Union[structures.NODBWorkingRecord, structures.NODBObservationData]]:
        time = context_a.top_record.coordinates['Time'].to_datetime()
        kwargs = {
            'min_time': time - datetime.timedelta(self.time_window),
            'max_time': time + datetime.timedelta(self.time_window),
            'station_uuid': context_a.top_record.metadata['CNODCStation'].best_value()
        }
        lat = context_a.top_record.coordinates['Latitude'].to_float_with_uncertainty()
        lat_min = ((lat.nominal_value - lat.std_dev) if isinstance(lat, UFloat) else lat) - self._lat_range
        lat_max = ((lat.nominal_value + lat.std_dev) if isinstance(lat, UFloat) else lat) + self._lat_range
        if lat_min < -89.9:
            yield from self.db_records.find_records(
                db=self._db,
                max_lat=lat_max,
                min_lat=-90,
                max_lon=180,
                min_lon=-180,
                **kwargs
            )
        elif lat_max > 89.9:
            yield from self.db_records.find_records(
                db=self._db,
                max_lat=90,
                min_lat=lat_min,
                max_lon=180,
                min_lon=-180,
                **kwargs
            )
        else:
            kwargs['min_lat'] = lat_min
            kwargs['max_lat'] = lat_max
            lon = context_a.top_record.coordinates['Longitude'].to_float_with_uncertainty()
            ref_lat = max(abs(lat_min), abs(lat_max))
            one_degree_lon = uhaversine((ref_lat, 0), (ref_lat, 1))
            lon_range = self.distance_window / one_degree_lon
            lon_min = ((lon.nominal_value - lon.std_dev) if isinstance(lon, UFloat) else lon) - lon_range
            lon_max = ((lon.nominal_value + lon.std_dev) if isinstance(lon, UFloat) else lon) + lon_range
            if lon_min < -180:
                # slice off the elements left and shift them around the global
                yield from self.db_records.find_records(
                    db=self._db,
                    max_lon=180,
                    min_lon=lon_min+360,
                    **kwargs
                )
                yield from self.db_records.find_records(
                    db=self._db,
                    max_lon=lon_max,
                    min_lon=-180,
                    **kwargs
                )
            elif lon_max > 180:
                # slice off the elements left and shift them around the global
                yield from self.db_records.find_records(
                    db=self._db,
                    max_lon=lon_max-360,
                    min_lon=-180,
                    **kwargs
                )
                yield from self.db_records.find_records(
                    db=self._db,
                    max_lon=180,
                    min_lon=lon_min,
                    **kwargs
                )
            else:
                yield from self.db_records.find_records(
                    db=self._db,
                    max_lon=lon_max,
                    min_lon=lon_min,
                    **kwargs
                )

    def _check_batch_duplicate(self, context_a: TestContext, context_b: TestContext) -> BatchCompareResult:
        result = self._check_is_duplicate(context_a.top_record, context_b.top_record)
        if result == DuplicateCheckResult.NO_MATCH:
            return BatchCompareResult.NO_MATCH
        elif result in (DuplicateCheckResult.IDENTICAL, DuplicateCheckResult.B_IS_SUPERSET):
            self._handle_positive_match_working(context_a, context_b.working_record.working_uuid)
            return BatchCompareResult.A_IS_DUPE
        elif result == DuplicateCheckResult.A_IS_SUPERSET:
            self._handle_positive_match_working(context_b, context_a.working_record.working_uuid)
            return BatchCompareResult.B_IS_DUPE
        else:
            self._handle_ambiguous_match_working(context_a, context_b)
            return BatchCompareResult.BOTH_MAYBE_DUPE

    def _handle_positive_match_working(self, context: TestContext, working_uuid: str):
        with context.self_context():
            context.top_record.metadata['CNODCWorkingDuplicateID'] = working_uuid
            self.report_for_review('working_duplicate_record_found')

    def _handle_ambiguous_match_working(self, context_a: TestContext, context_b: TestContext):
        with context_a.self_context():
            context_a.top_record.metadata['CNODCWorkingDuplicateID'] = context_b.working_record.working_uuid
            self.report_for_review('working_potential_duplicate_record_found')
        with context_b.self_context():
            context_b.top_record.metadata['CNODCWorkingDuplicateID'] = context_a.working_record.working_uuid
            self.report_for_review('working_potential_duplicate_record_found')

    def _check_is_duplicate(self, record_a: ocproc2.DataRecord, record_b: ocproc2.DataRecord) -> DuplicateCheckResult:
        # Station check
        if record_a.metadata.best_value('CNODCStation') != record_b.metadata.best_value('CNODCStation'):
            return DuplicateCheckResult.NO_MATCH
        # Time check
        time_a = record_a.coordinates['Time'].to_datetime()
        time_b = record_b.coordinates['Time'].to_datetime()
        if (time_b - time_a).total_seconds() > self.time_window:
            return DuplicateCheckResult.NO_MATCH
        # Distance check
        lat_a = record_a.coordinates['Latitude'].to_float_with_uncertainty()
        lon_a = record_a.coordinates['Longitude'].to_float_with_uncertainty()
        lat_b = record_b.coordinates['Latitude'].to_float_with_uncertainty()
        lon_b = record_b.coordinates['Longitude'].to_float_with_uncertainty()
        distance = uhaversine((lat_a, lon_a), (lat_b, lon_b))
        if umath.is_greater_than(distance, self.distance_window):
            return DuplicateCheckResult.NO_MATCH
        return self._check_record_contents(record_a, record_b)

    def _check_record_contents(self, record_a: ocproc2.DataRecord, record_b: ocproc2.DataRecord) -> DuplicateCheckResult:
        results = self._compare_records(record_a, record_b)
        identical = results[ValueCompareResult.IDENTICAL] if ValueCompareResult.IDENTICAL in results else 0
        a_better = results[ValueCompareResult.A_BETTER] if ValueCompareResult.A_BETTER in results else 0
        b_better = results[ValueCompareResult.B_BETTER] if ValueCompareResult.B_BETTER in results else 0
        compatible = results[ValueCompareResult.COMPATIBLE] if ValueCompareResult.COMPATIBLE in results else 0
        conflict = results[ValueCompareResult.CONFLICT] if ValueCompareResult.CONFLICT in results else 0
        if conflict > 0:
            total_semi_match = a_better + b_better + compatible + identical
            total_count = total_semi_match + conflict
            upper_threshold = int(self._probable_threshold * total_count)
            lower_threshold = int(self._improbable_threshold * total_count)
            if total_semi_match >= upper_threshold:
                return DuplicateCheckResult.PROBABLE_MATCH
            elif total_semi_match >= lower_threshold:
                return DuplicateCheckResult.IMPROBABLE_MATCH
            else:
                return DuplicateCheckResult.NO_MATCH
        elif compatible > 0:
            return DuplicateCheckResult.DISJOINT_MATCH
        elif b_better > 0:
            return DuplicateCheckResult.DISJOINT_MATCH if a_better > 0 else DuplicateCheckResult.B_IS_SUPERSET
        elif a_better > 0:
            return DuplicateCheckResult.A_IS_SUPERSET
        else:
            return DuplicateCheckResult.IDENTICAL

    def _compare_records(self, record_a: ocproc2.DataRecord, record_b: ocproc2.DataRecord) -> dict[ValueCompareResult, int]:
        results = {}
        self._update_results(results, self._compare_vmaps(record_a.coordinates, record_b.coordinates, missing_is_compatible=False))
        self._update_results(results, self._compare_vmaps(record_a.parameters, record_b.parameters))
        self._update_results(results, self._compare_vmaps(record_a.metadata, record_b.metadata))
        for srt in set(itertools.chain(record_a.subrecords.keys(), record_b.subrecords.keys())):
            self._update_results(results, self._compare_subrecord_sets(record_a.subrecords[srt], record_b.subrecords[srt]))
        return results

    def _compare_subrecord_sets(self,
                                subrecordsets_a: dict[int, ocproc2.RecordSet],
                                subrecordsets_b: dict[int, ocproc2.RecordSet]) -> dict[ValueCompareResult, int]:
        count_a = len(subrecordsets_a)
        count_b = len(subrecordsets_b)
        # 0 to 0 is easy
        if count_a == 0 or count_b == 0:
            if count_a == 0 and count_b == 0:
                return {}
            results = {}
            empty_set = ocproc2.RecordSet()
            if count_a > 0:
                for key in subrecordsets_a:
                    self._update_results(results, self._actual_compare_subrecord_sets(subrecordsets_a[key], empty_set))
            else:
                for key in subrecordsets_b:
                    self._update_results(results, self._actual_compare_subrecord_sets(empty_set, subrecordsets_b[key]))
            return results
        # One to one is easy
        if count_a == 1 and count_b == 1:
            return self._actual_compare_subrecord_sets(subrecordsets_a[list(subrecordsets_a.keys())[0]], subrecordsets_b[list(subrecordsets_b.keys())[0]])
        # Otherwise, we will have to do the best matches we can based on coordinates
        coord_match_results = []
        for rs_idx_a in subrecordsets_a:
            for rs_idx_b in subrecordsets_b:
                coord_match_results.append((rs_idx_a, rs_idx_b, self._compare_recordset_coordinates(subrecordsets_a[rs_idx_a], subrecordsets_b[rs_idx_b])))
        # Sort best to worst matches
        coord_match_results.sort(key=lambda x: x[2], reverse=True)
        used_a = set()
        used_b = set()
        results = {}
        for rs_idx_a, rs_idx_b, _ in coord_match_results:
            if rs_idx_a in used_a:
                continue
            if rs_idx_b in used_b:
                continue
            self._update_results(results, self._actual_compare_subrecord_sets(subrecordsets_a[rs_idx_a], subrecordsets_b[rs_idx_b]))
            used_a.add(rs_idx_a)
            used_b.add(rs_idx_b)
            if len(used_a) == count_a or len(used_b) == count_b:
                break
        if len(used_a) != count_a:
            empty_rs = ocproc2.RecordSet()
            for key in subrecordsets_a:
                if key in used_a:
                    continue
                self._update_results(results, self._actual_compare_subrecord_sets(subrecordsets_a[key], empty_rs))
        if len(used_b) != count_b:
            empty_rs = ocproc2.RecordSet()
            for key in subrecordsets_b:
                if key in used_b:
                    continue
                self._update_results(results, self._actual_compare_subrecord_sets(empty_rs, subrecordsets_b[key]))
        return results

    def _actual_compare_subrecord_sets(self, rs_a: ocproc2.RecordSet, rs_b: ocproc2.RecordSet) -> dict[ValueCompareResult, int]:
        results = {}
        a_count = len(rs_a.records)
        b_count = len(rs_b.records)
        min_count = min(a_count, b_count)
        for idx in range(0, min_count):
            self._update_results(results, self._compare_records(rs_a.records[idx], rs_b.records[idx]))
        if a_count > b_count:
            blank_rec = ocproc2.DataRecord()
            for i in range(min_count, a_count):
                self._update_results(results, self._compare_records(rs_a.records[i], blank_rec))
        elif b_count > a_count:
            blank_rec = ocproc2.DataRecord()
            for i in range(min_count, b_count):
                self._update_results(results, self._compare_records(blank_rec, rs_b.records[i]))
        return results

    def _compare_recordset_coordinates(self, rs_a: ocproc2.RecordSet, rs_b: ocproc2.RecordSet) -> float:
        a_count = len(rs_a.records)
        b_count = len(rs_b.records)
        min_count = min(a_count, b_count)
        max_count = max(a_count, b_count)
        score = 0
        max_score = 0
        for idx in range(0, min_count):
            max_score += 1
            res = self._compare_record_coordinates(rs_a.records[idx], rs_b.records[idx])
            if res == ValueCompareResult.IDENTICAL:
                score += 1
            elif res == ValueCompareResult.CONFLICT:
                pass
            else:
                score += 0.5
        max_score += (max_count - min_count)
        return score / max_score

    def _compare_record_coordinates(self, record_a: ocproc2.DataRecord, record_b: ocproc2.DataRecord) -> ValueCompareResult:
        return self._distill_compare_results(self._compare_vmaps(
            record_a.coordinates,
            record_b.coordinates,
            missing_is_compatible=False
        ))

    def _update_results(self, original: dict, new: dict):
        for k in new:
            if k not in original:
                original[k] = new[k]
            else:
                original[k] += new[k]

    def _compare_vmaps(self,
                       vmap_a: ocproc2.ValueMap,
                       vmap_b: ocproc2.ValueMap,
                       skip_keys: t.Optional[list[str]] = None,
                       missing_is_compatible: bool = True) -> dict[ValueCompareResult, int]:
        results = {}
        for key in set(itertools.chain(vmap_a.keys(), vmap_b.keys())):
            if skip_keys is not None and key in skip_keys:
                continue
            result = self._compare_values(vmap_a, vmap_b, key, missing_is_compatible)
            if result not in results:
                results[result] = 0
            results[result] += 1
        return results

    def _compare_values(self,
                        vmap_a: ocproc2.ValueMap,
                        vmap_b: ocproc2.ValueMap,
                        key: str,
                        missing_is_compatible: bool = True) -> ValueCompareResult:
        # Finds the best possible comparison between values for a given value map key
        # Missing in A
        if key not in vmap_a:
            return ValueCompareResult.B_BETTER if missing_is_compatible else ValueCompareResult.CONFLICT
        # Missing in B
        if key not in vmap_b:
            return ValueCompareResult.A_BETTER if missing_is_compatible else ValueCompareResult.CONFLICT
        # Fast check for the most common use case (a single value, not using MultiValue)
        if not (isinstance(vmap_a[key], ocproc2.MultiValue) or isinstance(vmap_b[key], ocproc2.MultiValue)):
            return self._compare_value_elements(vmap_a[key], vmap_b[key])
        # One or more is using multivalue, we will do a more complete check to try and pair
        # the values in a way that makes sense (i.e. has the best match for any given pair)
        # Best match is defined as Identical > A Better > B Better > Compatible > Conflict
        a_values = [v for v in vmap_a[key].all_values()]
        b_values = [v for v in vmap_b[key].all_values()]
        results = {}
        for idx_a, val_a_ in enumerate(a_values):
            results[idx_a] = {}
            for idx_b, val_b_ in enumerate(b_values):
                results[idx_a][idx_b] = self._compare_value_elements(val_a_, val_b_)
        # Track which values we've assigned
        check_b_values = list(x for x in range(0, len(b_values)))
        final_results = {}
        # Assign values in priority order (this function will remove items from check_b_values as it goes
        # to prevent the same item in B from being assigned twice)
        self._assign_matching_compare_results(results, check_b_values, final_results, ValueCompareResult.IDENTICAL)
        self._assign_matching_compare_results(results, check_b_values, final_results, ValueCompareResult.A_BETTER)
        self._assign_matching_compare_results(results, check_b_values, final_results, ValueCompareResult.B_BETTER)
        self._assign_matching_compare_results(results, check_b_values, final_results, ValueCompareResult.COMPATIBLE)
        self._assign_matching_compare_results(results, check_b_values, final_results, ValueCompareResult.CONFLICT)
        # If B has values left that weren't assigned
        if check_b_values and ValueCompareResult.B_BETTER not in final_results:
            final_results[ValueCompareResult.B_BETTER] = 1
        # If A has values left that weren't assigned
        if results and ValueCompareResult.A_BETTER not in final_results:
            final_results[ValueCompareResult.A_BETTER] = 1
        return self._distill_compare_results(final_results)

    def _distill_compare_results(self, results: dict[ValueCompareResult, int]) -> ValueCompareResult:
        # If any of the results conflict, then the result is a conflict
        if ValueCompareResult.CONFLICT in results:
            return ValueCompareResult.CONFLICT
        if ValueCompareResult.COMPATIBLE in results:
            return ValueCompareResult.COMPATIBLE
        a_better = ValueCompareResult.A_BETTER in results
        b_better = ValueCompareResult.B_BETTER in results
        # If A and B are both "better" for different values, we will call them COMPATIBLE
        if a_better and b_better:
            return ValueCompareResult.COMPATIBLE
        elif a_better:
            return ValueCompareResult.A_BETTER
        elif b_better:
            return ValueCompareResult.B_BETTER
        else:
            return ValueCompareResult.IDENTICAL

    def _assign_matching_compare_results(self,
                                         original_results: dict[int, dict[int, ValueCompareResult]],
                                         check_list: list[int],
                                         final_results: dict[ValueCompareResult, int],
                                         result_type: ValueCompareResult):
        for idx_a in list(original_results.keys()):
            if not check_list:
                break
            match = None
            for idx_b in list(check_list):
                if original_results[idx_a][idx_b] == result_type:
                    match = idx_b
                    break
            if match is not None:
                if result_type not in final_results:
                    final_results[result_type] = 0
                final_results[result_type] += 1
                del original_results[idx_a]
                check_list.remove(match)

    def _compare_value_elements(self, val_a: ocproc2.Value, val_b: ocproc2.Value) -> ValueCompareResult:
        is_numeric = False
        # Exactly the same value
        if val_a == val_b:
            return ValueCompareResult.IDENTICAL
        # Both empty, thus the units and metadata don't really matter
        elif val_a.is_empty() and val_b.is_empty():
            return ValueCompareResult.IDENTICAL
        # Value A is empty only, this means B is better
        elif val_a.is_empty():
            return ValueCompareResult.B_BETTER
        # Same but for B empty
        elif val_b.is_empty():
            return ValueCompareResult.A_BETTER
        # For numeric values, we try converting to a common unit and scale
        elif val_a.is_numeric() and val_b.is_numeric():
            val_a_real = self.value_in_units(val_a)
            val_b_real = self.value_in_units(
                val_b,
                expected_units=val_a.metadata.best_value('Units', None),
                temp_scale=val_a.metadata.best_value('TemperatureScale', None)
            )
            if not umath.is_close(val_a_real, val_b_real):
                return ValueCompareResult.CONFLICT
            is_numeric = True
        # For non-numeric values, we just check equality
        else:
            if not val_a.value == val_b.value:
                return ValueCompareResult.CONFLICT
        # Now we know the values are present, non-empty, and equal (given adjustments for units, scales, and
        # uncertainty), this means the difference must be in the metadata
        if is_numeric:
            return self._distill_compare_results(
                self._compare_vmaps(val_a.metadata, val_b.metadata, ['Units', 'Uncertainty', 'TemperatureScale'])
            )
        else:
            return self._distill_compare_results(self._compare_vmaps(val_a.metadata, val_b.metadata))

