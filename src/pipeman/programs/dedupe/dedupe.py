import datetime
import enum
import typing as t
from collections import defaultdict

from autoinject import injector

import medsutil.math as amath
from medsutil.awaretime import ScienceDateTime, AwareDateTime
from medsutil.ocproc2 import SingleElement, ElementMap, AbstractElement, MultiElement, OCProc2Ontology, BaseRecord, \
    RecordMap, RecordSet, ParentRecord, ChildRecord
from medsutil.ocproc2.operations import SetRelationships
from medsutil.ocproc2.refs import SingleElementRef
from medsutil.ocproc2.util import RequiredQuality, pair_up_records, dates_overlap, pair_up_recordsets, \
    pair_up_single_elements
from medsutil.units.structures import UnitError
from nodb.observations import QualityCheckFlags, DataMode
from pipeman.programs.qc.base import QualityController


class CompareResult(enum.Enum):
    A_BETTER = 'A'
    B_BETTER = 'B'
    IDENTICAL = 'C'
    DIFFERENT = 'D'


class ParentCompareResult(enum.Enum):

    IDENTICAL = 'D'
    B_BETTER = 'B'
    A_BETTER = 'A'
    COMPATIBLE = 'M'

    INCOMPATIBLE = 'R'

    NO_MATCH = 'N'


class RelationshipAction(enum.Enum):
    MARK_DUPLICATE = 'mark_duplicate'
    MARK_OTHER_DUPLICATE = 'mark_other_duplicate'
    MERGE = 'merge'
    REVIEW_MERGE = 'merge_with_review'

    MARK_OTHER_BETTER = 'mark_other_better'
    MARK_THIS_BETTER = 'mark_this_better'
    REVIEW_BETTER = "better_review"

    @staticmethod
    def is_reviewable(x: RelationshipAction):
        return x in (
            RelationshipAction.REVIEW_MERGE,
            RelationshipAction.REVIEW_BETTER
        )

    @staticmethod
    def encode_action_list(matches: dict[RelationshipAction, set[tuple[str, datetime.date]]]) -> dict[str, list[list[str]]]:
        return {
            k.value: [
                [x[0], x[1].isoformat()]
                for x in v
            ]
            for k, v in matches.items()
        }

    @staticmethod
    def decode_action_list(matches: dict[str, list[list[str]]]) -> dict[RelationshipAction, set[tuple[str, datetime.date]]]:
        return {
            RelationshipAction(k): set(
                (x[0], datetime.date.fromisoformat(x[1]))
                for x in v
            )
            for k, v in matches.items()
        }


class NODBDuplicateCheck(QualityController):

    ontology: OCProc2Ontology = None

    DEFAULT_TIME_WINDOW = 900  # seconds
    DEFAULT_DISTANCE_WINDOW = 5000  # m
    PROBABLE_THRESHOLD = 0.8  # fraction

    @injector.construct
    def __init__(self):
        super().__init__(test_name='nodb_dupe_check', test_version='1.0')

    def run(self):
        if self._record_quality_flags & QualityCheckFlags.DEDUPLICATE:
            self.qc_pass()

        relationship_matches = self.search_for_relationships()

        self.add_record_action(
            SetRelationships(relationships=RelationshipAction.encode_action_list(relationship_matches)),
            any(RelationshipAction.is_reviewable(x) for x in relationship_matches.keys())
        )
        self._record_quality_flags = self._record_quality_flags | QualityCheckFlags.DEDUPLICATE

    def set_duplicate_list(self, duplicates: dict | None):
        if duplicates is not None:
            elements = []
            for (source_uuid, obs_uuid, obs_date), dedupe_result in duplicates.items():
                action = self.get_dedupe_action(dedupe_result, obs_date, source_uuid)
                sub_element = SingleElement(f"{obs_date.isoformat()}/{obs_uuid}")
                sub_element.metadata["CNODCDuplicateType"] = action.value
                elements.append(sub_element)
            self.current_record.record.metadata['CNODCDuplicateCandidates'] = MultiElement(elements)
        elif 'CNODCDuplicateCandidates' in self.current_record.record.metadata:
            del self.current_record.record.metadata['CNODCDuplicateCandidates']

    def get_dedupe_action(self,
                          dedupe_result: ParentCompareResult,
                          obs_date: datetime.date,
                          source_uuid: str | None) -> RelationshipAction:
        match dedupe_result:
            case ParentCompareResult.IDENTICAL:
                return RelationshipAction.MARK_DUPLICATE
            case ParentCompareResult.A_BETTER:
                return RelationshipAction.MARK_OTHER_DUPLICATE
            case ParentCompareResult.B_BETTER:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return RelationshipAction.MARK_OTHER_DUPLICATE
                else:
                    return RelationshipAction.MARK_DUPLICATE
            case ParentCompareResult.COMPATIBLE:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return RelationshipAction.MARK_OTHER_DUPLICATE
                elif self.does_source_file_replace_current(source_uuid, obs_date):
                    return RelationshipAction.MARK_DUPLICATE
                else:
                    return RelationshipAction.MERGE
            case ParentCompareResult.INCOMPATIBLE:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return RelationshipAction.MARK_OTHER_DUPLICATE
                elif self.does_source_file_replace_current(source_uuid, obs_date):
                    return RelationshipAction.MARK_DUPLICATE
                else:
                    return RelationshipAction.REVIEW_MERGE
            case _:
                raise ValueError(f"Unexpected dedupe result: {dedupe_result}")

    def does_current_replace_source_file(self, source_file_uuid: str | None, received_date: datetime.date) -> bool:
        return self.searcher.is_source_file_replacement(source_file_uuid, received_date, self._source_file_uuid, self._source_file_date)

    def does_source_file_replace_current(self, source_file_uuid: str | None, received_date: datetime.date) -> bool:
        return self.searcher.is_source_file_replacement(self._source_file_uuid, self._source_file_date, source_file_uuid, received_date)

    def ref_exists(self, ref: AbstractElement) -> bool:
        for x in ref.all_values():
            if not self.searcher.record_exists(*x.to_string().split('/', maxsplit=1)):
                return False
        return True

    def get_relationship_action(self,
                                data_mode: DataMode,
                                self_better: int,
                                other_better: int) -> RelationshipAction:
        if DataMode.is_better_than(self._record_data_mode, data_mode):
            return RelationshipAction.MARK_THIS_BETTER
        if DataMode.is_better_than(data_mode, self._record_data_mode):
            return RelationshipAction.MARK_OTHER_BETTER
        if self_better > 0 and other_better == 0:
            return RelationshipAction.MARK_THIS_BETTER
        if other_better > 0 and self_better == 0:
            return RelationshipAction.MARK_OTHER_BETTER
        return RelationshipAction.REVIEW_BETTER

    @staticmethod
    def quality_flags_difference(quality_a: int, quality_b: int, skip_tests: int = 0) -> tuple[int, int]:
        a_better_tests = 0
        b_better_tests = 0
        while quality_a > 0 or quality_b > 0:
            if skip_tests % 2 == 0:
                a_check = quality_a % 2
                b_check = quality_b % 2
                if a_check == 1 and b_check == 0:
                    a_better_tests += 1
                elif a_check == 0 and b_check == 1:
                    b_better_tests += 1
            quality_a = quality_a >> 2
            quality_b = quality_b >> 2
        return a_better_tests, b_better_tests

    def search_for_relationships(self) -> dict[RelationshipAction, set[tuple[str, datetime.date]]]:

        relationships: dict[RelationshipAction, set[tuple[str, datetime.date]]] = {}

        def _process_record(source_uuid: str | None,
                            record_uuid: str,
                            record_date: datetime.date,
                            record: ParentRecord | None,
                            data_mode: DataMode,
                            quality_flags: int):

            if record is None: return

            match_result = self.compare_parent_records(self.current_record.record, record)
            if match_result is ParentCompareResult.NO_MATCH: return

            # this check for quality_flags still matches if one is deduped and the other isn't.
            self_better, other_better = self.quality_flags_difference(self._record_quality_flags, quality_flags, 1)
            if data_mode != self._record_data_mode or (self_better + other_better) > 0:
                action = self.get_relationship_action(match_result, data_mode, self_better, other_better)
            else:
                action = self.get_dedupe_action(match_result, record_date, source_uuid)
            if action not in relationships:
                relationships[action] = set()
            relationships[action].add((record_uuid, record_date))


        search_parameters = self.search_kwargs()
        seen: set[tuple[str, datetime.date]] = set()

        for working in self.searcher.geosearch_working_records(**search_parameters, quality_checks=QualityCheckFlags.DEDUPLICATE):
            wuuid = t.cast(str, working.working_uuid)
            wdate = t.cast(datetime.date, working.received_date)
            seen.add((wuuid, wdate))
            _process_record(
                working.source_file_uuid,
                wuuid,
                wdate,
                working.record,
                working.data_mode,
                working.quality_checks
            )

        for obs_data in self.searcher.geosearch_observations(**search_parameters):
            # prevents working records that were analyzed above from being re-screened
            if (obs_data.obs_uuid, obs_data.received_date) in seen:
                continue
            _process_record(
                obs_data.source_file_uuid,
                obs_data.obs_uuid,
                obs_data.received_date,
                obs_data.record,
                obs_data.data_mode,
                obs_data.quality_checks
            )

        return relationships

    def compare_parent_records(self, a: ParentRecord, b: ParentRecord) -> ParentCompareResult:
        result = self._build_result_stats(self.compare_record(a, b))
        if result[CompareResult.DIFFERENT] > self.PROBABLE_THRESHOLD:
            return ParentCompareResult.NO_MATCH
        result_tuple = (
            result[CompareResult.IDENTICAL],
            result[CompareResult.A_BETTER],
            result[CompareResult.B_BETTER],
            result[CompareResult.DIFFERENT],
        )
        match result_tuple:
            case (i, 0, 0, 0):
                return ParentCompareResult.IDENTICAL
            case (i, a, 0, 0):
                return ParentCompareResult.A_BETTER
            case (i, 0, b, 0):
                return ParentCompareResult.B_BETTER
            case (i, a, b, 0):
                return ParentCompareResult.COMPATIBLE
            case _:
                return ParentCompareResult.INCOMPATIBLE


    def current_coordinates_with_accuracy(self) -> tuple[amath.ScienceNumber, amath.ScienceNumber, ScienceDateTime]:
        current = self.current_record.record
        self.require_quality(current.coordinates.get("Latitude", None), required_quality=RequiredQuality.GOOD_VALUE_WITH_UNITS | RequiredQuality.IS_NUMERIC)
        self.require_quality(current.coordinates.get("Longitude", None), required_quality=RequiredQuality.GOOD_VALUE_WITH_UNITS | RequiredQuality.IS_NUMERIC)
        self.require_quality(current.coordinates.get("Time", None), required_quality=RequiredQuality.GOOD_VALUE_WITH_UNITS | RequiredQuality.IS_DATETIME)
        return (
            current.coordinates["Latitude"].to_scinum(),
            current.coordinates["Longitude"].to_scinum(),
            current.coordinates["Time"].to_scidate()
        )

    def search_kwargs(self) -> dict[str, t.Any]:
        dt, dx = self.search_windows()
        lat, lon, time = self.current_coordinates_with_accuracy()
        min_lat, max_lat = self.extend_range(lat, dx)
        min_lon, max_lon = self.extend_range(lon, dx)
        min_t, max_t = self.extend_time_range(time, dt)
        return {
            'platform_uuid': self.get_current_platform_id(),
            'min_latitude': min_lat,
            'max_latitude': max_lat,
            'min_longitude': min_lon,
            'max_longitude': max_lon,
            'start_time': min_t,
            'end_time': max_t
        }

    def extend_time_range(self, value: ScienceDateTime, dt: amath.BasicNumber) -> tuple[AwareDateTime, AwareDateTime]:
        min_t, max_t = value.range()
        return (
            min_t - datetime.timedelta(seconds=float(dt)),
            max_t + datetime.timedelta(seconds=float(dt)),
        )

    def extend_range(self, value: amath.ScienceNumber, dx: amath.BasicNumber, sigma: amath.BasicNumber = 2) -> tuple[float, float]:
        min_v, max_v = value.range(sigma)
        return (
            amath.sub(min_v, dx),
            amath.add(max_v, dx)
        )

    def search_windows(self) -> tuple[int | float, int | float]:
        time_window, distance_window = None, None
        platform = self.get_current_platform()
        if platform is not None:
            time_window = platform.dedupe_time_window
            distance_window = platform.dedupe_distance_window
        return (
            self.DEFAULT_TIME_WINDOW if time_window is None else time_window,
            self.DEFAULT_DISTANCE_WINDOW if distance_window is None else distance_window
        )

    def compare_value(self, a: t.Any, b: t.Any) -> CompareResult:
        if isinstance(a, str):
            if isinstance(b, str):
                return self.compare_str_str(a, b)
            elif isinstance(b, (float, int)):
                return self.compare_str_float(a, b)
        elif isinstance(a, float):
            if isinstance(b, str):
                return self.compare_str_float(b, a)
            elif isinstance(b, (float, int)):
                return self.compare_float_float(a, b)
        raise TypeError("unsupported type")

    def compare_str_str(self, a: str, b: str) -> CompareResult:
        if a == b:
            return CompareResult.IDENTICAL
        else:
            return CompareResult.DIFFERENT

    def compare_str_float(self, a: str, b: float | int) -> CompareResult:
        try:
            x = float(a)
            return self.compare_float_float(x, b)
        except (TypeError, ValueError):
            return CompareResult.DIFFERENT

    def compare_float_float(self, a : float | int, b: float | int) -> CompareResult:
        if amath.is_close(a, b):
            return CompareResult.IDENTICAL
        else:
            return CompareResult.DIFFERENT

    def compare_parameter(self, a: SingleElement, b: SingleElement) -> CompareResult:
        param_a = a.to_scinum()
        param_b = b.to_scinum()
        try:
            if param_a.units is not None and param_b.units is not None and param_a.units != param_b.units:
                param_b = param_b.convert(param_a.units)
            if not param_a.is_compatible(param_b):
                return CompareResult.DIFFERENT
            std_dev_diff = amath.sub(param_a.std_dev, param_b.std_dev)
            if amath.is_close(std_dev_diff, 0, abs_tol=amath.NumberString("1e-9")):
                return CompareResult.IDENTICAL
            elif amath.gt(std_dev_diff, 0):
                return CompareResult.B_BETTER
            else:
                return CompareResult.A_BETTER
        except UnitError:
            return CompareResult.DIFFERENT

    def compare_datetimes(self, a: SingleElement, b: SingleElement) -> CompareResult:
        range_a = a.to_scidate().range()
        range_b = b.to_scidate().range()
        if not dates_overlap(
            *range_a,
            *range_b
        ):
            return CompareResult.DIFFERENT
        else:
            diff_a = (range_a[1] - range_a[0]).total_seconds()
            diff_b = (range_b[1] - range_b[0]).total_seconds()
            if diff_a > diff_b:
                return CompareResult.B_BETTER
            elif diff_a < diff_b:
                return CompareResult.A_BETTER
            else:
                return CompareResult.IDENTICAL

    def compare_single_element(self, a: SingleElement | None, b: SingleElement | None) -> t.Iterable[CompareResult]:
        if a is None or a.is_empty():
            if b is None or b.is_empty():
                yield CompareResult.IDENTICAL
            else:
                yield CompareResult.B_BETTER
        elif b is None or b.is_empty():
            yield CompareResult.A_BETTER
        elif a.is_empty() or b.is_science_number():
            yield self.compare_parameter(a, b)
        elif a.is_iso_datetime() and b.is_iso_datetime():
            yield self.compare_datetimes(a, b)
        else:
            yield self.compare_value(a, b)
        if a is not None and b is not None:
            yield from self.compare_element_map(a.metadata, b.metadata)

    def compare_records(self, a: list[ChildRecord], b: list[ChildRecord]):
        if len(a) == 0 and len(b) == 0:
            yield CompareResult.IDENTICAL
        else:
            for rec_a, rec_b in self.pair_up_records(a, b):
                yield from self.compare_record(rec_a, rec_b)

    def pair_up_records(self, a: list[ChildRecord], b: list[ChildRecord]) -> t.Iterable[tuple[ChildRecord | None, ...]]:
        for paired_results in pair_up_records(a, b):
            yield tuple(*(x for x, _ in paired_results))

    def compare_recordset(self, a: RecordSet | None, b: RecordSet | None) -> t.Iterable[CompareResult]:
        if a is None:
            if b is None:
                yield CompareResult.IDENTICAL
            else:
                yield CompareResult.B_BETTER
        elif b is None:
            yield CompareResult.A_BETTER
        else:
            yield from self.compare_records(list(a.records.iterate_with_load()), list(b.records.iterate_with_load()))
            yield from self.compare_element_map(a.metadata, b.metadata)

    def compare_multi_recordsets(self, a: dict[int, RecordSet], b: dict[int, RecordSet]) -> t.Iterable[CompareResult]:
        rs_a_list = [*a.values()]
        rs_b_list = [*b.values()]
        for rs_a, rs_b in pair_up_recordsets(rs_a_list, rs_b_list):
            yield from self.compare_recordset(rs_a[0], rs_b[0])

    def best_result(self, *options: list[CompareResult]) -> t.Iterable[CompareResult]:
        best_option, best_stat = None, None
        for option in options:
            stats = self._build_result_stats(option)
            if self.is_better(stats, best_stat):
                best_option = option
                best_stat = stats
        if best_option is not None:
            yield from best_option

    def is_better(self, stats_new: dict[CompareResult, float | int], stats_old: dict[CompareResult, float | int] | None) -> bool:
        if stats_old is None:
            return True
        identical_diff = stats_new[CompareResult.IDENTICAL] - stats_old[CompareResult.IDENTICAL]
        if identical_diff >= 0.01:
            return True
        elif identical_diff <= -0.01:
            return False
        else:
            a_or_b_new = stats_new[CompareResult.A_BETTER] + stats_new[CompareResult.B_BETTER]
            a_or_b_old = stats_old[CompareResult.A_BETTER] + stats_old[CompareResult.B_BETTER]
            return a_or_b_new > a_or_b_old

    def _build_result_stats(self, option: t.Iterable[CompareResult]) -> dict[CompareResult, float | int]:
        results: dict[CompareResult, int | float] = defaultdict(lambda: 0)
        total = 0
        for res in option:
            if res not in results:
                results[res] = 0
            results[res] += 1
            total += 1
        if total > 0:
            for res in results:
                results[res] = results[res] / total
        return results

    def compare_multi_element(self, a: AbstractElement, b: AbstractElement) -> t.Iterable[CompareResult]:
        for sub_a, sub_b in pair_up_single_elements(
            [x for x in a.all_values()],
            [x for x in b.all_values()]
        ):
            yield from self.compare_single_element(sub_a[0], sub_b[0])

    def unique_pair_indexes(self, options: list[int]) -> t.Iterable[list[int]]:
        if len(options) == 1:
            yield options
        elif len(options) > 1:
            for k in options:
                for lst in self.unique_pair_indexes([z for z in options if z != k]):
                    yield [k, *lst]

    def unique_options[T](self, a: list[T | None], b: list[T | None]) -> t.Iterable[list[tuple[T | None, T | None]]]:
        for b_indexes in self.unique_pair_indexes([x for x in range(0, max(len(a), len(b)))]):
            yield [
                (a[a_idx], b[b_idx])
                for a_idx, b_idx in enumerate(b_indexes)
            ]

    def generate_multi_element_option(self, *pairs: tuple[AbstractElement | None, AbstractElement | None]) -> t.Iterable[CompareResult]:
        for a, b in pairs:
            yield from self.compare_element(a, b)

    def compare_element(self, a: AbstractElement | None, b: AbstractElement | None) -> t.Iterable[CompareResult]:
        if a is None or a.is_empty():
            if b is None or b.is_empty():
                yield CompareResult.IDENTICAL
            else:
                yield CompareResult.B_BETTER
        elif b is None or b.is_empty():
            yield CompareResult.A_BETTER
        elif isinstance(a, SingleElement) and isinstance(b, SingleElement):
            yield from self.compare_single_element(a, b)
        else:
            yield from self.compare_multi_element(a, b)

    def compare_element_map(self, a: ElementMap | None, b: ElementMap | None) -> t.Iterable[CompareResult]:
        if a is None and b is None:
            yield CompareResult.IDENTICAL
        elif a is None:
            yield CompareResult.B_BETTER
        elif b is None:
            yield CompareResult.A_BETTER
        else:
            for key in set(*a.keys(), *b.keys()):
                info = self.ontology.info(key)
                if info is not None and info.ignore_for_dedupe:
                    continue
                yield from self.compare_element(a.get(key, None), b.get(key, None))

    def compare_record_sets(self, a: dict[int, RecordSet] | None, b: dict[int, RecordSet] | None) -> t.Iterable[CompareResult]:
        if a is None or len(a) == 0:
            if b is None or len(b) == 0:
                yield CompareResult.IDENTICAL
            else:
                yield CompareResult.A_BETTER
        elif b is None or len(b) == 0:
            yield CompareResult.A_BETTER
        else:
            yield from self.compare_multi_recordsets(a, b)

    def compare_record_map(self, a: RecordMap | None, b: RecordMap | None) -> t.Iterable[CompareResult]:
        if a is None and b is None:
            yield CompareResult.IDENTICAL
        elif a is None:
            yield CompareResult.B_BETTER
        elif b is None:
            yield CompareResult.A_BETTER
        else:
            for key in set(*a.record_sets.keys(), *b.record_sets.keys()):
                yield from self.compare_record_sets(a.record_sets.get(key, None), b.record_sets.get(key, None))

    def compare_record(self, a: BaseRecord | None, b: BaseRecord | None) -> t.Iterable[CompareResult]:
        if a is None:
            if b is None:
                yield CompareResult.IDENTICAL
            else:
                yield CompareResult.B_BETTER
        elif b is None:
            yield CompareResult.A_BETTER
        else:
            yield from self.compare_element_map(a.parameters, b.parameters)
            yield from self.compare_element_map(a.coordinates, b.coordinates)
            yield from self.compare_element_map(a.metadata, b.metadata)
            yield from self.compare_record_map(a.subrecords, b.subrecords)
