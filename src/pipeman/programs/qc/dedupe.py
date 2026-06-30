import datetime
import enum
import typing as t
from collections import defaultdict

from autoinject import injector

import medsutil.math as amath
from medsutil.awaretime import ScienceDateTime, AwareDateTime
from medsutil.ocproc2 import SingleElement, ElementMap, AbstractElement, MultiElement, OCProc2Ontology, BaseRecord, \
    RecordMap, RecordSet, ParentRecord
from medsutil.ocproc2.refs import SingleElementRef
from medsutil.ocproc2.util import RequiredQuality
from medsutil.units.structures import UnitError
from pipeman.programs.qc.base import QualityController


class CompareResult(enum.Enum):
    A_BETTER = 'A'
    B_BETTER = 'B'
    IDENTICAL = 'C'
    DIFFERENT = 'D'


class ParentCompareResult(enum.Enum):

    MARK_DUPLICATE = 'D'
    MARK_DUPLICATE_UNLESS_REPLACEMENT = 'D2'
    MARK_OTHER_DUPLICATE = 'O'
    MERGE = 'M'

    REVIEW = 'R'

    NO_MATCH = 'N'


class DeduplicateAction(enum.Enum):
    MARK_DUPLICATE = 'mark_duplicate'
    MARK_OTHER_DUPLICATE = 'mark_other_duplicate'
    AUTOMERGE = 'automerge'
    MANUAL_MERGE = 'manual_merge'
    REVIEW = 'review'


class NODBDuplicateCheck(QualityController):

    ontology: OCProc2Ontology = None

    DEFAULT_TIME_WINDOW = 900  # seconds
    DEFAULT_DISTANCE_WINDOW = 5000  # m
    PROBABLE_THRESHOLD = 0.8  # fraction

    @injector.construct
    def __init__(self):
        super().__init__(test_name='nodb_dupe_check', test_version='1.0')

    def run(self):
        dupe_id_ref = t.cast(SingleElementRef, self.current_record.setdefault_metadata_ref("CNODCDuplicateID"))
        relations = t.cast(SingleElementRef, self.current_record.parameter_ref("CNODCRelatedRecords"))
        if self._set_qc_flag & 1 or not any(
            x is None
            or x.element.is_empty()
            or x.element.metadata.best('Quality', coerce=int, default=0) not in (1, 9)
            or not self.ref_exists(x.element)
            for x in (dupe_id_ref, relations)
        ):
            self.qc_pass()

        dedupe_results, diff_level_results = self.search_for_duplicates()
        if len(diff_level_results) == 0:
            self.current_record.record.metadata['CNODCRelatedRecords'] = None
            self.current_record.record.metadata['CNODCRelatedRecords'].metadata['Quality'] = 9
        else:
            elements = []
            for level in diff_level_results:
                for (obs_uuid, obs_date), result in diff_level_results[level].items():
                    se = SingleElement(f"{obs_date.isoformat()}/{obs_uuid}")
                    se.metadata['RelationshipType'] = 'related'
                    if result in (ParentCompareResult.MARK_DUPLICATE, ParentCompareResult.MARK_OTHER_DUPLICATE, ParentCompareResult.MERGE):
                        se.metadata['Quality'] = 1
                    else:
                        se.metadata['WorkingQuality'] = 0
                    elements.append(se)
            self.current_record.record.metadata['CNODCRelatedRecords'] = MultiElement(elements)


        # no duplicates found, mark test as good.
        self.set_qc_flag(1)  # indicates dedupe has been done
        if len(dedupe_results) == 0:
            self.set_duplicate_list(None)
            dupe_id_ref.element.value = None
            dupe_id_ref.element.metadata["Quality"] = 9
            self.qc_pass()
        elif len(dedupe_results) > 1:
            self.set_duplicate_list(dedupe_results)
            self.clear_duplicate(dupe_id_ref)
        else:
            (source_uuid, obs_uuid, obs_date), dedupe_result = next(iter(dedupe_results.items()))
            action = self.get_dedupe_action(dedupe_result, obs_date, source_uuid)
            self.set_duplicate(dupe_id_ref, obs_uuid, obs_date, action)

    def get_dedupe_action(self,
                          dedupe_result: ParentCompareResult,
                          obs_date: datetime.date,
                          source_uuid: str | None) -> DeduplicateAction:
        match dedupe_result:
            case ParentCompareResult.MARK_DUPLICATE:
                return DeduplicateAction.MARK_DUPLICATE
            case ParentCompareResult.MARK_OTHER_DUPLICATE:
                return DeduplicateAction.MARK_OTHER_DUPLICATE
            case ParentCompareResult.MARK_DUPLICATE_UNLESS_REPLACEMENT:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return DeduplicateAction.MARK_OTHER_DUPLICATE
                else:
                    return DeduplicateAction.MARK_DUPLICATE
            case ParentCompareResult.MERGE:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return DeduplicateAction.MARK_OTHER_DUPLICATE
                elif self.does_source_file_replace_current(source_uuid, obs_date):
                    return DeduplicateAction.MARK_DUPLICATE
                else:
                    return DeduplicateAction.AUTOMERGE
            case ParentCompareResult.REVIEW:
                if self.does_current_replace_source_file(source_uuid, obs_date):
                    return DeduplicateAction.MARK_OTHER_DUPLICATE
                elif self.does_source_file_replace_current(source_uuid, obs_date):
                    return DeduplicateAction.MARK_DUPLICATE
                else:
                    return DeduplicateAction.REVIEW
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

    def clear_duplicate(self, dupe_id_ref: SingleElementRef):
        dupe_id_ref.element.value = None
        if 'CNODCDuplicateType' in dupe_id_ref.element.metadata:
            del dupe_id_ref.element.metadata['CNODCDuplicateType']
        self.recommend_for_review('dedupe', dupe_id_ref)
        self.report_qc_error("multiple_for_review")

    def set_duplicate(self, dupe_id_ref: SingleElementRef, obs_uuid: str, obs_date: datetime.date, action: DeduplicateAction):
        self.set_duplicate_list(None)
        dupe_id_ref.element.value = f"{obs_date.isoformat()}/{obs_uuid}"
        dupe_id_ref.element.metadata["CNODCDuplicateType"] = action.value
        if action is DeduplicateAction.REVIEW or action is DeduplicateAction.MANUAL_MERGE:
            self.recommend_for_review("dedupe", dupe_id_ref, 1)
            self.report_qc_error("duplicate_for_review")
        else:
            dupe_id_ref.element.metadata["Quality"] = 1
            self.qc_pass()

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

    def search_for_duplicates(self) -> tuple[dict[tuple[str | None, str, datetime.date], ParentCompareResult], dict[str, dict[tuple[str | None, str, datetime.date], ParentCompareResult]]]:
        diff_level_matches: dict[str, dict[tuple[str | None, str, datetime.date], ParentCompareResult]] = {}
        potential_matches: dict[tuple[str | None, str, datetime.date], ParentCompareResult] = {}
        self_level = self.current_record.record.metadata.best("CNODCLevel", coerce=str, default="UNKNOWN")
        def _process_record(source_uuid: str | None, record_uuid: str, record_date: datetime.date, record: ParentRecord | None):
            if record is None: return
            match_result = self.compare_parent_records(self.current_record.record, record)
            if match_result is ParentCompareResult.NO_MATCH: return
            record_level = record.metadata.best("CNODCLevel", coerce=str, default="UNKNOWN")
            if record_level == self_level:
                potential_matches[(source_uuid, record_uuid, record_date)] = match_result
            else:
                if record_level not in diff_level_matches:
                    diff_level_matches[record_level] = {}
                diff_level_matches[record_level][(source_uuid, record_uuid, record_date)] = match_result
        search_parameters = self.search_kwargs()
        for working in self.searcher.geosearch_working_records(**search_parameters):
            _process_record(
                working.source_file_uuid,
                t.cast(str, working.working_uuid),
                t.cast(datetime.date, working.received_date),
                working.record
            )
        for obs_data in self.searcher.geosearch_working_records(**search_parameters):
            if (obs_data.source_file_uuid, obs_data.working_uuid, obs_data.received_date) in potential_matches:
                continue
            _process_record(
                obs_data.source_file_uuid,
                t.cast(str, obs_data.working_uuid),
                t.cast(datetime.date, obs_data.received_date),
                obs_data.record
            )
        return (
            self.reduce_matches(potential_matches),
            {
                k: self.reduce_matches(diff_level_matches[k])
                for k in diff_level_matches.keys()
            }
        )

    def reduce_matches(self, potential_matches: dict[tuple[str | None, str, datetime.date], ParentCompareResult]) -> dict[tuple[str | None, str, datetime.date], ParentCompareResult]:
        ordered_results = [
            (ParentCompareResult.MARK_DUPLICATE,),
            (ParentCompareResult.MARK_DUPLICATE_UNLESS_REPLACEMENT,),
            (ParentCompareResult.MARK_OTHER_DUPLICATE,),
            (ParentCompareResult.MERGE,),
            (ParentCompareResult.REVIEW,),
        ]
        for allow_values in ordered_results:
            if any(x in allow_values for x in potential_matches.values()):
                return {
                    k: v
                    for k, v in potential_matches.items()
                    if v in allow_values
                }
        return potential_matches

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
                return ParentCompareResult.MARK_DUPLICATE
            case (i, a, 0, 0):
                return ParentCompareResult.MARK_OTHER_DUPLICATE
            case (i, 0, a, 0):
                return ParentCompareResult.MARK_DUPLICATE_UNLESS_REPLACEMENT
            case (i, a, b, 0):
                return ParentCompareResult.MERGE
            case _:
                return ParentCompareResult.REVIEW


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
        if not self.dates_overlap(
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

    def compare_single_element(self, a: SingleElement, b: SingleElement) -> t.Iterable[CompareResult]:
        # united and uncertainty values need special checks
        if a.metadata.has_value('Units') or b.metadata.has_value('Units') or a.metadata.has_value('Uncertainty') or b.metadata.has_value('Uncertainty'):
            yield self.compare_parameter(a, b)
        elif a.is_iso_datetime() and b.is_iso_datetime():
            yield self.compare_datetimes(a, b)
        else:
            yield self.compare_value(a, b)
        yield from self.compare_element_map(a.metadata, b.metadata)

    def compare_records(self, a: list[BaseRecord], b: list[BaseRecord]):
        if len(a) == 0 and len(b) == 0:
            yield CompareResult.IDENTICAL
        else:
            for rec_a, rec_b in self.pair_up_records(a, b):
                yield from self.compare_record(rec_a, rec_b)

    def pair_up_records(self, a: list[BaseRecord], b: list[BaseRecord]) -> t.Iterable[tuple[BaseRecord | None, BaseRecord | None]]:
        used = set()
        for rec_a in a:
            for idx, rec_b in enumerate(b):
                if idx in used:
                    continue
                if self.record_coordinates_match(rec_a, rec_b):
                    used.add(idx)
                    yield rec_a, rec_b
                    break
            else:
                yield rec_a, None
        for idx, rec_b in enumerate(b):
            if idx not in used:
                yield None, rec_b

    def dates_overlap(self,
                      min_a: AwareDateTime,
                      max_a: AwareDateTime,
                      min_b: AwareDateTime,
                      max_b: AwareDateTime) -> bool:
        if min_a > max_b or min_b > max_a:
            return False
        if max_a < min_b or max_b < min_a:
            return False
        return True

    def record_coordinates_match(self, a: BaseRecord, b: BaseRecord) -> bool:
        for k in set(*a.coordinates.keys(), *b.coordinates.keys()):
            if k not in a.coordinates or k not in b.coordinates:
                return False
            if a.coordinates[k].is_iso_datetime() and b.coordinates[k].is_iso_datetime():
                if not self.dates_overlap(
                    *a.coordinates[k].to_scidate().range(),
                    *b.coordinates[k].to_scidate().range()
                ):
                    return False
            elif a.coordinates[k].is_numeric() and b.coordinates[k].is_numeric():
                if not a.coordinates[k].to_scinum().is_compatible(b.coordinates[k].to_scinum()):
                    return False
            else:
                return False
        return True

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

    def compare_recordsets(self, *options: tuple[RecordSet | None, RecordSet | None]) -> t.Iterable[CompareResult]:
        for rs_a, rs_b in options:
            yield from self.compare_recordset(rs_a, rs_b)

    def compare_multi_recordsets(self, a: dict[int, RecordSet], b: dict[int, RecordSet]) -> t.Iterable[CompareResult]:
        rs_a_list = [*a.values()]
        rs_b_list = [*b.values()]
        yield from self.best_result(*(
            [x for x in self.compare_recordsets(*option)]
            for option in self.unique_options(rs_a_list, rs_b_list)
        ))

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
        if isinstance(a, SingleElement) and isinstance(b, MultiElement):
            yield from self.best_result(*(
                [z for z in self.compare_element(a, x)]
                for x in b.values()
            ))
        elif isinstance(a, MultiElement) and isinstance(b, SingleElement):
            yield from self.best_result(*(
                [z for z in self.compare_element(x, b)]
                for x in a.values()
            ))
        elif isinstance(a, MultiElement) and isinstance(b, MultiElement):
            yield from self.best_result(*(
                [x for x in self.generate_multi_element_option(*option)]
                for option in
                self.unique_options(a.values(), b.values())
            ))
        else:
            raise TypeError("this shouldn't happen")

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
