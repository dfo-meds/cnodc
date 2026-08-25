import datetime
import enum
import typing as t

from autoinject import injector

import medsutil.math as amath
from medsutil.ocproc2 import SingleElement, ElementMap, AbstractElement, OCProc2Ontology, BaseRecord, \
    RecordMap, RecordSet, ParentRecord, ChildRecord
from medsutil.ocproc2.operations import SetRelationships, SetQualityCheck
from medsutil.ocproc2.util import pair_up_records, dates_overlap, pair_up_recordsets, \
    pair_up_single_elements
from medsutil.units.structures import UnitError
from nodb.observations import QualityCheckFlags, DataMode
from pipeman.programs.qc.base import QualityController


class CompareResult(enum.Enum):
    A_BETTER = 'A'
    B_BETTER = 'B'
    IDENTICAL = 'C'
    DIFFERENT = 'D'


class RelationshipAction(enum.Enum):

    A_IS_BROADCAST = "A_IS_BROADCAST"
    B_IS_BROADCAST = "B_IS_BROADCAST"
    A_IS_DELAYED_MODE = "A_IS_DELAYED"
    B_IS_DELAYED_MODE = "B_IS_DELAYED"
    SUPPLEMENTAL = "SUPPLEMENTAL"
    A_IS_DUPLICATE = "A_IS_DUPLICATE"
    B_IS_DUPLICATE = "B_IS_DUPLICATE"
    A_IS_CORRECTION = "A_IS_CORRECTION"
    B_IS_CORRECTION = "B_IS_CORRECTION"
    MERGE = "MERGE"
    A_WAS_MERGED_FROM = "A_WAS_MERGED"

    def is_duplicate(self) -> bool:
        return self in {
            RelationshipAction.A_IS_DUPLICATE,
            RelationshipAction.A_IS_BROADCAST,
            RelationshipAction.B_IS_CORRECTION,
            RelationshipAction.B_IS_DELAYED_MODE,
        }

    def is_update(self) -> bool:
        return self in {
            RelationshipAction.B_IS_DUPLICATE,
            RelationshipAction.B_IS_BROADCAST,
            RelationshipAction.A_IS_CORRECTION,
            RelationshipAction.A_IS_DELAYED_MODE,
        }

    def is_reviewable(self) -> bool:
        return self in {
            RelationshipAction.A_IS_DUPLICATE,
            RelationshipAction.B_IS_DUPLICATE,
            RelationshipAction.MERGE
        }

    @staticmethod
    def encode_actions(actions: list[tuple[str, datetime.date, RelationshipAction, bool]]) -> list[list[str | bool]]:
        return [
            [x[0], x[1].isoformat(), x[2].value, x[3]]
            for x in actions
        ]

    @staticmethod
    def decode_actions(actions: list[list[str | bool]]) -> list[tuple[str, datetime.date, RelationshipAction, bool]]:
        return [
            (str(x[0]), datetime.date.fromisoformat(str(x[1])), RelationshipAction(x[2]), bool(x[3]))
            for x in actions
        ]


class RelationshipCandidate:

    def __init__(self,
                 observation_uuid: str,
                 received_date: datetime.date,
                 source_file_uuid: str,
                 record: ParentRecord,
                 data_mode: DataMode,
                 quality_checks: int):
        self.observation_uuid = observation_uuid
        self.received_date = received_date
        self.source_file_uuid = source_file_uuid
        self.record = record
        self.data_mode = data_mode
        self.quality_checks = quality_checks


class NODBDuplicateCheck(QualityController):

    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            test_name="duplicate_check",
            test_version="1.0",
            test_protocol="nodb",
            **kwargs
        )

    def run(self):
        if self._record_quality_flags & QualityCheckFlags.DEDUPLICATE:
            self.qc_pass()

        relationships = self.build_relationship_candidate_list()
        reviewable = any(x[3] for x in relationships)
        self.add_record_action(
            SetRelationships(
                relationships=RelationshipAction.encode_actions(relationships)
            ),
            reviewable
        )
        self.add_record_action(
            SetQualityCheck(check_no=QualityCheckFlags.DEDUPLICATE),
            reviewable
        )

    def build_relationship_candidate_list(self) -> list[tuple[str, datetime.date, RelationshipAction, bool]]:
        results: list[tuple[str, datetime.date, RelationshipAction, bool]] = []
        cached = {
            "obs_id": self.current_record.record.metadata.best("CNODCObservationID", default=None, coerce=str),
            "sub_id": self.current_record.record.metadata.best("CNODCObservationSubID", default=None, coerce=str),
            "data_mode": self.current_record.record.metadata.best("CNODCDataMode", default=None, coerce=str),
            "quality_checks": self.current_record.record.metadata.best("CNODCQualityFlags", default=None, coerce=int),
            "is_broadcast": self.current_record.record.metadata.best("CNODCIsBroadcast", default=None, coerce=int),
            "source_version": self.current_record.record.metadata.best("CNODCSourceFileVersion", default=None, coerce=int),
            "source_file": self.current_record.record.metadata.best("CNODCSourceFile", default=None, coerce=str),
        }

        merged_from = self.current_record.record.metadata.best("CNODCMergedFrom", default=None)
        omit_records: set[tuple[str, datetime.date]] = set()
        if merged_from and isinstance(merged_from, list):
            for item in merged_from:
                if isinstance(item, list) and len(item) >= 2:
                    key: tuple[str, datetime.date] = str(item[0]), datetime.date.fromisoformat(str(item[1]))
                    if key in omit_records:
                        continue
                    omit_records.add(key)
                    results.append((
                        key[0],
                        key[1],
                        RelationshipAction.A_WAS_MERGED_FROM,
                        False
                    ))

        for candidate in self.find_relationship_candidates(omit_records):
            obs_id_match: bool = False
            if cached["obs_id"] is not None:
                candidate_obs_id = candidate.record.metadata.best("CNODCObservationID", default=None)
                if candidate_obs_id == cached["obs_id"]:
                    obs_id_match = True
            result = self._compare_candidate(candidate, cached)
            results.append((candidate.observation_uuid, candidate.received_date, result, obs_id_match or result.is_reviewable()))
        return results

    def _compare_candidate(self, candidate: RelationshipCandidate, cached: dict[str, t.Any]) -> RelationshipAction:

        # different sensor packages launched together (e.g. bottle vs CTD on a rosette)
        sub_id = candidate.record.metadata.best("CNODCObservationSubID", default=None, coerce=str)
        if sub_id and cached["sub_id"] and sub_id != cached["sub_id"]:
            return RelationshipAction.SUPPLEMENTAL

        # different data modes
        data_mode = candidate.record.metadata.best("CNODCDataMode", default=None, coerce=str)
        if data_mode == "DM" and cached["data_mode"] == "RT":
            return RelationshipAction.B_IS_DELAYED_MODE
        if data_mode == "RT" and cached["data_mode"] == "DM":
            return RelationshipAction.A_IS_DELAYED_MODE

        # one is a broadcast returning and the other is not
        is_broadcast = candidate.record.metadata.best("CNODCIsBroadcast", default=None, coerce=int)
        if is_broadcast and not cached["is_broadcast"]:
            return RelationshipAction.A_IS_BROADCAST
        if cached["is_broadcast"] and not is_broadcast:
            return RelationshipAction.B_IS_BROADCAST

        # a correction from the source file
        source_file = candidate.record.metadata.best("CNODCSourceFileIdentifier", default=None, coerce=str)
        source_version = candidate.record.metadata.best("CNODCSourceFileVersion", default=None, coerce=int)
        if source_file and cached["source_file"] and source_file == cached["source_file"] and source_version is not None and cached["source_version"] is not None:
            if source_version > cached["source_version"]:
                return RelationshipAction.B_IS_CORRECTION
            elif source_version < cached["source_version"]:
                return RelationshipAction.A_IS_CORRECTION

        # we're going to do a comparison
        results = self.compare_results(self.current_record.record, candidate.record)
        if results.get(CompareResult.DIFFERENT, 0) > 0:
            return RelationshipAction.MERGE

        a_better = results.get(CompareResult.A_BETTER, 0)
        b_better = results.get(CompareResult.B_BETTER, 0)
        if a_better > 0 and b_better > 0:
            return RelationshipAction.MERGE
        elif b_better > 0:
            return RelationshipAction.B_IS_DUPLICATE
        else:
            return RelationshipAction.A_IS_DUPLICATE

    def compare_results(self, a: ParentRecord, b: ParentRecord) -> dict[CompareResult, int]:
        results = {}
        for res in self.compare_record(a, b):
            if res not in results:
                results[res] = 0
            results[res] += 1
        return results

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

    def compare_multi_element(self, a: AbstractElement, b: AbstractElement) -> t.Iterable[CompareResult]:
        for sub_a, sub_b in pair_up_single_elements(
            [x for x in a.all_values()],
            [x for x in b.all_values()]
        ):
            yield from self.compare_single_element(sub_a[0], sub_b[0])

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

    def compare_record_sets(self, a: dict[int, RecordSet] | None, b: dict[int, RecordSet] | None) -> t.Iterable[
        CompareResult]:
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

    def compare_records(self, a: list[ChildRecord], b: list[ChildRecord]):
        if len(a) == 0 and len(b) == 0:
            yield CompareResult.IDENTICAL
        else:
            for rec_a, rec_b in self.pair_up_records(a, b):
                yield from self.compare_record(rec_a, rec_b)

    def pair_up_records(self, a: list[ChildRecord], b: list[ChildRecord]) -> t.Iterable[tuple[ChildRecord | None, ...]]:
        for paired_results in pair_up_records(a, b):
            yield tuple(*(x for x, _ in paired_results))

    def search_kwargs(self) -> dict[str, t.Any]:
        kwargs: dict[str, float | int | str | None] = {
            'platform_uuid': self.get_current_platform_id(),
        }
        if "Latitude" in self.current_record.record.coordinates:
            lat = self.current_record.record.coordinates["Latitude"].to_scinum()
            kwargs["min_latitude"], kwargs["max_latitude"] = lat.range()
        if "Longitude" in self.current_record.record.coordinates:
            lon = self.current_record.record.coordinates["Longitude"].to_scinum()
            kwargs["min_longitude"], kwargs["max_longitude"] = lon.range()
        if "Time" in self.current_record.record.coordinates:
            time = self.current_record.record.coordinates["Time"].to_scidate()
            kwargs["min_time"], kwargs["max_time"] = time.range()
        return kwargs

    def find_relationship_candidates(self, handled: t.Iterable[tuple[str, datetime.date]] | None = None) -> t.Iterable[RelationshipCandidate]:
        search_parameters = self.search_kwargs()
        observation_identifier: str | None = self.current_record.record.metadata.best("CNODCObservationID", default=None, coerce=str)

        seen: set[tuple[str, datetime.date]] = set()
        if handled is not None:
            seen.update(handled)
        if observation_identifier:
            for working in self.searcher.related_working_records(observation_identifier=observation_identifier):
                key = str(working.working_uuid), t.cast(datetime.date, working.received_date)
                if key in seen:
                    continue
                yield RelationshipCandidate(
                    str(working.working_uuid),
                    t.cast(datetime.date, working.received_date),
                    str(working.source_file_uuid),
                    t.cast(ParentRecord, working.record),
                    working.data_mode,
                    working.quality_checks
                )

        for working in self.searcher.geosearch_working_records(
                **search_parameters,
                quality_checks=QualityCheckFlags.DEDUPLICATE
        ):
            key = str(working.working_uuid), t.cast(datetime.date, working.received_date)
            if key in seen:
                continue
            seen.add(key)
            yield RelationshipCandidate(
                str(working.working_uuid),
                t.cast(datetime.date, working.received_date),
                str(working.source_file_uuid),
                t.cast(ParentRecord, working.record),
                working.data_mode,
                working.quality_checks
            )

        if observation_identifier:
            for obs_data in self.searcher.related_observations(observation_identifier=observation_identifier):
                key = str(obs_data.obs_uuid), obs_data.received_date
                if key in seen:
                    continue
                seen.add(key)
                yield RelationshipCandidate(
                    obs_data.obs_uuid,
                    obs_data.received_date,
                    obs_data.source_file_uuid,
                    t.cast(ParentRecord, obs_data.record),
                    obs_data.data_mode,
                    obs_data.quality_checks
                )

        for obs_data in self.searcher.geosearch_observations(**search_parameters):
            key = str(obs_data.obs_uuid), obs_data.received_date
            if key in seen:
                continue
            seen.add(key)
            yield RelationshipCandidate(
                obs_data.obs_uuid,
                obs_data.received_date,
                obs_data.source_file_uuid,
                t.cast(ParentRecord, obs_data.record),
                obs_data.data_mode,
                obs_data.quality_checks
            )
