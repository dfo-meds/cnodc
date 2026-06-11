from medsutil.awaretime import AwareDateTime
import typing as t
from autoinject import injector

from medsutil.cached import LeastRecentCache
from nodb.interface import NODB, NODBInstance
from nodb.observations import NODBPlatform
from pipeman.programs.qc.base import DeepDiveChecker, ParentRecordRef, review, ElementRef, SingleElementRef
import medsutil.ocproc2 as ocproc2


class NODBPlatformCheck(DeepDiveChecker):

    nodb: NODB

    @injector.construct
    def __init__(self):
        super().__init__(
            'nodb_platform',
            '1.0',
            station_invariant=False,
            test_tags=['GTSPP_1.1']
        )
        self._lru_cache = LeastRecentCache()

    def parent_record_check(self, ref: ParentRecordRef):
        self.platform_check(self.get_record_metadata_ref(ref, "CNODCPlatform", create_when_missing=True))

    @review("valid_platform", fail_flag=9)
    def platform_check(self, ref: ElementRef):
        if self.assert_is_instance(ref, SingleElementRef, msg="multivalued_not_allowed"):
            self._platform_check(t.cast(ocproc2.SingleElement, ref.element))

    def _platform_check(self, platform: ocproc2.SingleElement):
        with self.nodb as db:
            if not platform.is_empty():
                self.assert_is_not_none(NODBPlatform.find_by_uuid(db, platform.to_string()), msg="bad_platform_uuid")
                platform.metadata['Quality'] = 1
                self._set_platform_candidates(None)
            else:
                self._assign_platform(platform, db)

    def _assign_platform(self, platform: ocproc2.SingleElement, db: NODBInstance):
        platforms: list[str] = self._find_platform_matches(self.current_record.record, db)
        match len(platforms):
            case 0:
                self._set_platform_candidates(None)
                self.report_qc_error("no_platforms_found")
            case 1:
                self._set_platform_candidates(None)
                platform.value = platforms[0]
                platform.metadata['Quality'] = 1
            case _:
                self._set_platform_candidates(platforms)
                self.report_qc_error("many_platforms_found")

    def _set_platform_candidates(self, platforms: list[str] | None):
        if not platforms:
            if 'CNODCPlatformCandidates' in self.current_record.record.metadata:
                del self.current_record.record.metadata['CNODCPlatformCandidates']
        else:
            self.current_record.record.metadata['CNODCPlatformCandidates'] = platforms

    def _find_platform_matches(self, record: ocproc2.ParentRecord, db: NODBInstance) -> list[str]:
        search_kwargs: dict[str, str | None | AwareDateTime] = {
            "platform_id": record.metadata.best("PlatformID", coerce=str, default=None),
            "platform_name": record.metadata.best("PlatformName", coerce=str, default=None),
            "wmo_id": record.metadata.best("WMOID", coerce=str, default=None),
            "wigos_id": record.metadata.best("WIGOSID", coerce=str, default=None),
        }
        self.assert_true(any(x is not None for x in search_kwargs.values()))
        best_time = record.coordinates.ideal("Time")
        if best_time is not None and best_time.is_iso_datetime():
            search_kwargs["in_service_time"] = best_time.to_datetime()
        return self._get_platform_matches(search_kwargs, db)

    def _get_platform_matches(self, search_kwargs: dict[str, str | None | AwareDateTime], db: NODBInstance) -> list[str]:
        cache_str = ";".join(f"{k}={v}" for k, v in search_kwargs.items() if v is not None)
        return self._lru_cache.with_cache(
            cache_str,
            self._real_get_platform_matches,
            search_kwargs,
            db
        )

    def _real_get_platform_matches(self, search_kwargs, db) -> list[str]:
        raw_matches = [x for x in NODBPlatform.search(db, **search_kwargs)]
        if not raw_matches:
            return []
        resolved_matches = self._resolve_platform_matches(raw_matches, db)
        return list(set(p.platform_uuid for p in resolved_matches))

    def _resolve_platform_matches(self, matches: list[NODBPlatform], db: NODBInstance) -> list[NODBPlatform]:
        resolved_matches: list[NODBPlatform] = []
        for match in matches:
            resolved = self._resolve_platform_match(match, db)
            if resolved is not None:
                resolved_matches.append(resolved)
        return resolved_matches

    def _resolve_platform_match(self, platform: NODBPlatform | None, db: NODBInstance) -> NODBPlatform | None:
        if platform is None or platform.map_to_uuid is None:
            return platform
        else:
            return self._resolve_platform_match(NODBPlatform.find_by_uuid(db, platform.map_to_uuid), db)
