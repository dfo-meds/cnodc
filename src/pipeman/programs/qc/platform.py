from medsutil.awaretime import AwareDateTime
import typing as t
from autoinject import injector

from medsutil.cached import LeastRecentCache
from nodb.observations import NODBPlatform
from medsutil.ocproc2.util import RequiredQuality
from pipeman.programs.qc.base import DeepDiveChecker
from medsutil.ocproc2.refs import ParentRecordRef
import medsutil.ocproc2 as ocproc2


class NODBPlatformCheck(DeepDiveChecker):

    @injector.construct
    def __init__(self, searcher_cls=None):
        super().__init__(
            'nodb_platform',
            '1.0',
            searcher_cls=searcher_cls,
            station_invariant=False,
            test_tags=['GTSPP_1.1']
        )
        self._lru_cache = LeastRecentCache()

    def parent_record_check(self, ref: ParentRecordRef):
        platform_ref = ref.setdefault_metadata_ref("CNODCPlatform")
        with self.review("valid_platform", platform_ref) as ctx:
            ctx.check_review_already_complete(RequiredQuality.NOT_FINAL | RequiredQuality.NOT_ERRONEOUS | RequiredQuality.GOOD_STRUCTURE)
            self.platform_check(platform_ref.element)

    def platform_check(self, element: ocproc2.AbstractElement):
        self.assert_is_instance(element, ocproc2.SingleElement, msg="multivalued_not_allowed")
        if not element.is_empty():
            self.assert_is_not_none(self.searcher.find_by_uuid(element.to_string()), msg="bad_platform_uuid")
            element.metadata['Quality'] = 1
            self._set_platform_candidates(None)
        else:
            self._assign_platform(t.cast(ocproc2.SingleElement, element))

    def _assign_platform(self, platform: ocproc2.SingleElement):
        platforms: list[str] = self._find_platform_matches(self.current_record.record)
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

    def _find_platform_matches(self, record: ocproc2.ParentRecord) -> list[str]:
        search_kwargs: dict[str, str | None | AwareDateTime] = {
            "platform_id": record.metadata.best("PlatformID", coerce=str, default=None),
            "platform_name": record.metadata.best("PlatformName", coerce=str, default=None),
            "wmo_id": record.metadata.best("WMOID", coerce=str, default=None),
            "wigos_id": record.metadata.best("WIGOSID", coerce=str, default=None),
        }
        self.assert_true(any(x is not None for x in search_kwargs.values()), msg="no_platform_ids")
        best_time = record.coordinates.ideal("Time")
        if best_time is not None and best_time.is_iso_datetime():
            search_kwargs["in_service_time"] = best_time.to_datetime()
        return self._get_platform_matches(search_kwargs)

    def _get_platform_matches(self, search_kwargs: dict[str, str | None | AwareDateTime]) -> list[str]:
        cache_str = ";".join(f"{k}={v}" for k, v in search_kwargs.items() if v is not None)
        return self._lru_cache.with_cache(
            cache_str,
            self._real_get_platform_matches,
            search_kwargs
        )

    def _real_get_platform_matches(self, search_kwargs) -> list[str]:
        raw_matches = [x for x in self.searcher.search(**search_kwargs)]
        if not raw_matches:
            return []
        resolved_matches = self._resolve_platform_matches(raw_matches)
        return list(set(p.platform_uuid for p in resolved_matches))

    def _resolve_platform_matches(self, matches: list[NODBPlatform]) -> list[NODBPlatform]:
        resolved_matches: list[NODBPlatform] = []
        for match in matches:
            resolved = self._resolve_platform_match(match)
            if resolved is not None:
                resolved_matches.append(resolved)
        return resolved_matches

    def _resolve_platform_match(self, platform: NODBPlatform | None) -> NODBPlatform | None:
        if platform is None or platform.map_to_uuid is None:
            return platform
        else:
            return self._resolve_platform_match(self.searcher.find_by_uuid(platform.map_to_uuid))
