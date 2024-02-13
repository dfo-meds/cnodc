import datetime
from cnodc.ocproc2 import DataRecord
import cnodc.nodb.structures as structures
import typing as t
from cnodc.nodb import NODBControllerInstance
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest
import cnodc.ocproc2.structures as ocproc2


class NODBStationCheck(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__(
            'nodb_station_check',
            '1.0',
            station_invariant=False,
            test_tags=['GTSPP_1.1'],
            **kwargs
        )

    @RecordTest(top_only=True)
    def test_top_record(self, record: ocproc2.DataRecord, context: TestContext):
        # Skip station check if WorkingQuality=9
        if 'CNODCStation' in context.current_record.metadata:
            if context.current_record.metadata['CNODCStation'].best_value('WorkingQuality', 0) == 9:
                return
        station_options = self._find_station_matches(context.current_record)
        if not station_options:
            if context.current_record.metadata.has_value('CNODCStation'):
                context.report_for_review('station_bad_uuid')
                if 'CNODCStationString' in context.current_record.metadata:
                    del context.current_record.metadata['CNOCDStationString']
                if 'CNODCStationCandidates' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStationCandidates']
            elif self._has_station_id_candidate(context.current_record):
                context.report_for_review('station_no_record')
                if 'CNODCStationCandidates' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStationCandidates']
                context.current_record.metadata['CNODCStationString'] = '&'.join(
                    f"{x}={context.current_record.metadata.best_value(x)}"
                    for x in ('WMOID', 'StationName', 'WIGOSID', 'StationID')
                    if context.current_record.metadata.has_value(x)
                )
            else:
                context.report_for_review('station_no_id')
                if 'CNODCStation' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStation']
                if 'CNODCStationCandidates' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStationCandidates']
                if 'CNODCStationString' in context.current_record.metadata:
                    del context.current_record.metadata['CNOCDStationString']
        elif len(station_options) == 1:
            context.current_record.metadata['CNODCStation'] = station_options[0].station_uuid
            if 'CNODCStationCandidates' in context.current_record.metadata:
                del context.current_record.metadata['CNODCStationCandidates']
            if 'CNODCStationString' in context.current_record.metadata:
                del context.current_record.metadata['CNOCDStationString']
            if station_options[0].status == structures.StationStatus.INCOMPLETE:
                context.report_for_review('station_incomplete')
        else:
            context.report_for_review('station_many_records')
            if 'CNODCStation' in context.current_record.metadata:
                del context.current_record.metadata['CNODCStation']
            if 'CNODCStationString' in context.current_record.metadata:
                del context.current_record.metadata['CNOCDStationString']
            station_ids = [x.station_uuid for x in station_options]
            station_ids.sort()
            context.current_record.metadata['CNODCStationCandidates'] = station_ids

    def _has_station_id_candidate(self, record: DataRecord) -> bool:
        return any(record.metadata.has_value(x) for x in ('WMOID', 'StationName', 'WIGOSID', 'StationID'))

    def _find_station_matches(self, record: DataRecord) -> t.Optional[list[structures.NODBStation]]:
        with self.nodb as db:
            if record.metadata.has_value('CNODCStation'):
                station = structures.NODBStation.find_by_uuid(db, record.metadata.best_value('CNODCStation'))
                if station is not None:
                    return [station]
                return []
            obs_time = None
            if record.coordinates.has_value('Time') and record.coordinates['Time'].is_iso_datetime():
                obs_time = datetime.datetime.fromisoformat(record.coordinates['Time'].best_value())
            station_options = {
                x.station_uuid: x
                for x in self.searcher.search_stations(
                    db=db,
                    in_service_time=obs_time,
                    station_id=record.metadata.best_value('StationID'),
                    station_name=record.metadata.best_value('StationName'),
                    wmo_id=record.metadata.best_value('WMOID'),
                    wigos_id=record.metadata.best_value('WIGOSID')
                )
            }
            return self._select_best_matches(db, station_options)

    def _select_best_matches(self, db: NODBControllerInstance, options: dict[str, structures.NODBStation]) -> list[structures.NODBStation]:
        # No options means no options
        if not options:
            return []
        # One option means we just have to check if there is a map_to_uuid value
        if len(options) == 1:
            return [self._get_latest_match(db, list(options.values())[0]), options]
        new_options = self._filter_down_matches(db, [x for x in options.keys()], options)
        if new_options is not None:
            return new_options
        return []

    def _filter_down_matches(self, db: NODBControllerInstance, options_list: list[str], options: dict[str, structures.NODBStation]) -> t.Optional[list[structures.NODBStation]]:
        l_opts = len(options_list)

        # Filter down to only the most current matches based on this subset
        if l_opts > 1:
            new_list = set()

            for station_uuid in options_list:
                latest_match = self._get_latest_match(db, options[station_uuid], options)
                new_list.add(latest_match.station_uuid)

            options_list = list(new_list)
            l_opts = len(options_list)

        # No matches means we can't make a decision
        if l_opts == 0:
            return None

        # Return a list of stations otherwise (more than one match will be handled later)
        return [options[x] for x in options_list]

    def _get_latest_match(self, db: NODBControllerInstance, station: structures.NODBStation, cached_stations: dict = None) -> structures.NODBStation:
        # No map means this is the best match
        if station.map_to_uuid is None:
            return station
        # Use the cached list if possible
        elif cached_stations and station.map_to_uuid in cached_stations:
            return cached_stations[station.map_to_uuid]
        # Load from DB otherwise
        else:
            next_station = self._load_station(station.map_to_uuid)
            if next_station is not None:
                cached_stations[next_station.station_uuid] = next_station
                return self._get_latest_match(db, next_station, cached_stations)
            else:
                # TODO: this should probably prompt an error of some kind (map_to_uuid isn't set to a good value)
                return station
