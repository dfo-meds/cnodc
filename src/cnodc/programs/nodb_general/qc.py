import datetime
from cnodc.ocproc2 import DataRecord, MultiValue
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBControllerInstance
from cnodc.qc import VerificationTestResult, CNODCBaseQCTestSuite, TestContext
from cnodc.units import UnitConverter
from autoinject import injector


class NODBVerificationTestSuite(CNODCBaseQCTestSuite):

    converter: UnitConverter = None

    @injector.construct
    def __init__(self):
        super().__init__('nodb_verification', '1.0')
        self._db: t.Optional[NODBControllerInstance] = None

    def run_verification(self, record: DataRecord, db: NODBControllerInstance) -> VerificationTestResult:
        try:
            self._db = db
            return self.verify_record(record)
        finally:
            self._db = None

    def _verify_record(self, context: TestContext):
        if context.is_top_level():
            self._station_check(context)
        self._test_latitude(context)
        self._test_longitude(context)
        self._test_obs_time(context)

    def _station_check(self, context: TestContext):
        station_options = self._find_station_matches(context.current_record)
        if not station_options:
            if context.current_record.metadata.has_value('CNODCStation'):
                context.report_qc_failure('station_bad_uuid')
            elif self._has_station_id_candidate(context.current_record):
                context.report_qc_failure('station_no_record')
            else:
                context.report_qc_failure('station_no_id')
                if 'CNODCStation' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStation']
                if 'CNODCStationCandidates' in context.current_record.metadata:
                    del context.current_record.metadata['CNODCStationCandidates']
        elif len(station_options) == 1:
            context.current_record.metadata['CNODCStation'] = station_options[0].station_uuid
            if 'CNODCStationCandidates' in context.current_record.metadata:
                del context.current_record.metadata['CNODCStationCandidates']
        else:
            context.report_qc_failure('station_many_records')
            if 'CNODCStation' in context.current_record.metadata:
                del context.current_record.metadata['CNODCStation']
            context.current_record.metadata['CNODCStationCandidates'] = [x.station_uuid for x in station_options]

    def _has_station_id_candidate(self, record: DataRecord) -> bool:
        return any(record.metadata.has_value(x) for x in ('WMOID', 'StationName', 'WIGOSID', 'StationID'))

    def _find_station_matches(self, record: DataRecord) -> t.Optional[list[structures.NODBStation]]:
        if record.metadata.has_value('CNODCStation'):
            station = structures.NODBStation.find_by_uuid(self._db, record.metadata.best_value('CNODCStation'))
            if station is not None:
                return [station]
            return []
        obs_time = None
        if record.coordinates.has_value('Time') and record.coordinates['Time'].is_iso_datetime():
            obs_time = datetime.datetime.fromisoformat(record.coordinates['Time'].best_value())
        station_options = {
            x.station_uuid: x
            for x in structures.NODBStation.search(
                db=self._db,
                in_service_time=obs_time,
                station_id=record.metadata.best_value('StationID'),
                station_name=record.metadata.best_value('StationName'),
                wmo_id=record.metadata.best_value('WMOID'),
                wigos_id=record.metadata.best_value('WIGOSID')
            )
        }
        return self._select_best_matches(record, station_options)

    def _select_best_matches(self, record: DataRecord, options: dict[str, structures.NODBStation]) -> list[structures.NODBStation]:
        # No options means no options
        if not options:
            return []
        # One option means we just have to check if there is a map_to_uuid value
        if len(options) == 1:
            return [self._get_latest_match(list(options.values())[0])]
        new_options = self._filter_down_matches(record, [x for x in options.keys()], options)
        if new_options is not None:
            return new_options
        return []

    def _filter_down_matches(self, record: DataRecord, options_list: list[str], options: dict[str, structures.NODBStation]) -> t.Optional[list[structures.NODBStation]]:
        l_opts = len(options_list)

        # Filter down to only the most current matches based on this subset
        if l_opts > 1:
            new_list = set()

            for station_uuid in options_list:
                latest_match = self._get_latest_match(options[station_uuid], options)
                new_list.add(latest_match.station_uuid)

            options_list = list(new_list)
            l_opts = len(options_list)

        # No matches means we can't make a decision
        if l_opts == 0:
            return None

        # Return a list of stations otherwise (more than one match will be handled later)
        return [options[x] for x in options_list]

    def _get_latest_match(self, station: structures.NODBStation, cached_stations: dict = None) -> structures.NODBStation:
        # No map means this is the best match
        if station.map_to_uuid is None:
            return station
        # Use the cached list if possible
        elif cached_stations and station.map_to_uuid in cached_stations:
            return cached_stations[station.map_to_uuid]
        # Load from DB otherwise
        else:
            next_station = structures.NODBStation.find_by_uuid(self._db, station.map_to_uuid)
            if next_station is not None:
                cached_stations[next_station.station_uuid] = next_station
                return self._get_latest_match(next_station, cached_stations)
            else:
                # TODO: this should probably prompt an error of some kind (map_to_uuid isn't set to a good value)
                return station

    def _test_latitude(self, context: TestContext):
        if 'Latitude' not in context.current_record.coordinates:
            if context.is_top_level():
                context.report_qc_failure('lat_missing')
                context.current_record.coordinates.set('Latitude', None, WorkingQuality=19)
            return
        val = context.current_record.coordinates.get('Latitude')
        if val.metadata.best_value('WorkingQuality', 0) in (4, 9):
            return
        elif isinstance(val, MultiValue):
            context.report_qc_failure('lat_multi')
            val.metadata['WorkingQuality'] = 20
        elif val.is_empty():
            context.report_qc_failure('lat_empty')
            val.metadata['WorkingQuality'] = 19
        elif not val.is_numeric():
            context.report_qc_failure('lat_not_number')
            val.metadata['WorkingQuality'] = 14
        elif 'Units' in val.metadata and not self.converter.compatible(val.metadata['Units'].value, 'degree_north'):
            context.report_qc_failure('lat_bad_units')
            val.metadata['WorkingQuality'] = 20
        elif not val.in_range(-90, 90):
            context.report_qc_failure('lat_out_of_range')
            val.metadata['WorkingQuality'] = 14
        elif 'Quality' in val.metadata:
            val.metadata['WorkingQuality'] = val.metadata['Quality'].value
        else:
            val.metadata['WorkingQuality'] = 1

    def _test_longitude(self, context: TestContext):
        if 'Longitude' not in context.current_record.coordinates:
            if context.is_top_level():
                context.report_qc_failure('lon_missing')
                context.current_record.coordinates.set('Longitude', None, WorkingQuality=19)
            return
        val = context.current_record.coordinates.get('Longitude')
        if val.metadata.best_value('WorkingQuality', 0) in (4, 9):
            return
        elif isinstance(val, MultiValue):
            context.report_qc_failure('lon_multi')
            val.metadata['WorkingQuality'] = 20
        elif val.is_empty():
            context.report_qc_failure('lon_empty')
            val.metadata['WorkingQuality'] = 19
        elif not val.is_numeric():
            context.report_qc_failure('lon_not_number')
            val.metadata['WorkingQuality'] = 14
        elif 'Units' in val.metadata and not self.converter.compatible(val.metadata['Units'].value, 'degree_east'):
            context.report_qc_failure('lon_bad_units')
            val.metadata['WorkingQuality'] = 20
        elif not val.in_range(-180, 180):
            context.report_qc_failure('lon_out_of_range')
            val.metadata['WorkingQuality'] = 14
        elif 'Quality' in val.metadata:
            val.metadata['WorkingQuality'] = val.metadata['Quality'].value
        else:
            val.metadata['WorkingQuality'] = 1

    def _test_obs_time(self, context: TestContext):
        if 'Time' not in context.current_record.coordinates:
            if context.is_top_level():
                context.report_qc_failure('time_missing')
                context.current_record.coordinates.set('Time', None, WorkingQuality=19)
            return
        val = context.current_record.coordinates.get('Time')
        if val.metadata.best_value('WorkingQuality', 0) in (4, 9):
            return
        elif isinstance(val, MultiValue):
            context.report_qc_failure('time_multi')
            val.metadata['WorkingQuality'] = 20
        elif val.is_empty():
            context.report_qc_failure('time_empty')
            val.metadata['WorkingQuality'] = 19
        elif not val.is_iso_datetime():
            context.report_qc_failure('time_not_iso_format')
            val.metadata['WorkingQuality'] = 14
        else:
            max_time = datetime.datetime.now(datetime.timezone.utc)
            dt_val = datetime.datetime.fromisoformat(val.value)
            if dt_val > max_time:
                context.report_qc_failure('time_too_late')
                val.metadata['WorkingQuality'] = 14
            elif 'Quality' in val.metadata:
                val.metadata['WorkingQuality'] = val.metadata['Quality'].value
            else:
                val.metadata['WorkingQuality'] = 1

