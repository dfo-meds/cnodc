import datetime

from cnodc.exc import CNODCError
from cnodc.nodb import NODBWorkingObservation, NODBDatabaseProtocol
from cnodc.nodb.proto import NODBTransaction, LockMode
from cnodc.nodb.structures import QualityControlStatus, NODBStation, StationStatus, ObservationWorkingStatus
from cnodc.ocproc2 import DataRecord
from autoinject import injector

from cnodc.ocproc2.structures import NODBQCFlag


class BasicQualityController:

    database: NODBDatabaseProtocol = None

    @injector.construct
    def __init__(self, instance: str):
        self.name = "BQC"
        self.version = "1_0_0"
        self.instance = instance
        self._station_cache = {}

    def basic_qc_check(self, obs: NODBWorkingObservation, tx: NODBTransaction = None, first_time: bool = False) -> bool:
        if obs.qc_test_completed("basic"):
            return

        # Get record and clear QC codes
        record = obs.extract_data_record()
        obs.clear_qc_codes()

        if record.nodb_flag == NODBQCFlag.DISCARD_RECORD:
            obs.working_status = ObservationWorkingStatus.DISCARDED
            obs.qc_test_status = QualityControlStatus.

        elif record.nodb_flag == NODBQCFlag.RAISE_ERROR:
            obs.qc_test_status = ObservationWorkingStatus.ERROR

        else:
            # Apply coordinate and station check
            self._check_coordinates(record, obs)
            self._detect_station_id(record, obs, tx)

            # Check the results and add history info.
            if not obs.has_any_qc_code():
                record.add_history_info("BQC passed", self.name, self.version, self.instance)
                obs.store_data_record(record)
                obs.qc_test_status = QualityControlStatus.PASSED
                obs.mark_qc_test_complete("basic")
            else:
                record.add_history_warning("BQC failed", self.name, self.version, self.instance)
                obs.qc_test_status = QualityControlStatus.MANUAL_REVIEW
                obs.store_data_record(record)

    def _detect_station_id(self, record: DataRecord, working_record: NODBWorkingObservation, tx: NODBTransaction = None):
        # Don't reprocess where the station is known, but do check that the station info is complete
        if working_record.station_uuid is not None:
            station = self.database.load_station(
                working_record.station_uuid,
                tx=tx
            )
            if station.status == StationStatus.INCOMPLETE:
                working_record.apply_qc_code("incomplete_station_info")
            return

        # These are the identifiers we use
        identifiers = {
            x.lower(): (record.metadata[x].value() if x in record.metadata else None)
            for x in ('WMO_ID', 'WIGOS_ID', 'STATION_ID', 'STATION_NAME')
        }

        # No identifiers is a problem, so we bail immediately
        if not any(identifiers[x] is not None for x in identifiers):
            working_record.apply_qc_code("missing_station_id")
            return

        # Load the matches
        station_uuids = self.database.find_stations(**identifiers, with_lock=LockMode.FOR_KEY_SHARE, tx=tx)

        if len(station_uuids) > 1:
            # Remove redundant stations (with a map_to_uuid) where the match is also an option
            for stn in station_uuids.values():
                if stn.map_to_uuid is not None and stn.map_to_uuid in station_uuids:
                    del station_uuids[stn.pkey]

        if len(station_uuids) > 1:
            # Check if exactly one station matches the WMO or WIGOS ID
            wmo_wigos_match = None
            for stn in station_uuids.values():
                if (
                    stn.wmo_id is not None and identifiers['wmo_id'] is not None and stn.wmo_id == identifiers['wmo_id']
                ) or (
                    stn.wigos_id is not None and identifiers['wigos_id'] is not None and stn.wigos_id == identifiers['wigos_id']
                ):
                    if wmo_wigos_match is None:
                        wmo_wigos_match = stn.pkey
                    else:
                        wmo_wigos_match = None
                        break
            if wmo_wigos_match is not None:
                self._set_station_uuid(working_record, station_uuids[wmo_wigos_match])
                return

        match_count = len(station_uuids)
        # A single match is good
        if match_count == 1:
            self._set_station_uuid(working_record, list(station_uuids.values())[0])

        # No matches means we have not seen a station ID before, so we will flag it as such.
        elif match_count == 0:
            working_record.apply_qc_code("singleton_station_id")
            working_record.set_qc_metadata("station_identifiers", identifiers)

        # Otherwise, many matches
        else:
            working_record.apply_qc_code("ambiguous_station_id")
            working_record.set_qc_metadata("potential_matches", list(station_uuids.keys()))

    def _set_station_uuid(self, working_record: NODBWorkingObservation, station: NODBStation):
        if station.map_to_uuid is not None:
            actual_station = self.database.load_station(station.map_to_uuid)
            if actual_station is None:
                raise CNODCError(f"Station [{station.pkey}] has actual station [{station.map_to_uuid}] that does not exist", "BASIC", 1000)
            self._set_station_uuid(working_record, actual_station)
        else:
            working_record.station_uuid = station.pkey
            if station.status == StationStatus.INCOMPLETE:
                working_record.apply_qc_code("incomplete_station_info")

    def _check_coordinates(self, record: DataRecord, working_record: NODBWorkingObservation):
        coordinates_found = self._apply_basic_coordinate_test(record, working_record)
        if not coordinates_found[0]:
            working_record.apply_qc_code("missing_latitude")
        if not coordinates_found[1]:
            working_record.apply_qc_code("missing_longitude")
        if not coordinates_found[2]:
            working_record.apply_qc_code("missing_time")

    def _apply_basic_coordinate_test(self, record: DataRecord, working_record: NODBWorkingObservation) -> list[bool]:
        found = [False, False, False]
        if 'LAT' in record.coordinates:
            latitude = record.coordinates['LAT']
            lat_value = latitude.value()
            found[0] = True
            if lat_value is None:
                if latitude.nodb_flag != NODBQCFlag.MISSING:
                    latitude.nodb_flag = NODBQCFlag.REVIEW_MISSING
                    working_record.apply_qc_code("empty_latitude")
            elif (lat_value < -90 or lat_value > 90) and latitude.nodb_flag != NODBQCFlag.BAD:
                latitude.nodb_flag = NODBQCFlag.REVIEW_BAD
                working_record.apply_qc_code("bad_latitude")
        if 'LON' in record.coordinates:
            longitude = record.coordinates['LON']
            lon_value = longitude.value()
            found[1] = True
            if lon_value is None:
                if longitude.nodb_flag != NODBQCFlag.MISSING:
                    longitude.nodb_flag = NODBQCFlag.REVIEW_MISSING
                    working_record.apply_qc_code("empty_longitude")
            elif (lon_value < -180 or lon_value > 180) and longitude.nodb_flag != NODBQCFlag.BAD:
                longitude.nodb_flag = NODBQCFlag.REVIEW_BAD
                working_record.apply_qc_code("bad_longitude")
        if 'TIME' in record.coordinates:
            obs_time = record.coordinates['TIME']
            time_value = obs_time.value()
            found[2] = True
            if time_value is None:
                if obs_time.nodb_flag != NODBQCFlag.MISSING:
                    obs_time.nodb_flag = NODBQCFlag.REVIEW_MISSING
                    working_record.apply_qc_code("empty_time")
            elif obs_time.nodb_flag != NODBQCFlag.BAD:
                try:
                    real_time = datetime.datetime.fromisoformat(time_value)
                    if real_time > datetime.datetime.utcnow():
                        obs_time.nodb_flag = NODBQCFlag.REVIEW_BAD
                        working_record.apply_qc_code("bad_time")
                except ValueError:
                    obs_time.nodb_flag = NODBQCFlag.REVIEW_BAD
                    working_record.apply_qc_code("bad_time_format")
        if record.subrecords:
            for recordset_name in record.subrecords:
                for subrecord in record.subrecords[recordset_name]:
                    sub_found = self._apply_basic_coordinate_test(subrecord, working_record)
                    found = [found[i] or sub_found[i] for i in range(0, 3)]
        return found
