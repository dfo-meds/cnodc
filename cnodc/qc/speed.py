import math

from cnodc.nodb import NODBWorkingObservation, NODBPostgresController
from .common import QCSkip, QCError, QCReview, QCDelay, qc_test
import datetime


@qc_test('SPDC', 'Speed Check')
def speed_check(obs: NODBWorkingObservation, nodb: NODBPostgresController, search_range_hours: float = 24, max_search_results: int = 15, max_recheck_delay: int = 1, excess_speed_vote_threshold: int = 1):

    # Original observation must declare a latitude, longitude, station, and observation time to calculate speed
    if obs.latitude is None:
        raise QCSkip(f"Observation has no latitude")
    if obs.longitude is None:
        raise QCSkip(f"Observation has no longitude")
    if obs.obs_time is None:
        raise QCSkip(f"Observation has no observation time")
    if obs.station_uuid is None:
        raise QCSkip(f"Observation has no station UUID")

    # Station must be locatable
    station = nodb.find_station(obs.station_uuid)
    if station is None:
        raise QCError(f"Observation declares a station {obs.station_uuid} but it cannot be found")

    # Stations must declare a maximum speed for this check to apply
    if station.platform_max_speed is None:
        raise QCSkip(f"Station {station.pkey} does not declare a maximum speed")

    # Load similar observations from nearby
    check_observations = nodb.search_observations([
        ("station_uuid", "=", obs.station_uuid),
        ("obs_time", "<", obs.obs_time),
        ("obs_time", ">=", obs.obs_time - datetime.timedelta(hours=search_range_hours)),
        ("pkey", "!=", obs.pkey),
        ("latitude", "IS NOT NULL", None),
        ("longitude", "IS NOT NULL", None),
        ("obs_time", "IS NOT NULL", None),
        ("qc_tests_complete", "?", "SPDC")
    ], limit=max_search_results)

    # Handle the case if there are no observations (we might retry later or we might skip)
    if not check_observations:
        _raise_for_recheck(obs, max_recheck_delay)

    # Count the good and the bad
    results = []
    for item in check_observations:
        speed = obs.speed_to(item)
        if speed is not None:
            if speed > station.platform_max_speed:
                results.append(1)
            else:
                results.append(0)

    # This means nothing had a good enough speed to check(might happen if the platform is not moving)
    if not results:
        _raise_for_recheck(obs, max_recheck_delay)

    # At this point, we have decided if the record is good or bad, so we will remove this metadata
    obs.delete_metadata('SPDC_RECHECK_COUNT')

    # Quorum for deciding if the speed is bad is one third rounded up
    quorum = 1
    # Actual number of failures
    count = sum(results)
    # For now, this test is if the number of failed speed point tests exceeds 0 (i.e. any point suggests this one is
    # too fast).
    if count >= quorum:
        raise QCReview(f"Platform maximum speed exceeded", "platform_too_fast")


def _raise_for_recheck(obs: NODBWorkingObservation, max_recheck_delay: int):
    previous_rechecks = obs.get_metadata("SPDC_RECHECK_COUNT", 0)
    if max_recheck_delay > 0 and previous_rechecks < max_recheck_delay:
        obs.set_metadata("SPDC_RECHECK_COUNT", previous_rechecks + 1)
        raise QCDelay(f"No observations for comparison found, delaying")
    else:
        obs.delete_metadata("SPDC_RECHECK_COUNT")
        raise QCSkip(f"No observations for comparison found, skipping")
