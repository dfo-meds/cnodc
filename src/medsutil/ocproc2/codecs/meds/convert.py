import typing as t
from medsutil.ocproc2.elements import SingleElement
from medsutil.ocproc2.structures import ParentRecord
from medsutil.ocproc2.codecs.meds.structs import StationRecord, MedsEncoding
from medsutil.ocproc2.util import combine_quality_scores, find_quality_for_protocol


def get_gtspp_quality(*objects: SingleElement):
    def _qualities() -> t.Iterable[int | None]:
        for object in objects:
            yield from find_quality_for_protocol(object, "gtspp")
            yield from find_quality_for_protocol(object, "nodb")
    return combine_quality_scores(_qualities())



def ocproc2_to_station(record: ParentRecord) -> StationRecord:
    sr = StationRecord()

    time = record.coordinates.ideal("Time")
    if time and time.is_iso_datetime():
        sr.observation_time = time.to_datetime()
        sr.quality_datetime = get_gtspp_quality(time)

    lat = record.coordinates.ideal("Latitude")
    lon = record.coordinates.ideal("Longitude")
    if lat and lon and lat.is_numeric() and lon.is_numeric():
        sr.coordinates = (lon.to_float("degrees_east"), lat.to_float("degrees_north"))
        sr.quality_position = get_gtspp_quality(lat, lon)

    header = record.metadata.ideal("GTSHeader")

    if header:
        pieces = header.to_string().split(" ")
        sr.gts_header_info = pieces[0]
        sr.gts_source_node = pieces[1]
        # sr.gts_bulletin_time = ...  TODO: we need to add information here



    return sr


def station_to_ocproc2(station: StationRecord, encoding: MedsEncoding) -> ParentRecord:
    pr = ParentRecord()
    pr.coordinates["Time"] = SingleElement(
        station.observation_time,
        DatePrecision="minute",
        Quality=SingleElement(
            station.quality_datetime,
            TestProtocol="meds"
        ),
    )
    pr.coordinates["Longitude"] = SingleElement(
        station.longitude,
        Units="degrees_east",
        Quality=SingleElement(
            station.quality_position,
            TestProtocol="meds"
        ),
        Uncertainty=SingleElement(
            "0.00005" if encoding is MedsEncoding.MEDS_ASCII else (station.longitude * (2 ** -24)),
            UncertaintyType="uniform"
        )
    )
    pr.coordinates["Latitude"] = SingleElement(
        station.latitude,
        Units="degrees_north",
        Quality=SingleElement(
            station.quality_position,
            TestProtocol="meds"
        ),
        Uncertainty=SingleElement(
            "0.00005" if encoding is MedsEncoding.MEDS_ASCII else (station.longitude * (2 ** -24)),
            UncertaintyType="uniform"
        )
    )
    if station.gts_bulletin_time or station.gts_header_info or station.gts_source_node:
        # TODO: should we format bulletin time properly?
        pr.metadata["GTSHeader"] = f"{station.gts_header_info or ""} {station.gts_source_node or ""} {station.gts_bulletin_time or ""}"


    return pr
