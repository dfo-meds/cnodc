import zrlog
from dmd.containers.base import Field
import typing as t

TIME_PRECISION_MAP = {
    "month": "1970-01",
    "day": "1970-01-01",
    "hour": "1970-01-01T00Z",
    "minute": "1970-01-01T00:00Z",
    "second": "1970-01-01T00:00:00Z",
    "tenth_second": "1970-01-01T00:00:00.0Z",
    "hundredth_second": "1970-01-01T00:00:00.00Z",
    "millisecond": "1970-01-01T00:00:00.000Z",
}


def export_time_precision(field: Field,
                          mapping: str,
                          config: dict[str, t.Any]) -> dict[str, t.Any]:
    data = field.data()
    if data is not None:
        map_value = data["short_name"]
        if map_value in TIME_PRECISION_MAP:
            return {mapping: TIME_PRECISION_MAP[map_value]}
        else:
            # TODO: logging
            pass
    return {}


def preprocess_metadata_for_all():
    ...

def preprocess_metadata_for_erddap_xml():
    ...
