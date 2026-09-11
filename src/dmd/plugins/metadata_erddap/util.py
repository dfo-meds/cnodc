import zrlog
from dmd.containers.base import Field

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


def preprocess_metadata_for_all():
    ...

def preprocess_metadata_for_erddap_xml():
    ...
