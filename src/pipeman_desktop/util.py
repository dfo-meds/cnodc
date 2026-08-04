import enum

from medsutil.ocproc2 import ParentRecord, QCResult, RecordAction


class StopAction(Exception):
    pass

def quality_color(wq: int, ind_wq: int = None):
    if wq == 1:
        return 'forestgreen'
    elif wq == 2:
        return 'lightseagreen'
    elif wq == 3:
        return 'darkorange'
    elif wq == 4:
        return 'maroon'
    elif wq == 9:
        return 'purple'
    elif wq == -1:
        return 'red'
    elif ind_wq is not None and ind_wq in (3, 4, 9):
        return 'silver'
    return 'black'

class ReviewResult(enum.Enum):

    CONTINUE = 'continue'
    RECHECK = 'recheck'
    ERROR = 'error'
    ESCALATE = 'escalate'
    DESCALATE = 'descalate'

    RELEASE = 'release'
    LOAD_ERROR = '_error'
    LOGOUT = '_logout'
    FORCE_CLOSE = '_force'


class BatchOpenState(enum.Enum):

    OPENING = 'O'
    OPEN = 'O2'
    OPEN_ERROR = 'OE'
    CLOSING = 'C'
    CLOSED = 'C2'
    CLOSE_ERROR = 'CE'


class CloseBatchResult(enum.Enum):

    CLOSING = 'C'
    ALREADY_CLOSED = 'L'
    CANCELLED = 'N'
    UNABLE_TO_CLOSE = 'U'


class BatchType(enum.Enum):

    STATION = 'cnodc.desktop.client.api_client.next_station_failure'



def build_default_actions(record: ParentRecord) -> list[RecordAction]:
    default_actions = []
    for qcr in record.qc_tests.iterate_with_load():
        if qcr.result is QCResult.MANUAL_REVIEW:
            default_actions.extend(qcr.proposed_actions)
    return default_actions

def build_local_record(record: ParentRecord, working_uuid: str) -> dict:
    info = {}
    lat = record.coordinates.ideal("Latitude")
    lon = record.coordinates.ideal("Longitude")
    time = record.coordinates.ideal("Time")
    info.update({
        "lat": lat.to_string() if lat else None,
        "lat_qc": lat.quality if lat else None,
        "lon": lon.to_string() if lon else None,
        "lon_qc": lon.quality if lon else None,
        "datetime": time.to_string() if time else None,
        "datetime_qc": time.quality if time else None,
        "has_errors": 0,
        "display": _build_display(record, working_uuid),
    })
    return info

def _build_display(record: ParentRecord, working_uuid: str):
    s = []
    if record.coordinates.has_value('Time'):
        s.append(f'T:{record.coordinates.best("Time")}')
    if record.coordinates.has_value('Latitude') and record.coordinates.has_value('Longitude'):
        s.append(f'X:{record.coordinates.best("Longitude")}')
        s.append(f'Y:{record.coordinates.best("Latitude")}')
    if record.coordinates.has_value('Depth'):
        s.append(f'Z:{record.coordinates.best("Depth")}')
    elif record.coordinates.has_value('Pressure'):
        s.append(f'P:{record.coordinates.best("Pressure")}')
    if not s:
        s.append(f"I:{working_uuid}")
    return '  '.join(s)