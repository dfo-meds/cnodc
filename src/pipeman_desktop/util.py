import enum


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


