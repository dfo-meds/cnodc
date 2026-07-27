import enum


class StopAction(Exception):
    pass


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


