import datetime
import random

from medsutil.awaretime import AwareDateTime
from medsutil.types import TimeZoneInfo
import typing as t

CRON_SPEC_PIECE = int | str | list[int]
CRON_SPEC = tuple[
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
] | str

WEEKDAY_MAP = {
    'sun': 0,
    'mon': 1,
    'tue': 2,
    'wed': 3,
    'thu': 4,
    'fri': 5,
    'sat': 6,
    7: 0,
}


MONTH_MAP = {
    'jan': 1,
    'feb': 2,
    'mar': 3,
    'apr': 4,
    'may': 5,
    'jun': 6,
    'jul': 7,
    'aug': 8,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12,
}

SPECIAL_DEFS = {
    '@yearly': (0,0,1,1,'*'),
    '@annually': (0,0,1,1,'*'),
    '@monthly': (0,0,1,'*','*'),
    '@weekly': (0,0,'*','*',0),
    '@daily': (0,0,'*','*','*'),
    '@midnight': (0,0,'*','*','*'),
    '@hourly': (0,'*','*','*','*'),
}


class CompiledCron:

    def __init__(self, cron_spec: CRON_SPEC, hash_key: str | int = None, cron_spec_timezone: t.Optional[TimeZoneInfo] = None):
        if hash_key is None:
            rgen = random.Random(id(self))
        elif isinstance(hash_key, str):
            rgen = random.Random(hash(hash_key))
        else:
            rgen = random.Random(hash_key)
        cron_tuple = _expand_cron_str(cron_spec)
        self._cron_config: tuple[list[int], list[int], list[int], list[int], list[int]] = (
            _validate_and_expand_cron_piece(cron_tuple[0], 0, 59, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[1], 0, 23, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[2], 0, 31, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[3], 0, 12, special_map=MONTH_MAP, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[4], 0, 6, special_map=WEEKDAY_MAP, rgen=rgen)
        )
        self._cron_tz = cron_spec_timezone

    def __str__(self):
        return ' '.join(
            ','.join(str(x) for x in self._cron_config[y])
            for y in range(0, len(self._cron_config))
        )

    def __repr__(self):
        return f'<CronConfig:{str(self)}>'

    def next_execution(self, current_time: AwareDateTime = None) -> AwareDateTime:
        # default to the next minute (the earliest possible execution)
        if current_time is None:
            current_time = AwareDateTime.now(self._cron_tz)

        # make sure we're using the same timezone
        else:
            current_time = current_time.astimezone(self._cron_tz)

        # Round up to the next minute (this is the earliest time we will return)
        if current_time.second > 0 or current_time.microsecond > 0:
            current_time = (current_time + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)

        # advance the time until it meets the requirements
        current_info = self._extract_cron_info(current_time)
        while not self._check_cron_info(current_info):
            current_time = self._adjust_execution_time(current_info, current_time)
            current_info = self._extract_cron_info(current_time)
        return current_time

    def _check_cron_info(self, current_info: tuple[int, int, int, int, int]) -> bool:
        return all(current_info[x] in self._cron_config[x] for x in range(0, 5))

    def _extract_cron_info(self, current_time: AwareDateTime) -> tuple[int, int, int, int, int]:
        # Calculate the time profile in cron terms
        wd = current_time.isoweekday()
        return current_time.minute, current_time.hour, current_time.day, current_time.month, wd if wd < 7 else 0

    def _adjust_execution_time(self, current_info: tuple[int, int, int, int, int], current_time: AwareDateTime) -> AwareDateTime:

        # If we're not in the right month, advance to the start of the next valid month and recheck from there
        if current_info[3] not in self._cron_config[3]:
            next_month, year_adjust = _extract_next_value(self._cron_config[3], current_info[3])
            return AwareDateTime(
                year=current_time.year + year_adjust,
                month=next_month,
                day=min(self._cron_config[2]),
                hour=min(self._cron_config[1]),
                minute=min(self._cron_config[0]),
                second=0, microsecond=0,
                tzinfo=current_time.tzinfo
            )

        # If we're not in the right day of week, calculate the next valid day
        if current_info[4] not in self._cron_config[4]:
            day_adjust = 1
            new_dow = (current_info[4] + day_adjust) % 7
            while new_dow not in self._cron_config[4] and day_adjust < 7:
                day_adjust += 1
                new_dow = (current_info[4] + day_adjust) % 7
            if day_adjust >= 7:
                raise ValueError("this shouldn't happen")
            # Set to 00:00 on that day
            current_time = current_time + datetime.timedelta(days=day_adjust)
            return current_time.replace(
                minute=min(self._cron_config[0]),
                hour=min(self._cron_config[1]),
                second=0, microsecond=0
            )

        # if we're not in the right day of the month, calculate the next valid day
        if current_info[2] not in self._cron_config[2]:
            next_day, month_adjust = _extract_next_value(self._cron_config[2], current_info[2])
            year_adjust = 0
            if month_adjust and current_info[3] == 12:
                month_adjust = -11
                year_adjust = 1
            return AwareDateTime(
                year=current_time.year + year_adjust,
                month=current_time.month + month_adjust,
                day=next_day,
                hour=min(self._cron_config[1]),
                minute=min(self._cron_config[0]),
                second=0, microsecond=0,
                tzinfo=current_time.tzinfo
            )

        # if we're not in the right hour, adjust the hour
        if current_info[1] not in self._cron_config[1]:
            next_hour, day_adjust = _extract_next_value(self._cron_config[1], current_info[1])
            if day_adjust:
                current_time = current_time + datetime.timedelta(days=1)
            return current_time.replace(
                hour=next_hour,
                minute=min(self._cron_config[0]),
                second=0, microsecond=0
            )

        # if we're not in the right minute, adjust the minute
        if current_info[0] not in self._cron_config[0]:
            next_minute, hour_adjust = _extract_next_value(self._cron_config[0], current_info[0])
            if hour_adjust:
                current_time = current_time + datetime.timedelta(hours=1)
            return current_time.replace(minute=next_minute, second=0, microsecond=0)

        raise ValueError("this really shouldn't happen - the time should have been accepted as valid instead")


def _extract_next_value(values: list[int], current_value: int) -> tuple[int, t.Literal[0 ,1]]:
    min_value: int | None = None
    min_greater_value: int | None = None
    for value in values:
        if min_value is None or value < min_value:
            min_value = value
        if value > current_value and (min_greater_value is None or value < min_greater_value):
            min_greater_value = value
    if min_greater_value is not None:
        return min_greater_value, 0
    elif min_value is not None:
        return min_value, 1
    else:
        raise ValueError("this also shouldn't happen")

def _expand_cron_str(cron_spec) -> tuple[CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE]:
    if isinstance(cron_spec, str):
        if cron_spec in SPECIAL_DEFS:
            return SPECIAL_DEFS[cron_spec]
        x = cron_spec.split(" ")
        if len(x) == 5:
            return x[0], x[1], x[2], x[3], x[4]
        else:
            raise ValueError(f'Invalid string cron spec: {cron_spec}')
    elif len(cron_spec) != 5:
        raise ValueError(f'Invalid sequence cron spec: {cron_spec}')
    else:
        return cron_spec[0], cron_spec[1], cron_spec[2], cron_spec[3], cron_spec[4]

def _validate_and_expand_cron_piece(piece: CRON_SPEC_PIECE,
                                    min_value: int,
                                    max_value: int,
                                    rgen: random.Random,
                                    special_map: dict[str | int, int] = None) -> list[int]:
    if isinstance(piece, str):
        int_or_list = _expand_cron_str_piece(piece, min_value, max_value, special_map or {}, rgen)
    else:
        int_or_list = piece
    return _validate_and_expand_cron_ints(int_or_list, min_value, max_value, special_map or {})

def _expand_cron_str_piece(piece: str, min_value: int, max_value: int, special_map: dict[str | int, int], rgen: random.Random) -> list[int]:
    if "," in piece:
        # Treat these as separate entries basically that we combine
        l = list()
        for subpiece in piece.split(","):
            l.extend(_expand_cron_str_piece(subpiece, min_value, max_value))
        return l

    else:
        # Handle increment
        increment = "1"
        if "/" in piece:
            piece, increment = piece.split("/", maxsplit=1)

        # Figure out our range
        if "-" in piece:
            min_range, max_range = piece.split("-", maxsplit=1)
        elif piece.isdigit() or piece in special_map:
            min_range = max_range = piece
        elif piece == "H":
            min_range = max_range = rgen.randint(min_value, max_value)
        elif piece == "*":
            min_range = min_value
            max_range = max_value
        else:
            raise ValueError(f"Invalid cron string piece: {piece}")

        # Resolve strings to ints as needed
        if min_range in special_map:
            min_range = special_map[min_range]
        if max_range in special_map:
            max_range = special_map[max_range]

        # validate our values and ranges. note that we handle min_value/max_value validation later
        min_r = int(min_range)
        max_r = int(max_range)
        inc = int(increment)
        if min_r > max_r:
            raise ValueError("Minimum value of a range can't be greater than the maximum value")
        if inc < 1:
            raise ValueError("Increment must be strictly positive")

        # Build a list
        return [x for x in range(min_r, max_r + 1, inc)]


def _validate_and_expand_cron_ints(piece: int | list[int], min_value: int, max_value: int, special_map: dict[str | int, int | list[int]]) -> list[int]:
    if special_map and isinstance(piece, int) and piece in special_map:
        piece = special_map[piece]
    if isinstance(piece, int):
        _validate_cron_int(piece, min_value, max_value)
        return [piece]
    else:
        for x in piece:
            _validate_cron_int(x, min_value, max_value)
        return piece


def _validate_cron_int(piece: int, min_value, max_value):
    if piece < min_value:
        raise ValueError(f"Value [{piece}] is less than minimum [{min_value}]")
    elif piece > max_value:
        raise ValueError(f"Value [{piece}] is more than maximum [{max_value}]")
