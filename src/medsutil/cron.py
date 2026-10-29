"""
Provides an interpreter and helper class for cron times.

A cron task runs when the time exactly matches the cron specification. This class parses a specification
so that it can decide if it is time to run a task.


A cron statement typically has five parts: m H D M W

These are minute, hour, day, month, and day-of-week respectively.

Each part can be any of the following:

- A single specific number (e.g. `1`)
- A range of numbers (e.g. `1-5`) indicating any number in the range, inclusive
- The asterisk `*` indicating all valid values as a range
- A range may end with an increment (e.g. `/2` indicates every second value in the range)
- A comma-delimited list of the above options
- An `H` indicating a random value is selected. The cron specification can come with a seed for the random
  number generator to ensure stable hashes are selected.
- Specific numbers may also be replaced with three-letter lowercase months or names of days of the week

NB: Sunday is day "0".

There are several special cron phrases available that begin with an ampersand - these are replaced with
the equivalent five-tuple below:

- `@yearly` -> `0 0 1 1 *`
- `@annually` -> `0 0 1 1 *`
- `@monthly` -> `0 0 1 * *`
- `@weekly` -> `0 0 * * 0`
- `@daily` -> `0 0 * * *`
- `@midnight` -> `0 0 * * *`
- `@hourly` -> `0 * * * *`

Cron times are run against the current system timezone unless a different timezone is provided.

The specifications may be provided as a string (either a special phrase or a space-delimited expression)
or a five-tuple of valid cron pieces.

"""
import datetime
import random
import typing as t

from medsutil.awaretime import AwareDateTime
import medsutil.types as ct


CRON_SPEC_PIECE = int | str | list[int]
""" The type of a piece of a cron statement """


CRON_SPEC = tuple[
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
    CRON_SPEC_PIECE,
] | str
""" Acceptable formats for cron statements"""


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
""" Maps weekday strings (and the number 7) to numbers """


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
""" Maps month names to their numbers """


SPECIAL_DEFS = {
    '@yearly': (0,0,1,1,'*'),
    '@annually': (0,0,1,1,'*'),
    '@monthly': (0,0,1,'*','*'),
    '@weekly': (0,0,'*','*',0),
    '@daily': (0,0,'*','*','*'),
    '@midnight': (0,0,'*','*','*'),
    '@hourly': (0,'*','*','*','*'),
}
""" Maps special cron statements to their actual implementation """


class CompiledCron:
    """ Represents a cron specification that has been parsed. """

    def __init__(self,
                 cron_spec: CRON_SPEC,
                 hash_key: str | int = None,
                 cron_spec_timezone: t.Optional[ct.TimeZoneInfo] = None):
        """
        :param cron_spec: The cron specification to parse
        :param hash_key: A hash key to ensure consistent processing of H tags.
        :param cron_spec_timezone: The timezone to interpret the cron specification in.
        :raises ValueError: If the cron specification is invalid.
        """
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
            _validate_and_expand_cron_piece(cron_tuple[2], 1, 31, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[3], 1, 12, special_map=MONTH_MAP, rgen=rgen),
            _validate_and_expand_cron_piece(cron_tuple[4], 0, 6, special_map=WEEKDAY_MAP, rgen=rgen)
        )
        self._cron_tz = cron_spec_timezone

    def __str__(self): # pragma: no coverage # debugging only
        """ Returns a compiled cron configuration string.
            Note that this is not what is passed but instead is a simplified version using only spaces, the comma,
            and numbers.
        """
        return ' '.join(
            ','.join(str(x) for x in self._cron_config[y])
            for y in range(0, len(self._cron_config))
        )

    def __repr__(self): # pragma: no coverage # debugging only
        return f'<CronConfig:{str(self)}>'

    def next_execution(self, current_time: AwareDateTime = None) -> AwareDateTime:
        """ Given the current time (or assuming the current time), determines the next execution
            time that matches the cron specification and is in the future.
        """

        # default to the current time
        if current_time is None:
            current_time = AwareDateTime.now(self._cron_tz)

        # otherwise, make sure we're using the same timezone
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

    def _extract_cron_info(self, current_time: AwareDateTime) -> tuple[int, int, int, int, int]:
        """ Converts a datetime into the information in the same format as cron (a five-tuple). """
        wd = current_time.isoweekday()
        return current_time.minute, current_time.hour, current_time.day, current_time.month, wd if wd < 7 else 0

    def _check_cron_info(self, current_info: tuple[int, int, int, int, int]) -> bool:
        """ Checks if all the given info matches the cron specification. """
        return all(current_info[x] in self._cron_config[x] for x in range(0, 5))

    def _adjust_execution_time(self, current_info: tuple[int, int, int, int, int], current_time: AwareDateTime) -> AwareDateTime:
        """ Given a proposed but invalid execution time, we advance the execution time to one where at least
            one cron specification matches. If we do this iteratively, we will find a matching time.

            This works by performing the following checks (use the first that applies):

            1. If the current month is invalid:
               - move ahead to the next valid month (or the first valid month of the next year)
               - use the first day, hour, and minute value that matches.
            2. If the day of the week is invalid:
               - skip ahead up to seven days to the next day of the week that is valid
               - use the first hour and minute value that matches.
            3. If the current day of the month is invalid:
               - skip ahead to the next valid day of the month (or the first valid day of the next month)
               - use the first hour and minute value that matches.
            4. If the current hour is invalid:
               - skip ahead to the next valid hour (or the first valid hour of the next day)
               - use the first minute value that matches.
            5. If the current minute is invalid:
               - skip ahead to the next valid minute (or the first valid minute of the next hour)

            Note that the returned date time is not necessary valid, but at least one component is guaranteed
            to be valid. To find the next time, this function is called iteratively on the results of the previous
            call until all components are valid.

            TODO: should we add some kind of limiting check here to prevent infinite loops?
                  Consider if the max difference could be greater than eight years (e.g. if
                  we set it up for Feb 29 every year, that would be at most 8 years between
                  calls.

            :raises ValueError: if there are issues with the checks.
        """

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
    """ Given a list of values and a current one, finds the next valid one.

        We assume values is like a ring - we want:
        - The lowest value that is greater than the current one
        - Otherwise, the lowest value in the list

        The return result distinguishes between these cases.

        :param values: The list of values to search in
        :param current_value: The current value we're looking for.
        :returns: A tuple where the first is an item from values and the second is:
          0: if the returned value is greater than current_value
          1: if the returned value is less than current_value
    """
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

def _expand_cron_str(cron_spec: CRON_SPEC) -> tuple[CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE, CRON_SPEC_PIECE]:
    """ Expands a cron specification into a tuple.

        :raises ValueError: If the cron spec is invalid.
    """

    # handle the special cases
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
    """ Validates and expands a cron piece to a list of valid integers.

        :param piece: The cron piece to validate.
        :param min_value: The minimum value that the value can be
        :param max_value: The maximum value that the value can be
        :param rgen: Random number generator
        :param special_map: A map of strings or integers to their real values
        :returns: A list of integers that match the cron expression.
        :raises ValueError: If the cron expression is invalid.
    """
    if isinstance(piece, str):
        int_or_list = _expand_cron_str_piece(piece, min_value, max_value, special_map or {}, rgen)
    else:
        int_or_list = piece
    return _validate_and_expand_cron_ints(int_or_list, min_value, max_value, special_map or {})

def _expand_cron_str_piece(piece: str,
                           min_value: int,
                           max_value: int,
                           special_map: dict[str | int, int],
                           rgen: random.Random) -> list[int]:
    """ Expands a cron string into a list of integers.

        :param piece: The cron piece to expand
        :param min_value: The minimum value that the value can be
        :param max_value: The maximum value that the value can be
        :param rgen: Random number generator
        :param special_map: A map of strings or integers to their real values
        :returns: A list of integers that match the cron expression.
        :raises ValueError: If the cron spec is invalid.
    """

    # Handle commas first by breaking them into pieces and recursing.
    if "," in piece:
        # Treat these as separate entries basically that we combine
        l = list()
        for subpiece in piece.split(","):
            l.extend(_expand_cron_str_piece(subpiece, min_value, max_value, special_map, rgen))
        return l

    else:

        # Handle increment (e.g. 1-10/n)
        increment = "1"
        if "/" in piece:
            piece, increment = piece.split("/", maxsplit=1)

        # Figure out our range:

        # an explicit range
        if "-" in piece:
            min_range, max_range = piece.split("-", maxsplit=1)

        # a specific value from the map
        elif piece in special_map:
            min_range = max_range = special_map[piece]

        # a specific integer
        elif piece.isdigit():
            min_range = max_range = int(piece)

        # the special hash (a random value from our generator)
        elif piece == "H":
            min_range = max_range = rgen.randint(min_value, max_value)

        # any possible value
        elif piece == "*":
            min_range = min_value
            max_range = max_value

        # not valid
        else:
            raise ValueError(f"Invalid cron string piece: {piece}")

        # Resolve strings to ints as needed
        if min_range in special_map and isinstance(min_range, str) and not min_range.isdigit():
            min_range = special_map[min_range]

        if max_range in special_map and isinstance(max_range, str) and not max_range.isdigit():
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


def _validate_and_expand_cron_ints(piece: int | list[int],
                                   min_value: int,
                                   max_value: int,
                                   special_map: dict[str | int, int | list[int]]) -> list[int]:
    """ Validates that all the cron pieces are present and does any integer replacements that are in the map.

        :raises ValueError: If the cron piece is invalid.
    """

    # check
    if special_map:
        if isinstance(piece, int):
            if piece in special_map:
                piece = special_map[piece]
        else:
            temp = []
            for x in piece:
                if x in special_map:
                    new_val = special_map[x]
                    if isinstance(new_val, list):
                        temp.extend(new_val)
                    else:
                        temp.append(new_val)
                else:
                    temp.append(x)
            piece = temp

    if isinstance(piece, int):
        _validate_cron_int(piece, min_value, max_value)
        return [piece]
    else:
        for x in piece:
            _validate_cron_int(x, min_value, max_value)
        return piece


def _validate_cron_int(piece: int, min_value: int, max_value: int) -> None:
    """ Validates that piece is in the given range.

        :raises ValueError: If the piece is invalid.
    """
    if piece < min_value:
        raise ValueError(f"Value [{piece}] is less than minimum [{min_value}]")
    elif piece > max_value:
        raise ValueError(f"Value [{piece}] is more than maximum [{max_value}]")
