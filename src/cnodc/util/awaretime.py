"""
Managing Python datetimes is complex. This module provides helper functions to keep datetimes nice and tidy.

It is assumed in this project that all date times will be aware of their timezone (that is, there are no naive times).
Scientific data is often stored as UTC time, so the default timezone is generally going to be Etc/UTC. Sometimes datetimes
will be the local file system time.

When adding new code to this project, you should use these methods or a similar one to ensure you always have a
timezone aware datetime object.
"""
import datetime
import time
import zoneinfo
import typing as t


TZ_INFO = t.Union[str, datetime.timezone, datetime.timedelta]


def ensure_timezone(dt: datetime.datetime, default_tz: t.Optional[TZ_INFO] = None):
    """ Give the datetime object a timezone (if not specified, the default is whatever Python thinks the local timezone is)"""
    if dt.tzinfo is not None:
        return dt
    if default_tz is None:
        try:
            return dt.astimezone()
        except OSError as ex:
            print(dt)
            raise ex
    if isinstance(default_tz, str):
        default_tz = zoneinfo.ZoneInfo(default_tz)
    elif isinstance(default_tz, datetime.timedelta):
        default_tz = datetime.timezone(default_tz)
    return dt.replace(tzinfo=default_tz)


def utc_awaretime(year: int,
                  month: int,
                  day: int,
                  hour: int = 0,
                  minute: int = 0,
                  second: int = 0,
                  microsecond: int = 0,
                  tzinfo: t.Optional[TZ_INFO] = None,
                  *,
                  fold: int = 0):
    """ Build a datetime from the given parameters, assuming tz is UTC if not provided. """
    return awaretime(year, month, day, hour, minute, second, microsecond, tzinfo or 'Etc/Utc', fold=fold)

def awaretime(year: int,
              month: int,
              day: int,
              hour: int = 0,
              minute: int = 0,
              second: int = 0,
              microsecond: int = 0,
              tzinfo: t.Optional[TZ_INFO] = None,
              *,
              fold: int = 0):
    """ Build a datetime from the given parameters, assuming tz is local time if not provided. """
    return ensure_timezone(datetime.datetime(year, month, day, hour, minute, second, microsecond, fold=fold), tzinfo)

def utc_now():
    """ Get the current time in UTC. """
    return datetime.datetime.now(zoneinfo.ZoneInfo('Etc/UTC'))

def now():
    """ Get the current time in local time zone. """
    return utc_now().astimezone()

def utc_from_string(s: str, fmt: str):
    """ Parse the string as a datetime using the given format, assuming the timezone is UTC if not provided. """
    return from_string(s, fmt, 'Etc/UTC')

def from_string(s: str, fmt: str, default_tz: t.Optional[TZ_INFO] = None):
    """ Parse the string as a datetime using the given format, assuming the timezone is in the given timezone if not provided (defaulting to local time). """
    return ensure_timezone(datetime.datetime.strptime(s, fmt), default_tz)

def utc_from_isoformat(s: str):
    """ Parse the string as an ISO formatted datetime, assuming the timezone is UTC if not provided. """
    return from_isoformat(s, 'Etc/UTC')

def from_isoformat(s: str, default_tz: t.Optional[TZ_INFO] = None):
    """ Parse the string as an ISO formatted datetime, assuming the timezone is the given timezone if not provided (defaulting to local time). """
    return ensure_timezone(datetime.datetime.fromisoformat(s), default_tz)

def from_timestamp(n: float):
    """ Convert the given UNIX timestamp into a datetime, assuming the timezone """
    return ensure_timezone(datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc))
