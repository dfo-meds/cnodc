"""
Managing Python datetimes is complex. This module provides helper functions to keep datetimes nice and tidy.

It is assumed in this project that all date times will be aware of their timezone (that is, there are no naive times).
Scientific data is often stored as UTC time, so the default timezone is generally going to be Etc/UTC. Sometimes datetimes
will be the local file system time.

When adding new code to this project, you should use these methods or a similar one to ensure you always have a
timezone aware datetime object.
"""
import datetime
import zoneinfo
import time
import typing as t

TZ_INFO = t.Union[str, datetime.tzinfo, datetime.timedelta]

class AwareDateTime(datetime.datetime):
    """ Subclass of datetime.datetime that forces the datetime to local timezone if not specified. """

    def __new__(cls,
                year: t.SupportsIndex,
                month: t.SupportsIndex,
                day: t.SupportsIndex,
                hour: t.SupportsIndex = 0,
                minute: t.SupportsIndex = 0,
                second: t.SupportsIndex = 0,
                microsecond: t.SupportsIndex = 0,
                tzinfo: TZ_INFO = None,
                fold: int = 0):
        tzinfo = AwareDateTime._build_time_zone(tzinfo)
        obj = super().__new__(cls, year, month, day, hour, minute, second, microsecond, tzinfo, fold=fold)
        if tzinfo is None:
            dt = datetime.datetime(year, month, day, hour, minute, second, microsecond, fold=fold).astimezone()
            obj = obj.replace(tzinfo=dt.tzinfo)
        return obj

    def replace(self, *args, tzinfo = ...,  **kwargs):
        if tzinfo is Ellipsis:
            tzinfo = self.tzinfo
        else:
            tzinfo = AwareDateTime._build_time_zone(tzinfo)
        return super().replace(*args, tzinfo=tzinfo, **kwargs)

    def asutc(self) -> t.Self:
        return self.astimezone(datetime.timezone.utc)

    def time(self) -> time.time:
        return self.timetz()

    @classmethod
    def today(cls) -> t.Self:
        return cls.utcnow().astimezone()

    @classmethod
    def now(cls, tz: t.Optional[TZ_INFO] = None) -> t.Self:
        return cls.utcnow().astimezone(tz)

    @classmethod
    def utcnow(cls):
        return cls.from_datetime(datetime.datetime.now(datetime.timezone.utc))

    @classmethod
    def fromtimestamp(cls, timestamp: float, tz: t.Optional[TZ_INFO] = datetime.timezone.utc) -> t.Self:
        return cls.from_datetime(datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc), tzinfo=tz)

    @classmethod
    def utcfromtimestamp(cls, timestamp: float):
        return cls.fromtimestamp(timestamp)

    @classmethod
    def fromisoformat(cls, date_string, tzinfo: TZ_INFO = None):
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo=tzinfo)

    @classmethod
    def utcfromisoformat(cls, date_string: str):
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo='Etc/UTC')

    @classmethod
    def strptime(cls, date_string: str, format: str, tzinfo: TZ_INFO = None):
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo=tzinfo)

    @classmethod
    def utcstrptime(cls, date_string: str, format: str, tzinfo: TZ_INFO = None):
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo='Etc/UTC')

    @classmethod
    def from_datetime(cls, dt: datetime.datetime, tzinfo=None) -> t.Self:
        if isinstance(dt, AwareDateTime):
            return dt
        return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo or tzinfo, dt.fold)

    @staticmethod
    def _build_time_zone(tz: TZ_INFO) -> t.Optional[datetime.tzinfo]:
        if isinstance(tz, str):
            return zoneinfo.ZoneInfo(tz)
        if isinstance(tz, datetime.timedelta):
            return datetime.timezone(tz)
        return tz

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
    return AwareDateTime(year, month, day, hour, minute, second, microsecond, tzinfo or 'Etc/Utc', fold=fold)

def awaretime(year: int,
              month: int,
              day: int,
              hour: int = 0,
              minute: int = 0,
              second: int = 0,
              microsecond: int = 0,
              tzinfo: t.Optional[TZ_INFO] = None,
              *,
              fold: int = 0) -> AwareDateTime:
    """ Build a datetime from the given parameters, assuming tz is local time if not provided. """
    return AwareDateTime(year, month, day, hour, minute, second, microsecond, tzinfo, fold=fold)

def utc_now() -> AwareDateTime:
    """ Get the current time in UTC. """
    return AwareDateTime.utcnow()

def now() -> AwareDateTime:
    """ Get the current time in local time zone. """
    return AwareDateTime.now()

def utc_from_string(s: str, fmt: str) -> AwareDateTime:
    """ Parse the string as a datetime using the given format, assuming the timezone is UTC if not provided. """
    return AwareDateTime.utcstrptime(s, fmt)

def from_string(s: str, fmt: str, default_tz: t.Optional[TZ_INFO] = None) -> AwareDateTime:
    """ Parse the string as a datetime using the given format, assuming the timezone is in the given timezone if not provided (defaulting to local time). """
    return AwareDateTime.strptime(s, fmt, default_tz)

def utc_from_isoformat(s: str) -> AwareDateTime:
    """ Parse the string as an ISO formatted datetime, assuming the timezone is UTC if not provided. """
    return AwareDateTime.utcfromisoformat(s)

def from_isoformat(s: str, default_tz: t.Optional[TZ_INFO] = None) -> AwareDateTime:
    """ Parse the string as an ISO formatted datetime, assuming the timezone is the given timezone if not provided (defaulting to local time). """
    return AwareDateTime.fromisoformat(s, default_tz)

def from_timestamp(n: float) -> AwareDateTime:
    """ Convert the given UNIX timestamp into a datetime, assuming the timezone """
    return AwareDateTime.utcfromtimestamp(n)
