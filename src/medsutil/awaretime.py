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
import typing as t
import medsutil.types as ct


class AwareDateTime(datetime.datetime):
    """ Subclass of datetime.datetime that forces the datetime to local timezone if not specified. """

    def __new__(cls: AwareDateTime,
                year: t.SupportsIndex,
                month: t.SupportsIndex,
                day: t.SupportsIndex,
                hour: t.SupportsIndex = 0,
                minute: t.SupportsIndex = 0,
                second: t.SupportsIndex = 0,
                microsecond: t.SupportsIndex = 0,
                tzinfo: ct.TimeZoneInfo | None = None,
                fold: int = 0):
        tzinfo: t.Optional[datetime.tzinfo] = cls._build_time_zone(tzinfo)
        if tzinfo is None:
            dt = datetime.datetime(year, month, day, hour, minute, second, microsecond, fold=fold).astimezone()
            tzinfo = dt.tzinfo
        obj = super().__new__(cls, year, month, day, hour, minute, second, microsecond, tzinfo, fold=fold)
        return obj

    def __copy__(self) -> AwareDateTime:
        return self.__class__(self.year, self.month, self.day, self.hour, self.minute, self.second, self.microsecond, self.tzinfo, fold=self.fold)

    def __deepcopy__(self, memodict=None) -> AwareDateTime:
        return self.__copy__()

    def replace(self, *args, tzinfo = ...,  **kwargs) -> AwareDateTime:
        tzinfo = self._build_time_zone(tzinfo if tzinfo is not Ellipsis else self.tzinfo)
        return self.from_datetime(super().replace(*args, tzinfo=tzinfo, **kwargs))

    def asutc(self) -> AwareDateTime:
        return self.astimezone(datetime.timezone.utc)

    def time(self) -> datetime.time:
        return self.timetz()

    def astimezone(self, tz: ct.TimeZoneInfo = None) -> AwareDateTime:
        return self.from_datetime(super().astimezone(self._build_time_zone(tz)))

    @classmethod
    def today(cls) -> AwareDateTime:
        return cls.utcnow().astimezone()

    @classmethod
    def now(cls, tz: t.Optional[ct.TimeZoneInfo] = None) -> AwareDateTime:
        return cls.utcnow().astimezone(tz)

    @classmethod
    def utcnow(cls):
        return cls.from_datetime(datetime.datetime.now(datetime.timezone.utc))

    @classmethod
    def fromtimestamp(cls, timestamp: float, tz: t.Optional[ct.TimeZoneInfo] = datetime.timezone.utc) -> AwareDateTime:
        return cls.from_datetime(datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc), tzinfo=tz)

    @classmethod
    def utcfromtimestamp(cls, timestamp: float) -> AwareDateTime:
        return cls.fromtimestamp(timestamp)

    @classmethod
    def fromisoformat(cls, date_string, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo=tzinfo)

    @classmethod
    def utcfromisoformat(cls, date_string: str) -> AwareDateTime:
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo='Etc/UTC')

    @classmethod
    def strptime(cls, date_string: str, format: str, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo=tzinfo)

    @classmethod
    def utcstrptime(cls, date_string: str, format: str) -> AwareDateTime:
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo='Etc/UTC')

    @classmethod
    def from_datetime(cls, dt: datetime.datetime, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        if isinstance(dt, AwareDateTime):
            return dt
        elif isinstance(dt, datetime.datetime):
            return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo or tzinfo, dt.fold)
        else:
            return cls(dt.year, dt.month, dt.day)

    @staticmethod
    def _build_time_zone(tz: ct.TimeZoneInfo | None) -> datetime.tzinfo | None:
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
                  tzinfo: t.Optional[ct.TimeZoneInfo] = None,
                  *,
                  fold: int = 0) -> AwareDateTime:
    """ Build a datetime from the given parameters, assuming tz is UTC if not provided. """
    return AwareDateTime(year, month, day, hour, minute, second, microsecond, tzinfo or 'Etc/Utc', fold=fold)

def awaretime(year: int,
              month: int,
              day: int,
              hour: int = 0,
              minute: int = 0,
              second: int = 0,
              microsecond: int = 0,
              tzinfo: t.Optional[ct.TimeZoneInfo] = None,
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

def from_string(s: str, fmt: str, default_tz: t.Optional[ct.TimeZoneInfo] = None) -> AwareDateTime:
    """ Parse the string as a datetime using the given format, assuming the timezone is in the given timezone if not provided (defaulting to local time). """
    return AwareDateTime.strptime(s, fmt, default_tz)

def utc_from_isoformat(s: str) -> AwareDateTime:
    """ Parse the string as an ISO formatted datetime, assuming the timezone is UTC if not provided. """
    return AwareDateTime.utcfromisoformat(s)

def from_isoformat(s: str, default_tz: t.Optional[ct.TimeZoneInfo] = None) -> AwareDateTime:
    """ Parse the string as an ISO formatted datetime, assuming the timezone is the given timezone if not provided (defaulting to local time). """
    return AwareDateTime.fromisoformat(s, default_tz)

def from_timestamp(n: float) -> AwareDateTime:
    """ Convert the given UNIX timestamp into a datetime, assuming the timezone """
    return AwareDateTime.utcfromtimestamp(n)
