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
                fold: int = 0,
                uncertainty=None):
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
        """ Replaces the given arguments as per datetime.replace, except guaranteed not to return a naive datetime. """
        tzinfo = self._build_time_zone(tzinfo if tzinfo is not Ellipsis else self.tzinfo)
        return self.from_datetime(super().replace(*args, tzinfo=tzinfo, **kwargs))

    def asutc(self) -> AwareDateTime:
        """ Converts to UTC. """
        return self.astimezone(datetime.timezone.utc)

    def time(self) -> datetime.time:
        """ Obtains the current time. """
        return self.timetz()

    def astimezone(self, tz: ct.TimeZoneInfo = None) -> AwareDateTime:
        """ Converts to the given timezone (default is current system time). """
        return self.from_datetime(super().astimezone(self._build_time_zone(tz)))

    @classmethod
    def today(cls) -> AwareDateTime:
        """ Obtains the current date/time in the system timezone. """
        return cls.utcnow().astimezone()

    @classmethod
    def now(cls, tz: t.Optional[ct.TimeZoneInfo] = None) -> AwareDateTime:
        """ Obtains the current date/time in the given timezone (or the system timezone if none specified). """
        return cls.utcnow().astimezone(tz)

    @classmethod
    def utcnow(cls):
        """ Obtains the current date/time in UTC timezone. """
        return cls.from_datetime(datetime.datetime.now(datetime.timezone.utc))

    @classmethod
    def fromtimestamp(cls, timestamp: float, tz: t.Optional[ct.TimeZoneInfo] = datetime.timezone.utc) -> AwareDateTime:
        """ Converts a UTC timestamp into an AwareDateTime object (with the given timezone, or UTC if not specified). """
        return cls.from_datetime(datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc), tzinfo=tz)

    @classmethod
    def utcfromtimestamp(cls, timestamp: float) -> AwareDateTime:
        """ Converts a UTC timestamp into an AwareDateTime object (in UTC). """
        return cls.fromtimestamp(timestamp)

    @classmethod
    def fromisoformat(cls, date_string: str, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        """ Converts from an ISO formatted string to an AwareDateTime object (assumes current system timezone unless specified). """
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo=tzinfo)

    @classmethod
    def utcfromisoformat(cls, date_string: str) -> AwareDateTime:
        """ Converts from an ISO formatted string to an AwareDateTime object (assumes UTC if not specified). """
        return cls.from_datetime(datetime.datetime.fromisoformat(date_string), tzinfo='Etc/UTC')

    @classmethod
    def strptime(cls, date_string: str, format: str, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        """ Converts from a date string in the given format to an AwareDateTime object (assumes current system timezone unless specified). """
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo=tzinfo)

    @classmethod
    def utcstrptime(cls, date_string: str, format: str) -> AwareDateTime:
        """ Converts from a date string in the given format to an AwareDateTime object (assumes UTC unless specified). """
        return cls.from_datetime(datetime.datetime.strptime(date_string, format), tzinfo='Etc/UTC')

    @classmethod
    def from_datetime(cls, dt: datetime.datetime | str | datetime.date | None, tzinfo: ct.TimeZoneInfo = None) -> AwareDateTime:
        """ Converts a date object or string into an AwareDateTime object (assumes current system timezone unless specified)."""
        if dt is None:
            return None
        if isinstance(dt, AwareDateTime):
            return dt
        elif isinstance(dt, datetime.datetime):
            return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo or tzinfo, dt.fold)
        elif isinstance(dt, str):
            return cls.fromisoformat(dt, tzinfo=tzinfo)
        else:
            return cls(dt.year, dt.month, dt.day, tzinfo=tzinfo)

    @staticmethod
    def _build_time_zone(tz: ct.TimeZoneInfo | None) -> datetime.tzinfo | None:
        """ Builds a timezone object from the given information. """
        if isinstance(tz, str):
            return zoneinfo.ZoneInfo(tz)
        if isinstance(tz, datetime.timedelta):
            return datetime.timezone(tz)
        return tz


class ScienceDateTime(AwareDateTime):
    """ A datetime that also has an uncertainty.

        This handles cases where the datetime is known to be rounded
        (e.g. to the nearest minute).

        We can then use ``is_compatible()`` to check if two ScienceDateTime objects
        could refer to the same date (i.e. the [min, max] ranges overlaps).

        The uncertainty is provided as a :py:class:`datetime.timedelta` object.
    """

    def __init__(self, *args, uncertainty: datetime.timedelta | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.uncertainty = uncertainty

    def is_compatible(self, dt: AwareDateTime) -> bool:
        """ Checks if this datetime and another have a range that overlap. """
        my_min, my_max = self.range()
        if isinstance(dt, ScienceDateTime):
            other_min, other_max = dt.range()
            return other_min <= my_max and other_max >= my_min
        else:
            return my_min <= dt <= my_max

    def range(self) -> tuple[t.Self, t.Self]:
        """ Returns the minimum and maximum range that this datetime could be. """
        if self.uncertainty is not None:
            return self - self.uncertainty, self + self.uncertainty
        else:
            return self, self
