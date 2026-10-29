""" Provides an ISODuration class that works with durations roughly as per the ISO standard.

    Note: we've taken some liberties.

    - we've assumed 1 hour = 60 minutes = 3600 seconds always.
    - we've assumed 1 year = 12 months always.
    - we've given users the options of treating 1 day as a Julian day ( = 24 hours = 86400 seconds) or not. The default
      is to treat is as a Julian day.

    This last setting is controlled in a few places by `use_calendar_day` - when it is set to true, we use the more
    strict ISO standard that one day means one day and not 86,400 seconds.

    This therefore might be better interpreted as a Julian-day duration that is formatted like the ISO standard. In
    practice, this is often what scientists actually mean and they almost always have the same values.

    Worth also noting that DST was not considered here. It is probably best you use this on UTC times for calculating
    durations. You may need to change the timezone of the output.

    Note that this creates only a few oddities which are documented here:

    - Dates before 1582 may be handled oddly. Don't rely on this module for dates before 1600.
    - There are 27 leap seconds from 1972 to 2016 which may produce oddities. When a leap second is introduced, the
      last minute of the day (June 30 or Dec 31 or both) has an additional second. For example, an ISO duration of
      PT1M30S from 2016-12-31T23:58:00 might be generally expected to be 2017-01-01T00:00:30. It actually becomes
      2017-01-01T00:00:29 because of the additional one second at 2016-12-31T23:59:60.
      This is where the Julian vs calendar day interpretation comes into play. In the default mode, P1D from
      2016-12-31T05:00:00 is actually 2017-01-01T04:59:59 (because of the additional second, there's actually 86,401
      seconds to 5:00:00 the next day). With the `use_calendar_day` value set to `True`, you will obtain
      2017-01-01T05:00:00 as might be expected.
    - In Toronto (Eastern time), adding four hours to 2026-11-01 00:00:00-04:00 will bring you to 2026-11-01
      04:00:00-04:00 - note that this is actually 2026-11-01 03:00:00-05:00 in civil time in Toronto due to DST ending.
      It is probably best if you use UTC for your calculations and then convert the output to whatever timezone you
      need.

    However, assuming a calendar day might occasionally be 86,401 seconds means a duration of "one day" cannot be
    converted into a specific number of seconds, minutes, or hours (much like "one day" can't be converted to months
    because its either 1/28, 1/29, 1/30, or 1/31 of a month depending on the month). The addition of DST considerations
    further adds complexity.

"""
import datetime
import typing as t

from medsutil import datadict as dd
from medsutil.multienum import MultiValuedEnum

class DurationUnit(MultiValuedEnum):
    """ Represents different possible durations of time. """

    YEAR = "years", "a"
    MONTH = "months", "mon"
    DAYS = "days", "d"  # NB: ISODuration lets you treat this as calendar days (exactly one day) or julian days (86400 seconds) - they are usually the same
    HOURS = "hours", "h"
    MINUTES = "minutes", "min"
    SECONDS = "seconds", "s"
    WEEKS = "weeks", "w"


class ISODuration(dd.DataDictObject):
    """ Tracks a duration value with support for ISO formatting.

        .. attention::
            Note that we use 1 year = 12 months and 1 hour = 60 minutes = 3600 minutes.
            The class defaults to 1 day = 86400 seconds, but can be changed to assume
            exactly one calendar day (regardless of leap seconds) is meant.
    """

    years: int = dd.p_int(default=0)
    """ Number of years """

    months: int = dd.p_int(default=0)
    """ Number of months"""

    days: int = dd.p_int(default=0)
    """ Number of Julian (or calendar) days """

    hours: int = dd.p_int(default=0)
    """ Number of hours """

    minutes: int = dd.p_int(default=0)
    """ Number of minutes """

    seconds: int = dd.p_int(default=0)
    """ Number of seconds """

    def __eq__(self, other):
        if isinstance(other, ISODuration):
            return self.time_part_total_seconds() == other.time_part_total_seconds() and self.date_part_total_months() == other.date_part_total_months() and self.days == other.days
        return NotImplemented

    def __add__(self, other):
        if isinstance(other, ISODuration):
            return ISODuration(
                years=self.years + other.years,
                months=self.months + other.months,
                days=self.days + other.days,
                hours=self.hours + other.hours,
                minutes=self.minutes + other.minutes,
                seconds=self.seconds + other.seconds,
            )
        elif isinstance(other, datetime.date):
            return self.add_to(other)
        else:
            return NotImplemented

    def __sub__(self, other):
        if isinstance(other, ISODuration):
            return ISODuration(
                years=self.years - other.years,
                months=self.months - other.months,
                days=self.days - other.days,
                hours=self.hours - other.hours,
                minutes=self.minutes - other.minutes,
                seconds=self.seconds - other.seconds,
            )
        else:
            return NotImplemented

    def __rsub__(self, other):
        if isinstance(other, datetime.date):
            return self.subtract_from(other)
        return NotImplemented

    def __radd__(self, other):
        return self.__add__(other)

    def isoformat(self) -> str:
        """ Converts to ISO format string."""
        s = "P"
        if self.years != 0:
            s += f"{self.years}Y"
        if self.months != 0:
            s += f"{self.months}M"
        if self.days != 0:
            s += f"{self.days}D"
        if self.hours != 0 or self.minutes != 0 or self.seconds != 0:
            s += "T"
            if self.hours != 0:
                s += f"{self.hours}H"
            if self.minutes != 0:
                s += f"{self.minutes}M"
            if self.seconds != 0:
                s += f"{self.seconds}S"
        return s

    def add_to(self, dt: datetime.date, use_calendar_days: bool = False) -> datetime.date:
        """ Adds the duration to a given date object. """
        dt2 = dt + datetime.timedelta(
            seconds=self.time_part_total_seconds()
        )
        return self._adjust_year_month_day(dt2, self.date_part_total_months(), self.days, use_calendar_days)

    def subtract_from(self, dt: datetime.date, use_calendar_days: bool = False) -> datetime.date:
        """ Subtracts the duration from a given date object.
            :raises ValueError: If the duration cannot be represented in the given units.
        """
        dt2 = dt - datetime.timedelta(
            seconds=self.time_part_total_seconds()
        )
        return self._adjust_year_month_day(dt2, -1 * self.date_part_total_months(), -1 * self.days, use_calendar_days)

    def to_duration(self, output_units: DurationUnit, use_calendar_days: bool = False) -> float:
        """ Converts to a duration in the given units. """
        match self.years, self.months, self.days, self.hours, self.minutes, self.seconds:
            case 0, 0, 0, h, m, s:
                return self.convert_duration_units((h * 3600) + (m * 60) + s, DurationUnit.SECONDS, output_units, use_calendar_days)
            case 0, 0, d, 0, 0, 0:
                return self.convert_duration_units(d, DurationUnit.DAYS, output_units, use_calendar_days)
            case 0, 0, d, h, m, s:
                if not use_calendar_days:
                    return self.convert_duration_units((d * 86400) + (h * 3600) + (m * 60) + s, DurationUnit.SECONDS, output_units, use_calendar_days)
                else:
                    raise ValueError(f"Cannot combine days with hours/minute/seconds when converting to a single duration unless leap seconds are ignored:  [{self.isoformat()}]")
            case y, m, 0, 0, 0, 0:
                return self.convert_duration_units((y * 12) + m, DurationUnit.MONTH, output_units, use_calendar_days)
            case _:
                raise ValueError(f"Cannot combine months/years and days/hours/minutes/seconds when converting to a single duration: [{self.isoformat()}]")

    def date_part_total_months(self) -> int:
        """ Returns the total number of months in the given duration. """
        return ((self.years or 0) * 12) + (self.months or 0)

    def time_part_total_seconds(self) -> int:
        """ Returns the total number of seconds in the given duration. """
        return (
            (self.seconds or 0)
            + ((self.minutes or 0) * 60)
            + ((self.hours or 0) * 3600)
        )

    @staticmethod
    def _adjust_year_month_day(dt: datetime.date, d_months: int, d_days: int, use_calendar_days: bool = False) -> datetime.date:
        """ Adjusts a given date for a given delta of months and days. """
        if use_calendar_days:
            dt = dt + datetime.timedelta(days=d_days)
        else:
            dt = dt + datetime.timedelta(seconds=(86400 * d_days))
        if d_months == 0:
            return dt
        year, month = dt.year, dt.month
        month += d_months
        while month > 12:
            year += 1
            month -= 12
        while month < 1:
            month += 12
            year -= 1
        return dt.replace(year=year, month=month)

    @staticmethod
    def convert_duration_units(x: float, from_units: DurationUnit, to_units: DurationUnit, use_calendar_days: bool = False) -> float:
        """ Converts a duration from one unit to another.
            :raises ValueError: If the duration cannot be represented in the given units.
        """
        match from_units, to_units:
            case DurationUnit.SECONDS, DurationUnit.SECONDS:
                return x
            case DurationUnit.SECONDS, DurationUnit.MINUTES:
                return x / 60
            case DurationUnit.SECONDS, DurationUnit.HOURS:
                return x / 3600
            case DurationUnit.SECONDS, DurationUnit.DAYS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 86400
            case DurationUnit.SECONDS, DurationUnit.WEEKS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 604800
            case DurationUnit.MINUTES, DurationUnit.SECONDS:
                return x * 60
            case DurationUnit.MINUTES, DurationUnit.MINUTES:
                return x
            case DurationUnit.MINUTES, DurationUnit.HOURS:
                return x / 60
            case DurationUnit.MINUTES, DurationUnit.DAYS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 1440
            case DurationUnit.MINUTES, DurationUnit.WEEKS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 10080
            case DurationUnit.HOURS, DurationUnit.SECONDS:
                return x * 3600
            case DurationUnit.HOURS, DurationUnit.MINUTES:
                return x * 60
            case DurationUnit.HOURS, DurationUnit.HOURS:
                return x
            case DurationUnit.HOURS, DurationUnit.DAYS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 24
            case DurationUnit.HOURS, DurationUnit.WEEKS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x / 168
            case DurationUnit.DAYS, DurationUnit.SECONDS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 86400
            case DurationUnit.DAYS, DurationUnit.MINUTES:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 1440
            case DurationUnit.DAYS, DurationUnit.HOURS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 24
            case DurationUnit.DAYS, DurationUnit.DAYS:
                return x
            case DurationUnit.DAYS, DurationUnit.WEEKS:
                return x / 7
            case DurationUnit.WEEKS, DurationUnit.SECONDS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 604800
            case DurationUnit.WEEKS, DurationUnit.MINUTES:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 10080
            case DurationUnit.WEEKS, DurationUnit.HOURS:
                if use_calendar_days:
                    raise ValueError("Cannot convert s/m/h to days when using calendar days")
                return x * 168
            case DurationUnit.WEEKS, DurationUnit.DAYS:
                return x * 7
            case DurationUnit.WEEKS, DurationUnit.WEEKS:
                return x
            case DurationUnit.MONTH, DurationUnit.YEAR:
                return x / 12
            case DurationUnit.MONTH, DurationUnit.MONTH:
                return x
            case DurationUnit.YEAR, DurationUnit.YEAR:
                return x
            case DurationUnit.YEAR, DurationUnit.MONTH:
                return x * 12
            case _:
                raise ValueError(f"Cannot convert {from_units} to {to_units} consistently")

    @classmethod
    def from_duration(cls, duration: float | int, units: DurationUnit) -> ISODuration:
        """ Converts a duration in given units to an ISO durtion object.
            :raises ValueError: if units is invalid
        """
        if units is DurationUnit.YEAR:
            return ISODuration(years=duration)
        elif units is DurationUnit.MONTH:
            return ISODuration(months=duration)
        elif units is DurationUnit.WEEKS:
            return ISODuration(days=duration * 7)
        elif units is DurationUnit.DAYS:
            return ISODuration(days=duration)
        elif units is DurationUnit.HOURS:
            return ISODuration(hours=duration)
        elif units is DurationUnit.MINUTES:
            return ISODuration(minutes=duration)
        elif units is DurationUnit.SECONDS:
            return ISODuration(seconds=duration)
        else:
            raise ValueError(f"Invalid duration: {units}")

    @classmethod
    def from_iso_format(cls, iso_duration: str) -> t.Self:
        """ Parse an ISO duration string and returns an object. """
        tcr = iso_duration.replace("-", "").replace(":", "").upper()
        if tcr[0] != 'P':
            raise ValueError('ISO formats begin with a P')
        else:
            parts = [0, 0, 0, 0, 0, 0]
            weeks = 0
            buffer = ''
            in_time = False
            used_alt_format = False
            for i in range(1, len(tcr)):
                if tcr[i].isdigit():
                    buffer += tcr[i]
                elif tcr[i] == 'T':
                    in_time = True
                    if buffer:
                        if len(buffer) != 8:
                            raise ValueError(f'Invalid alternate duration date length')
                        parts[0] = int(buffer[0:4])
                        parts[1] = int(buffer[4:6])
                        parts[2] = int(buffer[6:8])
                        buffer = ''
                        used_alt_format = True
                elif tcr[i] == 'Y':
                    parts[0] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'M':
                    parts[4 if in_time else 1] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'D':
                    parts[2] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'H':
                    parts[3] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'S':
                    parts[5] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'W':
                    weeks = int(buffer)
                    buffer = ''
                else:
                    raise ValueError(f'Invalid character found at position [{i}] in [{tcr}]')
            if buffer and used_alt_format:
                if len(buffer) in (2, 4, 6):
                    parts[3] = int(buffer[0:2])
                else:
                    raise ValueError(f'Invalid alternate duration time length [{buffer}]')
                if len(buffer) in (4, 6):
                    parts[4] = int(buffer[2:4])
                if len(buffer) == 6:
                    parts[5] = int(buffer[4:6])
            if weeks > 0:
                if any(x > 0 for x in parts):
                    raise ValueError('Cannot specify weeks and other time parts')
                return cls(days=weeks*7)
            else:
                return cls(years=parts[0], months=parts[1], days=parts[2], hours=parts[3], minutes=parts[4], seconds=parts[5])
