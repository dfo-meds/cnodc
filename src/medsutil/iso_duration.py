import datetime
import typing as t

from medsutil import datadict as dd
from medsutil.awaretime import AwareDateTime
from pipeman.programs.dmd.metadata import EntityRef


class ISODuration(EntityRef):

    years: int = dd.p_int()
    months: int = dd.p_int()
    days: int = dd.p_int()
    hours: int = dd.p_int()
    minutes: int = dd.p_int()
    seconds: int = dd.p_int()

    def add_to(self, dt: AwareDateTime) -> AwareDateTime:
        dt2 = dt + datetime.timedelta(
            days=self.days or 0,
            seconds=self.time_part_total_seconds()
        )
        return self._adjust_year_month(dt2, self.date_part_total_months())

    def _adjust_year_month(self, dt: AwareDateTime, d_months: int) -> AwareDateTime:
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

    def subtract_from(self, dt: AwareDateTime) -> AwareDateTime:
        dt2 = dt - datetime.timedelta(
            days=self.days or 0,
            seconds=self.time_part_total_seconds()
        )
        return self._adjust_year_month(dt2, -1 * self.date_part_total_months())

    def date_part_total_months(self) -> int:
        return ((self.years or 0) * 12) + (self.months or 0)

    def time_part_total_seconds(self) -> int:
        return ((((self.hours or 0) * 60) + (self.minutes or 0)) * 60) + (self.seconds or 0)


    @classmethod
    def from_iso_format(cls, iso_duration: str) -> t.Self:
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
                return cls(years=parts[0] or None, months=parts[1] or None, days=parts[2] or None, hours=parts[3] or None, minutes=parts[4] or None, seconds=int(parts[5]) or None)
