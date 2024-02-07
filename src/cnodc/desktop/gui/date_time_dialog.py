import enum
import math
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd
import tkinter.messagebox as tkmb
import typing as t
import cnodc.desktop.translations as i18n
import tkcalendar as tkc
import datetime


def ask_date(*args, **kwargs):
    dtd = DateTimeDialog(*args, mode=_DateTimeDialogMode.DATE_ONLY, **kwargs)
    return dtd.result


def ask_datetime(*args, **kwargs):
    dtd = DateTimeDialog(*args, mode=_DateTimeDialogMode.DATE_TIME, **kwargs)
    return dtd.result


def ask_time(*args, **kwargs):
    dtd = DateTimeDialog(*args, mode=_DateTimeDialogMode.TIME_ONLY, **kwargs)
    return dtd.result


class _DateTimeDialogMode(enum.Enum):

    DATE_ONLY = 1
    TIME_ONLY = 2
    DATE_TIME = 3


class DateTimeDialog(tksd.Dialog):

    def __init__(self,
                 parent,
                 mode: _DateTimeDialogMode,
                 default: t.Optional[t.Union[datetime.datetime, datetime.date, datetime.time]] = None,
                 title: t.Optional[str] = None,
                 prompt: t.Optional[str] = None):
        self._mode = mode
        self._default = default
        self._prompt = prompt
        self._date_control: t.Optional[tkc.DateEntry] = None
        self._hour = tk.StringVar()
        self._minute = tk.StringVar()
        self._second = tk.StringVar()
        self._timezone = tk.StringVar()
        self.result = None
        if isinstance(self._default, (datetime.time, datetime.datetime)):
            self._hour.set(str(self._default.hour))
            self._minute.set(str(self._default.minute))
            self._second.set(str(self._default.second + (self._default.microsecond / 1000000)))
        else:
            self._hour.set("0")
            self._minute.set("0")
            self._second.set("0")
        super().__init__(parent=parent, title=title)

    def body(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.columnconfigure(1, weight=1)
        parent.columnconfigure(2, weight=1)
        parent.columnconfigure(3, weight=1)

        if self._prompt is not None:
            label = ttk.Label(parent, text=self._prompt, wraplength=300, justify=tk.LEFT)
            label.grid(row=0, column=0, sticky='NSEW', columnspan=4)
        first = None
        if self._mode in (_DateTimeDialogMode.DATE_TIME, _DateTimeDialogMode.DATE_ONLY):
            self._date_control = tkc.DateEntry(parent, width=5)
            self._date_control.grid(row=1, column=0, sticky='NSEW', columnspan=4, padx=2, pady=2)
            if isinstance(self._default, (datetime.datetime, datetime.date)):
                self._date_control.set_date(self._default)
            first = self._date_control
        if self._mode in (_DateTimeDialogMode.DATE_TIME, _DateTimeDialogMode.TIME_ONLY):
            ttk.Label(parent, text=i18n.get_text('hour')).grid(row=2, column=0, sticky='NSEW', padx=2, pady=2)
            ttk.Label(parent, text=i18n.get_text('minute')).grid(row=2, column=1, sticky='NSEW', padx=2, pady=2)
            ttk.Label(parent, text=i18n.get_text('second')).grid(row=2, column=2, sticky='NSEW', padx=2, pady=2)
            ttk.Label(parent, text=i18n.get_text('timezone')).grid(row=2, column=3, sticky='NSEW', padx=2, pady=2)
            hour = ttk.Combobox(parent, textvariable=self._hour, values=[str(x) for x in range(0, 24)], width=5)
            hour.grid(row=3, column=0, sticky='NSEW', padx=2, pady=2)
            minute = ttk.Combobox(parent, textvariable=self._minute, values=[str(x) for x in range(0, 60)], width=5)
            minute.grid(row=3, column=1, sticky='NSEW', padx=2, pady=2)
            second = ttk.Entry(parent, textvariable=self._second, width=5)
            second.grid(row=3, column=2, sticky='NSEW', padx=2, pady=2)
            tz = ttk.Combobox(parent, textvariable=self._timezone, values=['UTC'], width=5)
            tz.current(0)
            tz.grid(row=3, column=3, sticky='NSEW', padx=2, pady=2)
        return first

    def validate(self):
        self.result = None
        try:
            if self._mode == _DateTimeDialogMode.DATE_TIME:
                dt = self._date_control.get_date()
                full_seconds = float(self._second.get() or '0')
                if full_seconds < 0:
                    raise ValueError('Seconds must be zero or more')
                elif full_seconds >= 60:
                    raise ValueError('Seconds must be less than 60')
                seconds = int(math.floor(full_seconds))
                microseconds = int((full_seconds - seconds) * 1000000)
                self.result = datetime.datetime(
                    year=dt.year,
                    month=dt.month,
                    day=dt.day,
                    hour=int(self._hour.get()),
                    minute=int(self._minute.get()),
                    second=seconds,
                    microsecond=microseconds,
                    tzinfo=datetime.timezone.utc
                )
            elif self._mode == _DateTimeDialogMode.DATE_ONLY:
                self.result = self._date_control.get_date()
            elif self._mode == _DateTimeDialogMode.TIME_ONLY:
                full_seconds = float(self._second.get() or '0')
                if full_seconds < 0:
                    raise ValueError('Seconds must be zero or more')
                elif full_seconds >= 60:
                    raise ValueError('Seconds must be less than 60')
                seconds = int(math.floor(full_seconds))
                microseconds = int((full_seconds - seconds) * 1000000)
                self.result = datetime.time(
                    hour=int(self._hour.get()),
                    minute=int(self._minute.get()),
                    second=seconds,
                    microsecond=microseconds,
                    tzinfo=datetime.timezone.utc
                )
            return True
        except (ValueError, TypeError) as ex:
            tkmb.showwarning('Invalid', 'Invalid time or date')
            return False




