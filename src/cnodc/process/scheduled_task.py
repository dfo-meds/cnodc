import pathlib
import random

from .base import BaseProcess
import time
import datetime
import typing as t
from cnodc.util import CNODCError


class ScheduledTask(BaseProcess):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_execution_start: t.Optional[datetime.datetime] = None
        self._last_execution_end: t.Optional[datetime.datetime] = None
        self._next_execution: t.Optional[datetime.datetime] = None
        self._first_warning = False
        self.set_defaults({
            "save_file": None,
            "delay_fuzz_milliseconds": 0,
            "run_on_boot": False,
            "delay_seconds": None,
            "schedule_mode": "from_completion"
        })

    def _run(self):
        self._load_execution_times()
        self.update_next_execution_time()
        while self.halt_flag.check_continue(False):
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                if self.check_execution(now):
                    self.is_working.set()
                    self._last_execution_start = now
                    self.execute()
                    now = datetime.datetime.now(datetime.timezone.utc)
                    self._last_execution_end = now
                    self._preserve_execution_times()
                    self.update_next_execution_time()
                    self.is_working.clear()
                time.sleep(self.sleep_time(now))
            except CNODCError as ex:
                if ex.is_recoverable:
                    self._log.exception(f"Recoverable exception occurred during scheduled task processing")
                else:
                    raise ex

    def _preserve_execution_times(self):
        if self._first_warning:
            return
        file = self.get_config("save_file")
        if file is None:
            self._log.warning(f"Save file is disabled")
            self._first_warning = True
            return
        file_path = pathlib.Path(file).resolve()
        if not file_path.parent.exists():
            self._log.warning(f"Save file directory doesn't exist: [{file_path}]")
            self._first_warning = True
            return
        try:
            with open(file_path, "w") as h:
                h.write(self._last_execution_start.isoformat())
                h.write("\n")
                h.write(self._last_execution_end.isoformat())
        except Exception as ex:
            self._log.exception(f"Error saving execution times")
            self._first_warning = True

    def _load_execution_times(self):
        file = self.get_config("save_file")
        if file is None:
            self._log.warning(f"Save file is disabled")
            self._first_warning = True
            return
        file_path = pathlib.Path(file).resolve()
        if not file_path.exists():
            return
        try:
            with open(file_path, "r") as h:
                self._last_execution_start = datetime.datetime.fromisoformat(h.readline().strip("\r\n"))
                self._last_execution_end = datetime.datetime.fromisoformat(h.readline().strip("\r\n"))
        except Exception as ex:
            self._log.exception(f"Error loading execution times")

    def update_next_execution_time(self):
        mode = self.get_config("schedule_mode")
        if mode == "from_completion":
            if self._last_execution_end is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + self._execution_delay(True)
            else:
                self._next_execution = self._last_execution_end + self._execution_delay()
        elif mode == "from_start":
            if self._last_execution_start is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + self._execution_delay(True)
            else:
                self._next_execution = self._last_execution_start + self._execution_delay()
        elif mode == "cron":
            # TODO: implement this
            raise CNODCError(f"Cron-based scheduling not yet available", "SCHEDTASK", 1001)
        else:
            raise CNODCError(f"Invalid schedule_mode: [{mode}]", "SCHEDTASK", 1000)
        self._next_execution = self._next_execution.replace(microsecond=0)
        self._log.debug(f"Next execution: {self._next_execution.isoformat()}")

    def _execution_delay(self, first_run: bool = False) -> datetime.timedelta:
        delay = int(self.get_config("delay_seconds"))
        if first_run and self.get_config("run_on_boot"):
            delay = 0
        fuzz = int(self.get_config("delay_fuzz_milliseconds"))
        if fuzz > 0:
            delay += random.randint(0, fuzz) / 1000.0
        return datetime.timedelta(seconds=delay)

    def check_execution(self, now: datetime.datetime) -> bool:
        return now >= self._next_execution

    def sleep_time(self, now: datetime.datetime) -> float:
        time_diff = (self._next_execution - now).total_seconds()
        if time_diff < 0.25:
            return 0.05
        elif time_diff < 1.05:
            return 0.25
        elif time_diff < 5.25:
            return 1
        else:
            return time_diff - 5

    def execute(self):
        raise NotImplementedError()
