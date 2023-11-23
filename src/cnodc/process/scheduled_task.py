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

    def _run(self):
        self.update_next_execution_time()
        while self.check_continue():
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                if self.check_execution(now):
                    self._last_execution_start = now
                    self.execute()
                    now = datetime.datetime.now(datetime.timezone.utc)
                    self._last_execution_end = now
                    self.update_next_execution_time()
                time.sleep(self.sleep_time(now))
            except CNODCError as ex:
                if ex.is_recoverable:
                    self._log.exception(f"Recoverable exception occurred during scheduled task processing")
                else:
                    raise ex

    def update_next_execution_time(self):
        mode = self.get_config("schedule_mode", "cron")
        if mode == "from_completion":
            if self._last_execution_end is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=(
                    0
                    if self.get_config("run_on_boot", False) else
                    int(self.get_config("delay_seconds"))
                ))
            else:
                self._next_execution = self._last_execution_end + datetime.timedelta(seconds=int(self.get_config("delay_seconds")))
        elif mode == "from_start":
            if self._last_execution_start is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=(
                    0
                    if self.get_config("run_on_boot", False) else
                    int(self.get_config("delay_seconds"))
                ))
            else:
                self._next_execution = self._last_execution_start + datetime.timedelta(seconds=int(self.get_config("delay_seconds")))
        elif mode == "cron":
            # TODO: implement this
            raise CNODCError(f"Cron-based scheduling not yet available", "SCHEDTASK", 1001)
        else:
            raise CNODCError(f"Invalid schedule_mode: [{mode}]", "SCHEDTASK", 1000)
        self._next_execution = self._next_execution.replace(microsecond=0)
        self._log.debug(f"Next execution: {self._next_execution.isoformat()}")

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
