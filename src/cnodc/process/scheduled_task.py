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
        self._next_execution: t.Optional[datetime.datetime] = None
        self.set_defaults({
            "save_file": None,
            "delay_fuzz_milliseconds": 1000,
            "run_on_boot": False,
            "delay_seconds": None,
            "schedule_mode": "from_completion"
        })

    def _run(self):
        self.update_next_execution_time()
        while self.continue_loop():
            now = datetime.datetime.now(datetime.timezone.utc)
            if self.check_execution(now):
                now = self._run_scheduled_task(now)
            time.sleep(self.sleep_time(now))

    def _run_scheduled_task(self, now) -> datetime.datetime:
        try:
            self.is_working.set()
            self._save_data['last_start'] = now.isoformat()
            self.execute()
        except CNODCError as ex:
            if not ex.is_recoverable:
                raise ex from ex
            else:
                self._log.exception(f"Recoverable error while executing scheduled task")
        finally:
            now = datetime.datetime.now(datetime.timezone.utc)
            self._save_data['last_end'] = now.isoformat()
            self._save_data.save_file()
            self.update_next_execution_time()
            self.is_working.clear()
            return now


    def update_next_execution_time(self):
        mode = self.get_config("schedule_mode")
        if mode == "from_completion":
            last_end = self._save_data.get('last_end')
            if last_end is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + self._execution_delay(True)
            else:
                self._next_execution = datetime.datetime.fromisoformat(last_end) + self._execution_delay()
        elif mode == "from_start":
            last_start = self._save_data.get('last_start')
            if last_start is None:
                self._next_execution = datetime.datetime.now(datetime.timezone.utc) + self._execution_delay(True)
            else:
                self._next_execution = datetime.datetime.fromisoformat(last_start) + self._execution_delay()
        elif mode == "cron":
            # TODO: implement this
            raise CNODCError(f"Cron-based scheduling not yet available", "SCHEDTASK", 1001)
        else:
            raise CNODCError(f"Invalid schedule_mode: [{mode}]", "SCHEDTASK", 1000)
        # Round down the microseconds, this will help prevent the time from continually escalating
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
