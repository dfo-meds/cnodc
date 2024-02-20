"""Scheduled task workers operate on a schedule.

    Several schedule modes are implemented:

    - from_completion delays the next run for the given number of seconds from the completion of
      the previous run
    - from_start delays the next run for the given number of seconds from the start of the
      previous run
    - cron will provide a cron-like interface (but not yet implemented)

"""
import random
from .base import BaseWorker
import datetime
import typing as t
from cnodc.util import CNODCError


class ScheduledTask(BaseWorker):

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
        """Implement _run() by regularly checking if it is time to run the script."""
        self._update_next_execution_time()
        while self.continue_loop():
            now = datetime.datetime.now(datetime.timezone.utc)
            if self._check_execution(now):
                now = self._run_scheduled_task(now)
            self.responsive_sleep(self._sleep_time(now))

    def _update_next_execution_time(self):
        """Decide on the next scheduled execution time."""
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
        """Calculate the delay from a given time point (start or end) to the next execution."""
        try:
            delay = int(self.get_config("delay_seconds"))
        except TypeError:
            raise CNODCError('Invalid delay', 'SCHEDTASK', 1002)
        if first_run and self.get_config("run_on_boot"):
            delay = 0
        try:
            fuzz = int(self.get_config("delay_fuzz_milliseconds"))
        except TypeError:
            raise CNODCError('Invalid fuzz delay', 'SCHEDTASK', 1003)
        if fuzz > 0:
            delay += random.randint(0, fuzz) / 1000.0
        return datetime.timedelta(seconds=delay)

    def _check_execution(self, now: datetime.datetime) -> bool:
        """Check if the current time is after the next execution time."""
        return now >= self._next_execution

    def _run_scheduled_task(self, now) -> datetime.datetime:
        """Execute the scheduled task"""
        try:
            self._save_data['last_start'] = now.isoformat()
            self.before_item()
            self.execute()
        except CNODCError as ex:
            # We assume non-recoverable errors will continually happen and recoverable errors may not
            # so a recoverable error isn't cause to crash the worker.
            if not ex.is_recoverable:
                raise ex from ex
            else:
                self._log.exception(f"Recoverable error while executing scheduled task")
        finally:
            self.after_item()
            now = datetime.datetime.now(datetime.timezone.utc)
            self._save_data['last_end'] = now.isoformat()
            self._save_data.save_file()
            self._update_next_execution_time()
        return now

    def _sleep_time(self, now: datetime.datetime) -> float:
        """Calculate an ideal amount of time to sleep before the next execution"""
        time_diff = (self._next_execution - now).total_seconds()
        return 0.05 if time_diff < 0.25 else time_diff - 0.25

    def execute(self):
        """Override with logic for the scheduled task."""
        raise NotImplementedError()
