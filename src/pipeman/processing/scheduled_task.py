"""Scheduled task workers operate on a schedule.

    Several schedule modes are implemented:

    - from_completion delays the next run for the given number of seconds from the completion of
      the previous run
    - from_start delays the next run for the given number of seconds from the start of the
      previous run
    - cron will provide a cron-like interface (but not yet implemented)

"""
import random

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from pipeman.processing.base_worker import BaseWorker
import datetime
import typing as t
from pipeman.exceptions import CNODCError
from medsutil.cron import CompiledCron
import os

class ScheduledTaskError(CodedError): CODE_SPACE = 'SCHEDTASK'

class Never:

    @staticmethod
    def isoformat(*args, **kwargs):
        return "@never"


class ScheduledTask(BaseWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_execution: t.Optional[AwareDateTime | Never] = None
        self.set_defaults({
            "delay_fuzz_milliseconds": 1000,
            "run_on_boot": False,
            "delay_seconds": None,
            "schedule_mode": "from_completion",
            "cron_job_hash": None,
        })
        self._status_info.update({
            'executions': 0,
        })
        self._compiled_cron = None
        self._timezone = os.environ.get("TZ", default="America/Toronto")

    @property
    def compiled_cron(self) -> CompiledCron:
        if self._compiled_cron is None:
            try:
                self._compiled_cron = CompiledCron(
                    cron_spec=self.get_config("cron"),
                    hash_key=self.get_config("cron_job_hash"),
                    cron_spec_timezone=self._timezone,
                )
            except (ValueError, TypeError):
                raise ScheduledTaskError(f"Invalid cron expression: [{self.get_config("cron")}]", 2000)
        return self._compiled_cron

    @property
    def next_execution(self) -> AwareDateTime | Never:
        if self._next_execution is None:
            self._update_next_execution_time()
        return t.cast(AwareDateTime | Never, self._next_execution)

    def on_start(self):
        self._update_next_execution_time()
        super().on_start()

    def _run_once(self) -> float:
        now = AwareDateTime.now(self._timezone)
        if self._check_execution(now):
            self._status_info['executions'] += 1
            self.report(activity='executing')
            with self._run_time_histogram.time():
                now = self._run_scheduled_task(now)
        return self._sleep_time(now)

    def _check_execution(self, now: AwareDateTime) -> bool:
        """Check if the current time is after the next execution time."""
        if isinstance(self.next_execution, Never):
            return False
        return now >= self.next_execution

    def _run_scheduled_task(self, now: AwareDateTime) -> AwareDateTime:
        """Execute the scheduled task"""
        try:
            self.save_data['last_start'] = now.isoformat()
            self.save_data.save_file()
            self.before_cycle()
            self.execute()
        except CodedError as ex:
            # We assume non-recoverable errors will continually happen and recoverable errors may not
            # so a recoverable error isn't cause to crash the worker.
            if not ex.is_transient:
                raise
            else:
                self._log.exception(f"Recoverable error while executing scheduled task")
        finally:
            self.after_cycle()
            now = AwareDateTime.now(self._timezone)
            self.save_data['last_end'] = now.isoformat()
            self.save_data.save_file()
            self._update_next_execution_time()
        return now

    def execute(self):
        """Override with logic for the scheduled task."""
        raise NotImplementedError()  # pragma: no coverage

    def _sleep_time(self, now: AwareDateTime) -> float:
        """Calculate an ideal amount of time to sleep before the next execution"""
        if isinstance(self.next_execution, Never):
            return 3600 * 24 * 365 * 100  # puts the process into permanent sleep mode for 100 years
        else:
            time_diff = (self._next_execution - now).total_seconds()
            return 0.05 if time_diff < 0.25 else time_diff - 0.25

    def _update_next_execution_time(self):
        """Decide on the next scheduled execution time."""

        mode = self.get_config("schedule_mode", "from_completion")

        # Handle run on boot
        if self._next_execution is None and self.get_config("run_on_boot", False):
            self._next_execution = AwareDateTime.now(self._timezone)
            if mode in ("cron", "cron_from_completion", "cron_from_start"):
                # check that the cron expression compiles on boot if we aren't checking it below
                _ = self.compiled_cron

        elif mode == "from_completion":
            self._next_execution = self._next_execution_from_iso_time(self.save_data.get("last_end"))

        elif mode == "from_start":
            self._next_execution = self._next_execution_from_iso_time(self.save_data.get("last_start"))

        elif mode == "cron" or mode == "cron_from_completion":
            self._next_execution = self._next_cron_from_iso_time(self.save_data.get("last_end"))

        elif mode == "cron_from_start":
            self._next_execution = self._next_cron_from_iso_time(self.save_data.get("last_start"))

        elif mode == "boot_only":
            self._next_execution = Never()

        else:
            raise ScheduledTaskError(f"Invalid schedule_mode: [{mode}]", 1000)

        # logging stuff
        self._log.debug(f"Next execution: {self.next_execution.isoformat()}")
        self.report(next_execution=self.next_execution.isoformat())

    def _next_cron_from_iso_time(self, last_time_iso: str | None) -> AwareDateTime:
        last_time = (
            AwareDateTime.fromisoformat(last_time_iso)
            if isinstance(last_time_iso, str) else
            AwareDateTime.now(self._timezone)
        )
        return self.compiled_cron.next_execution(last_time).replace(microsecond=0)

    def _next_execution_from_iso_time(self, last_time_iso: str | None) -> AwareDateTime:
        """Calculate the delay from a given time point (start or end) to the next execution."""
        last_time = (
            AwareDateTime.fromisoformat(last_time_iso)
            if isinstance(last_time_iso, str) else
            AwareDateTime.now(self._timezone)
        )
        try:
            delay = int(self.get_config("delay_seconds"))
        except (ValueError, TypeError):
            raise ScheduledTaskError('Invalid delay', 1002)
        try:
            fuzz = int(self.get_config("delay_fuzz_milliseconds"))
            if fuzz > 0:
                delay += random.randint(0, fuzz) / 1000.0  # nosec B311 # random not for security purposes
            return (last_time + datetime.timedelta(seconds=delay)).replace(microsecond=0)
        except (ValueError, TypeError):
            raise ScheduledTaskError('Invalid fuzz delay', 1003)