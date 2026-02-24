import datetime
import pathlib

from cnodc.process.scheduled_task import ScheduledTask
import zirconium as zr


class RequestCleanupTask(ScheduledTask):

    app_config: zr.ApplicationConfig = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.cleanup", **kwargs)
        self._requests_dir = None
        self._request_dir_failed = False
        self.set_defaults({
            "max_request_age_seconds": 3600
        })

    def get_requests_dir(self):
        if self._request_dir_failed:
            return None
        if self._requests_dir is None:
            self._requests_dir = self.app_config.as_path(("flask", "UPLOAD_FOLDER"))
            if not self._requests_dir:
                self._request_dir_failed = True
                self._log.error(f"Requests directory is not configured, cannot run cleanup")
            if self._requests_dir.exists() and not self._requests_dir.is_dir():
                self._request_dir_failed = True
                self._log.error(f"Requests directory is not a directory, cannot run cleanup")
            self._requests_dir = self._requests_dir / "requests"
        return self._requests_dir

    def execute(self):
        req_dir = self.get_requests_dir()
        if req_dir is None:
            return
        threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self.get_config("max_request_age_seconds"))
        for workflow_dir in req_dir.iterdir():
            if not workflow_dir.is_dir():
                continue
            for request_dir in workflow_dir.iterdir():
                if not request_dir.is_dir():
                    continue
                self._cleanup_request_directory(request_dir, threshold)

    def _cleanup_request_directory(self, request_dir: pathlib.Path, threshold: datetime.datetime):
        timestamp_file = request_dir / ".timestamp"
        if not timestamp_file.exists():
            return
        with open(timestamp_file, "r") as h:
            try:
                dt = datetime.datetime.fromisoformat(h.read())
                if dt > threshold:
                    return
            except ValueError as ex:
                self._log.exception(f"Error while checking datetime format in {request_dir}")
                return
        for f in request_dir.iterdir():
            f.unlink(True)
        request_dir.rmdir()
