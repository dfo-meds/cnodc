import datetime
import json
import pathlib
import queue
import threading

from cnodcsubmit.submitter import CNODCSubmitter


class CNODCSubmissionManager(threading.Thread):

    def __init__(self,
                 *args,
                 transfer_queue: queue.Queue,
                 halt: threading.Event,
                 stop: threading.Event,
                 config: dict[str, int | float | str | None],
                 **kwargs):
        self._config = json.dumps(config)
        self._queue = transfer_queue
        self._halt = halt
        self._stop = stop
        super().__init__(*args, **(kwargs or {}))

    def queue_file(self, workflow_name: str, file: str, metadata: dict[str, str] | None):
        self._queue.put((workflow_name, file, None if not metadata else json.dumps(metadata)))

    def stop(self):
        self._stop.set()
        self.join()

    def run(self):
        config = json.loads(self._config)
        submitter = CNODCSubmitter(
            str(config.get("api", "")),
            float(config.get("max_bandwidth_kbps", 1024)),
            float(config.get("request_time_guess_ms", 1000)),
            int(config.get("overhead_guess_bytes", 250)),
            float(config.get("window_ms", 10000))
        )
        submitter.login(
            config.get("username", ""),
            config.get("password", "")
        )
        while not (self._halt.is_set() or self._stop.is_set()):
            try:
                workflow_name, file_name, metadata_str = self._queue.get(timeout=2)
                metadata = None if not metadata_str else json.loads(metadata_str)
                self._handle_file(submitter, workflow_name, pathlib.Path(file_name), metadata)
            except queue.Empty:
                ...

    def _handle_file(self, submitter: CNODCSubmitter, workflow_name: str, file: pathlib.Path, metadata: dict[str, str] | None = None):
        start_at_byte, chunk_number, last_actions = self._find_position(file)
        submit_more = True
        while submit_more:
            # TODO: handle errors in submissions properly, including restarting
            offset, at_eof, last_actions = submitter.submit_file_chunk(
                workflow_name=workflow_name,
                file=file,
                metadata=metadata,
                start_at=start_at_byte,
                chunk_number=chunk_number,
                last_actions=last_actions
            )
            start_at_byte += offset
            submit_more = not at_eof
            chunk_number += 1
            if not submit_more:
                self._save_position(file, start_at_byte, chunk_number, last_actions)
                self._clear_position(file)
                break
            else:
                self._save_position(file, start_at_byte, chunk_number, last_actions)
            if self._halt.is_set():
                break

    def _clear_position(self, file: pathlib.Path):
        complete = file.with_name(file.name + ".complete")
        if not complete.exists():
            with open(complete, "w") as h:
                h.write(datetime.datetime.now().isoformat())
        tracker = file.with_name(file.name + ".tracker")
        if tracker.exists():
            tracker.unlink()

    def _save_position(self, file: pathlib.Path, start_at: int, chunk_number: int, last_actions: dict | None):
        tracker = file.with_name(file.name + ".tracker")
        with open(tracker, "w") as h:
            h.write(json.dumps({
                "start_at": start_at,
                "chunk_number": chunk_number,
                "last_actions": last_actions
            }))

    def _find_position(self, file: pathlib.Path) -> tuple[int, int, dict | None]:
        tracker = file.with_name(file.name + ".tracker")
        if tracker.exists():
            with open(tracker, "r") as h:
                info = json.loads(h.read())
                return info["start_at"], info["chunk_number"], info["last_actions"]
        return 0, 0, None




