import datetime
import gzip
import json
import os
import pathlib
import queue
import shutil
import threading
import time


class CNODCLocalScanner(threading.Thread):

    def __init__(self,
                 *args,
                 transfer_queue: queue.Queue,
                 halt: threading.Event,
                 stop: threading.Event,
                 config: dict[str, int | float | str | None],
                 **kwargs):
        super().__init__(*args, **(kwargs or {}))
        self._config = json.dumps(config)
        self._queue = transfer_queue
        self._halt = halt
        self._stop = stop

    def queue_file(self, workflow_name: str, file: str, metadata: dict[str, str] | None):
        self._queue.put((workflow_name, file, None if not metadata else json.dumps(metadata)))

    def stop(self):
        self._stop.set()
        self.join()

    def run(self):
        # useful metadata:
        # source: where is the file coming from
        # source-name: actual source (e.g. region)
        # program-name: actual program
        # data-mode: RT or DM
        # quality-flags: 0
        config = json.loads(self._config)
        target_directory = config.get("target_directory", "")
        target_workflow = config.get("workflow_name", "")
        recursive = bool(config.get("recursive", False))
        gzip_files = bool(config.get("gzip_files", True))
        last_mod_threshold = float(config.get("last_modified_threshold_seconds", 60))
        metadata = config.get("metadata", {})
        if not target_directory:
            raise ValueError("Invalid target directory")
        if not target_workflow:
            raise ValueError("Invalid target workflow")
        if not isinstance(metadata, dict):
            raise TypeError("Metadata must be a dict")
        file_status = set()
        while not (self._halt.is_set() or self._stop.is_set()):
            work = [target_directory]
            while work:
                target_dir = work.pop()
                for file in os.scandir(target_dir):
                    if file.is_file():
                        m_time = datetime.datetime.fromtimestamp(file.stat().st_mtime)
                        if (datetime.datetime.now() - m_time).total_seconds() <= last_mod_threshold:
                            continue
                        self._handle_file(
                            target_workflow,
                            pathlib.Path(file.path),
                            metadata,
                            file_status,
                            gzip_files
                        )
                    elif recursive and file.is_dir():
                        work.append(file.path)
            time.sleep(2)

    def _handle_file(self, workflow_name: str, file: pathlib.Path, metadata: dict[str, str] | None, file_status: set[str], gzip_files: bool):
        if str(file) not in file_status:
            complete = file.with_name(file.name + ".complete")
            if not complete.exists():
                if gzip_files:
                    gzipped = file.with_name(file.name + ".gz")
                    if not gzipped.exists():
                        gzip_temp = file.with_name(file.name + ".gz.temp")
                        with gzip.open(gzip_temp, "wb") as out:
                            with open(file, "rb") as h:
                                shutil.copyfileobj(h, out)
                        gzip_temp.move(gzipped)
                    file = gzipped
                md = {}
                if metadata:
                    md.update(metadata)
                md['last-modified-date'] = datetime.datetime.fromtimestamp(file.stat().st_mtime).astimezone(datetime.timezone.utc).isoformat()
                md['filename'] = file.name
                self.queue_file(workflow_name, str(file), metadata)
            file_status.add(str(file))

