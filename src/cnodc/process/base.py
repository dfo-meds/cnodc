import multiprocessing as mp
import tempfile
import threading
import time
import typing as t
import zrlog
import pathlib
from zrlog.logger import ImprovedLogger
from autoinject import injector
from cnodc.nodb import NODBController
from cnodc.storage import StorageController
import requests
from cnodc.util import HaltFlag, dynamic_object
import json


class _ThreadingHaltFlag(HaltFlag):

    def __init__(self, global_halt: threading.Event):
        self._global = global_halt

    def _should_continue(self) -> bool:
        return not self._global.is_set()


class _NoHaltFlag(HaltFlag):

    def __init__(self):
        pass

    def _should_continue(self) -> bool:
        return True


class SaveData:

    def __init__(self, save_file: t.Union[str, pathlib.Path, None]):
        self._save_file = pathlib.Path(save_file) if isinstance(save_file, str) else save_file
        self._file_loaded: bool = False
        self._save_failed: bool = False
        self._values = {}

    def __getitem__(self, item):
        self.load_file()
        return self._values[item]

    def __setitem__(self, key, value):
        self.load_file()
        self._values[key] = value

    def get(self, item, default=None):
        if item in self._values:
            return self._values[item]
        return default

    def __contains__(self, key):
        return key in self._values

    def load_file(self):
        if not self._file_loaded:
            self._file_loaded = True
            if self._save_file and self._save_file.exists():
                try:
                    with open(self._save_file, "r") as h:
                        self._values = json.loads(h.read()) or {}
                except Exception:
                    zrlog.get_logger("cnodc.save_file").exception("Exception while opening save file")

    def save_file(self):
        if self._save_file is not None and self._file_loaded and not self._save_failed:
            try:
                with open(self._save_file, "w") as h:
                    h.write(json.dumps(self._values))
            except Exception:
                zrlog.get_logger("cnodc.save_file").exception("Exception while saving save data")
                self._save_failed = True


class BaseWorker:

    def __init__(self,
                 process_name: str,
                 process_version: str,
                 _process_uuid: str,
                 _halt_flag: HaltFlag,
                 _end_flag: HaltFlag,
                 _config: dict = None,
                 defaults: dict = None):
        self._halt_flag = _halt_flag
        self._end_flag = _end_flag
        self._process_uuid = _process_uuid
        self._process_name = process_name
        self._process_version = process_version
        self._config = _config or {}
        self._log: t.Optional[ImprovedLogger] = zrlog.get_logger(f"cnodc.worker.{process_name.lower()}")
        self._save_data = SaveData(self.get_config('save_file'))
        self._save_data.load_file()
        self._defaults = defaults or {}
        self._temp_dir: t.Optional[tempfile.TemporaryDirectory] = None
        zrlog.set_extras({
            'process_uuid': self._process_uuid,
            'process_name': self._process_name,
            'process_version': self._process_version,
        })

    def responsive_sleep(self, time_seconds: float, max_delay: float = 1.0):
        if time_seconds < (2 * max_delay):
            time.sleep(time_seconds)
        else:
            st = time.monotonic()
            et = st
            while (et - st) < time_seconds:
                time.sleep(min(max_delay, max(time_seconds - (et - st), 0.01)))
                et = time.monotonic()
                if not self.continue_loop():
                    break

    def continue_loop(self):
        return self._halt_flag.check_continue(False) and self._end_flag.check_continue(False)

    def get_config(self, key, default=None):
        if key in self._config and self._config[key] is not None:
            return self._config[key]
        elif key in self._defaults and self._defaults[key] is not None:
            return self._defaults[key]
        return default

    def set_defaults(self, values: dict):
        self._defaults.update(values)

    def run(self) -> None:
        try:
            self._log.debug(f'Starting process {self._process_uuid}')
            self.on_start()
            self._log.debug(f'Process {self._process_uuid} is running')
            self._run()
        except Exception as ex:
            self._log.error(f"{ex.__class__.__name__}: {str(ex)}")
            self._log.exception(ex)
        finally:
            self._log.debug(f'Cleaning up {self._process_uuid}')
            self.on_complete()
            self._save_data.save_file()
            self._log.debug(f'Process {self._process_uuid} complete')

    def on_start(self):
        pass

    def on_complete(self):
        pass

    def _run(self):
        pass

    def before_item(self):
        pass

    def temp_dir(self) -> pathlib.Path:
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory()
        return pathlib.Path(self._temp_dir.name)

    def after_item(self):
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
