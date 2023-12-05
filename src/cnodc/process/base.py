import multiprocessing as mp
import typing as t
import zrlog
import pathlib
from zrlog.logger import ImprovedLogger
from autoinject import injector
from cnodc.nodb import NODBController
from cnodc.storage import StorageController
import requests
from cnodc.util import HaltFlag
import json


class _SubprocessHaltFlag(HaltFlag):

    def __init__(self, halt: mp.Event):
        self._halt = halt

    def _should_continue(self) -> bool:
        return not self._halt.is_set()


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


class BaseProcess(mp.Process):

    def __init__(self, log_name: str, process_name: str, process_uuid: str, halt_flag: mp.Event, config: dict = None):
        super().__init__(daemon=True)
        self._log_name = log_name
        self.process_name = process_name
        self._log: t.Optional[ImprovedLogger] = None
        self._config = config or {}
        self._save_data: t.Optional[SaveData] = None
        self._defaults = {}
        self._halt: mp.Event = halt_flag
        self.is_working: mp.Event = mp.Event()
        self._shutdown: mp.Event = mp.Event()
        self.halt_flag: HaltFlag = _SubprocessHaltFlag(self._halt)
        self.process_uuid = process_uuid

    def continue_loop(self):
        return not(self._shutdown.is_set() or self._halt.is_set())

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
            self._log = zrlog.get_logger(self._log_name)
            zrlog.set_extras({
                'process_uuid': self.process_uuid,
                'process_name': self.process_name,
            })
            self._save_data = SaveData(self.get_config('save_file'))
            self._save_data.load_file()
            self.on_start()
            self._run()
        except Exception as ex:
            self._log.error(f"{ex.__class__.__name__}: {str(ex)}")
            self._log.exception(ex)
        finally:
            self.on_complete()
            self._save_data.save_file()

    def on_start(self):
        pass

    def on_complete(self):
        pass

    def _run(self):
        pass
