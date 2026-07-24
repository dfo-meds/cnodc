from medsutil import json
import pathlib
import typing as t
import medsutil.types as mt

import zrlog


class SaveData:
    """Represents data that should be saved to the hard drive while running a process.
        Note that save data may be wiped if running on a virtual machine, so process workers
        cannot rely on the data being there and should have sensible default behaviour.

        Save data must be compatible with JSON objects.
    """

    def __init__(self, save_file: t.Union[str, pathlib.Path, None], raise_on_save_fail: bool = False):
        self._save_file = pathlib.Path(save_file) if isinstance(save_file, str) else save_file
        self._file_loaded: bool = False
        self._save_failed: bool = False
        self._raise_on_save_fail: bool = raise_on_save_fail
        self._values = {}

    def __getitem__(self, item: str) -> mt.SupportsExtendedJson:
        self.load_file()
        return self._values[item]

    def __setitem__(self, key: str, value: mt.SupportsExtendedJson):
        self.load_file()
        self._values[key] = value

    def __contains__(self, key: str) -> bool:
        self.load_file()
        return key in self._values

    def get(self, item: str, default: mt.SupportsExtendedJson = None) -> mt.SupportsExtendedJson:
        self.load_file()
        if item in self._values:
            return self._values[item]
        return default

    def load_file(self):
        """Ensure that the file is loaded, if it exists."""
        if not self._file_loaded:
            self._file_loaded = True
            if self._save_file and self._save_file.exists():
                try:
                    with open(self._save_file, "r") as h:
                        self._values = json.loads(h.read()) or {}
                except OSError:
                    zrlog.get_logger("cnodc.save_file").exception("Exception while opening save file")

    def save_file(self):
        """Save the data to disk."""
        if self._save_file is not None and self._file_loaded and not self._save_failed:
            try:
                self._save_file.parent.mkdir(mode=0o660, parents=True, exist_ok=True)
                with open(self._save_file, "w") as h:
                    h.write(json.dumps(self._values))
            except OSError:
                if self._raise_on_save_fail:
                    raise
                zrlog.get_logger("cnodc.save_file").exception("Exception while saving save data")
                self._save_failed = True
