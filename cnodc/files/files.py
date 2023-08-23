import pathlib
from autoinject import injector

from cnodc.util import HaltFlag


class DirFileHandle:

    def __init__(self):
        pass

    def __str__(self):
        pass

    def exists(self) -> bool:
        pass

    def download(self, local_path: pathlib.Path, allow_overwrite: bool = False, halt_flag: HaltFlag = None):
        pass

    def upload(self, local_path: pathlib.Path, allow_overwrite: bool = False, halt_flag: HaltFlag = None):
        pass

    def search(self, pattern: str, recursive: bool = True, halt_flag: HaltFlag = None):
        pass

    def child(self, sub_path: str):
        pass

    def delete(self):
        pass

    def is_dir(self):
        pass

    def name(self):
        pass

    def path(self):
        pass


@injector.injectable_global
class FileController:

    def __init__(self):
        pass

    def get_handle(self, file_path) -> DirFileHandle:
        pass

