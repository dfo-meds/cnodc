import pathlib


class DirFileHandle:

    def __init__(self):
        pass

    def __str__(self):
        pass

    def exists(self) -> bool:
        pass

    def download(self, local_path: pathlib.Path):
        pass

    def upload(self, local_path: pathlib.Path):
        pass

    def child(self, sub_path: str):
        pass

    def delete(self):
        pass


class FileController:

    def __init__(self):
        pass

    def get_handle(self, file_path) -> DirFileHandle:
        pass

